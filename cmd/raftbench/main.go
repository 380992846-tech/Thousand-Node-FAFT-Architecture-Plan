// cmd/raftbench/main.go
//
// raftbench —— 用 third_party/flexiraft（hashicorp/raft 的 FPaxos 改造版）
// 直接测量 **不同 quorum 几何下的真实提交吞吐与延迟**。
//
// ── 这个基准回答什么问题 ────────────────────────────────────────────
// pkg/faft 里的所有 FAFT 数字都是**组合概率分析与代价模型**，不是实测。
// 本程序补上缺失的那一半：把 |Q1|/|Q2| 真正接到共识实现上，跑出
// 吞吐、延迟分位、RPC 计数。
//
// ── 为什么基线可信 ─────────────────────────────────────────────────
// flexiraft 在 DataQuorumSize=0 且 ElectionQuorumSize=0 时：
//   quorumSize()      -> voters/2 + 1        （= 上游）
//   dataQuorumSize()  -> voters/2 + 1        （= 上游）
//   commitment.recalculate() 的 q 回退值也 = 多数
// 也就是与上游 hashicorp/raft **逐行等价**。所以 "majority 基线" 与
// "FlexiRaft 实验组" 共用 100% 的代码路径，只差两个整数 ——
// 实现差异这个混淆变量被彻底排除。这是本基准最强的设计属性。
//
// ── 本机时钟的分辨率限制（重要）─────────────────────────────────────
// 本机 Go 单调时钟是**粗粒度跳变**的：200ms 忙等里只有 142 次跳变，
// p50 跳变 992µs、最大 4.5ms；time.Sleep(10µs) 最快也要 521µs 才返回。
// 后果：**任何低于约 1ms 的延迟测量都会读成 0**。
// 因此本程序：
//   1. 启动时用 probeClockFloor() 量出本机的时钟地板，写进结果；
//   2. 只有注入延迟 >= 1ms 时才报告延迟分位，否则标注"低于时钟地板，
//      不可信"，并把主指标让给吞吐（跨秒窗口，噪声可平均掉）。
// 所有跑出来的数字都必须带着这个地板读，否则会把"测不出来"误读成
// "没有开销"。
//
// ── 这不是端到端集群测量 ───────────────────────────────────────────
// 传输是进程内 in-memory（可选注入延迟），日志在内存里，单机单进程。
// 它测的是**共识协议本身**的开销，不是"生产集群能跑多少 QPS"。
// 见 docs/MEASUREMENT.md 的证据分级（Measured / Analysis / Simulated）。
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-kv/kvstore/pkg/bench"
	fraft "github.com/distributed-kv/kvstore/third_party/flexiraft"
)

// ─────────────────────────────────────────────────────────────────────
// FSM
// ─────────────────────────────────────────────────────────────────────

// sink 防止 FSM 的 apply 被优化掉。
var sink atomic.Uint64

// kvFSM 极简 FSM：统计已应用条数，并对"应用的到底是什么"留一个指纹。
//
// 为什么要有指纹：只比条数抓不住"两个节点在同一 index 应用了不同命令"。
// 每条命令的 payload 前 8 字节是全局唯一的序号 seq，FSM 把它和 index
// 一起混进 hash。跑完后各节点 (count, lastIndex, hash) 必须完全一致 ——
// 这是比条数强得多的安全性检查（仍然不是形式化证明，但足以抓住
// quorum 不相交、commit 规则写错这类灾难性缺陷）。
//
// Apply 由 raft 的**单个 FSM goroutine** 串行调用，所以 hash 是单写者；
// 用 atomic 只是为了跨 goroutine 读取时不触发竞态检测。
type kvFSM struct {
	applied atomic.Uint64
	lastIdx atomic.Uint64
	hash    atomic.Uint64
}

func mixHash(h, index, seq uint64) uint64 {
	// FNV-1a 风格的混合，够用且便宜。
	h ^= index
	h *= 1099511628211
	h ^= seq
	h *= 1099511628211
	return h
}

func (f *kvFSM) Apply(l *fraft.Log) interface{} {
	f.applied.Add(1)
	f.lastIdx.Store(l.Index)

	var seq uint64
	if len(l.Data) >= 8 {
		seq = binary.BigEndian.Uint64(l.Data[:8])
	}
	f.hash.Store(mixHash(f.hash.Load(), l.Index, seq))

	sink.Add(uint64(len(l.Data)))
	return nil
}

func (f *kvFSM) Snapshot() (fraft.FSMSnapshot, error) { return noopSnapshot{}, nil }
func (f *kvFSM) Restore(rc io.ReadCloser) error       { return rc.Close() }

type noopSnapshot struct{}

func (noopSnapshot) Persist(sink fraft.SnapshotSink) error { return sink.Close() }
func (noopSnapshot) Release()                              {}

// progress 把进度打到 stderr，避免污染 stdout 的 JSON。
func progress(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "[progress] "+format+"\n", args...)
}

// fence 内存屏障，防止编译器把忙等循环优化掉。
var fence atomic.Uint64

// probeClockFloor 测量本机 Go 单调时钟的**最小可分辨跳变**。
//
// 做法：反复「读一次时钟，空转到它变化，再读一次」，取 200 次里的最小值。
// 这个值就是延迟测量的物理下限 —— 任何比它短的区间都可能读成 0。
//
// 本机实测约 300µs，且 200ms 忙等中最大跳变达 4.5ms。这不是本程序的
// bug，是运行环境（虚拟化/沙箱时钟）的属性，所以必须随结果一起落盘。
func probeClockFloor() time.Duration {
	best := time.Duration(1 << 62)
	for i := 0; i < 200; i++ {
		t0 := time.Now()
		for j := 0; ; j++ {
			if !time.Now().Equal(t0) {
				break
			}
			fence.Add(1)
			if j > 100_000_000 {
				return best
			}
		}
		if d := time.Since(t0); d > 0 && d < best {
			best = d
		}
	}
	if best == time.Duration(1<<62) {
		return 0
	}
	return best
}

// ─────────────────────────────────────────────────────────────────────
// 集群
// ─────────────────────────────────────────────────────────────────────

type node struct {
	id    fraft.ServerID
	addr  fraft.ServerAddress
	raw   *fraft.InmemTransport
	trans *delayTransport
	store *fraft.InmemStore
	snaps *fraft.InmemSnapshotStore
	fsm   *kvFSM
	raft  *fraft.Raft
}

type cluster struct {
	nodes    []*node
	counters *rpcCounters
	q1, q2   int // 生效值
}

func (c *cluster) leader() *node {
	for _, nd := range c.nodes {
		if nd.raft.State() == fraft.Leader {
			return nd
		}
	}
	return nil
}

func (c *cluster) waitLeader(timeout time.Duration) (*node, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.leader(); l != nil {
			return l, nil
		}
		time.Sleep(2 * time.Millisecond)
	}
	return nil, fmt.Errorf("等待 leader 超时（%v）", timeout)
}

func (c *cluster) shutdown() {
	for _, nd := range c.nodes {
		if nd.raft != nil {
			_ = nd.raft.Shutdown().Error()
		}
	}
	for _, nd := range c.nodes {
		nd.raw.Close()
	}
}

func majority(n int) int { return n/2 + 1 }

// effectiveQuorum 把 0（未设）解析成多数，并校验范围。
func effectiveQuorum(n, q int) int {
	if q <= 0 || q > n {
		return majority(n)
	}
	return q
}

type buildOpts struct {
	n       int
	q1, q2  int
	delay   time.Duration
	sigma   float64
	seed    int64
	payload int
	// mae = MaxAppendEntries：每条 AppendEntries 最多带多少条日志。
	// 0 表示用上游默认（64）。它决定复制追赶速度的上限 ≈ mae/delay。
	mae int
	// hbTO / elecTO 覆盖心跳与选举超时。0 表示用默认（50ms）。
	// 分片数上千时要放大：否则 3000 个 Raft 实例的心跳+选举定时器
	// 本身就会把 CPU 吃满，测出来的就不再是结构而是调度器。
	hbTO, elecTO time.Duration
}

func buildCluster(o buildOpts) (*cluster, error) {
	if o.n < 1 {
		return nil, fmt.Errorf("n 必须 >= 1")
	}
	q1 := effectiveQuorum(o.n, o.q1)
	q2 := effectiveQuorum(o.n, o.q2)
	// Flexible Paxos 的安全性条件（Howard et al., OPODIS 2016 §4.2）。
	// 违反它不会报错，只会静默地丢数据 —— 所以必须在入口拦住。
	if q1+q2 <= o.n {
		return nil, fmt.Errorf("违反 Flexible Paxos 约束：|Q1|+|Q2| = %d+%d = %d <= n=%d；"+
			"该组合会让两个 quorum 不相交，导致已提交的日志丢失", q1, q2, q1+q2, o.n)
	}

	counters := &rpcCounters{}
	cl := &cluster{counters: counters, q1: q1, q2: q2}

	// 1. 建节点（先全部建好，再互联）。
	for i := 0; i < o.n; i++ {
		id := fraft.ServerID(fmt.Sprintf("n%d", i))
		addr := fraft.ServerAddress(fmt.Sprintf("mem-%d", i))
		realAddr, trans := newDelayTransport(addr, o.delay, o.sigma, o.seed+int64(i)*7919, counters)
		cl.nodes = append(cl.nodes, &node{
			id:    id,
			addr:  realAddr,
			raw:   trans.InmemTransport,
			trans: trans,
			store: fraft.NewInmemStore(),
			snaps: fraft.NewInmemSnapshotStore(),
			fsm:   &kvFSM{},
		})
	}

	// 2. 全互联。注意：Connect 对参数做 `t.(*InmemTransport)` 断言，
	//    必须传原始 transport，不能传 delayTransport。
	for _, a := range cl.nodes {
		for _, b := range cl.nodes {
			if a != b {
				a.raw.Connect(b.addr, b.raw)
			}
		}
	}

	// 3. 相同 configuration 引导每个节点。
	servers := make([]fraft.Server, 0, o.n)
	for _, nd := range cl.nodes {
		servers = append(servers, fraft.Server{ID: nd.id, Address: nd.addr, Suffrage: fraft.Voter})
	}
	configuration := fraft.Configuration{Servers: servers}

	for _, nd := range cl.nodes {
		cfg := newConfig(nd.id, q1, q2, o.mae, o.hbTO, o.elecTO)
		if err := fraft.BootstrapCluster(cfg, nd.store, nd.store, nd.snaps, nd.trans, configuration); err != nil {
			cl.shutdown()
			return nil, fmt.Errorf("引导 %s 失败: %w", nd.id, err)
		}
	}

	// 4. 启动。
	for _, nd := range cl.nodes {
		cfg := newConfig(nd.id, q1, q2, o.mae, o.hbTO, o.elecTO)
		r, err := fraft.NewRaft(cfg, nd.fsm, nd.store, nd.store, nd.snaps, nd.trans)
		if err != nil {
			cl.shutdown()
			return nil, fmt.Errorf("启动 %s 失败: %w", nd.id, err)
		}
		nd.raft = r
	}
	return cl, nil
}

func newConfig(id fraft.ServerID, q1, q2, mae int, hbTO, elecTO time.Duration) *fraft.Config {
	cfg := fraft.DefaultConfig()
	cfg.LocalID = id
	if hbTO <= 0 {
		hbTO = 50 * time.Millisecond
	}
	if elecTO <= 0 {
		elecTO = 50 * time.Millisecond
	}
	cfg.HeartbeatTimeout = hbTO
	cfg.ElectionTimeout = elecTO
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.LeaderLeaseTimeout = hbTO / 2
	cfg.SnapshotThreshold = 1 << 22 // 基准期间不触发快照
	cfg.SnapshotInterval = time.Hour
	cfg.TrailingLogs = 1 << 22
	cfg.NoSnapshotRestoreOnStart = true
	cfg.LogLevel = "ERROR"
	cfg.DataQuorumSize = q2
	cfg.ElectionQuorumSize = q1
	if mae > 0 {
		// 每条 AppendEntries 最多携带多少条日志。上游默认 64。
		//
		// 这个值决定**复制的追赶速度上限**：每次都带满 mae 条，
		// 每条 RPC 的往返是 delay，所以单个 follower 的复制吞吐上限
		// ≈ mae / delay。|Q2| 变小让 leader 可以不等 follower，
		// 但它产生日志的速度一旦超过这个上限，差额就只能在 leader 上堆积。
		cfg.MaxAppendEntries = mae
	}
	return cfg
}

// ─────────────────────────────────────────────────────────────────────
// 结果
// ─────────────────────────────────────────────────────────────────────

// runStats 一次测量窗口的统计。
type runStats struct {
	Index          int            `json:"index"`
	Ops            int            `json:"ops"`
	ElapsedSec     float64        `json:"elapsed_sec"`
	Throughput     float64        `json:"throughput_ops_per_sec"`
	Latency        bench.Snapshot `json:"latency_ms"`
	LeaderRetries  int64          `json:"leader_retries"`
	AppendEntries  int64          `json:"append_entries_rpc"`
	PipelineAE     int64          `json:"pipeline_append_entries_rpc"`
	RequestVoteRPC int64          `json:"request_vote_rpc"`
	AERPCPerOp     float64        `json:"append_entries_rpc_per_op"`

	// 窗口结束瞬间 leader 日志末尾与最慢 follower 之差（条数）。
	// |Q2| 小于多数时为正 —— 它是「已提交但尚未复制」的日志量。
	ReplicationGapMax uint64 `json:"replication_gap_entries"`

	// ClientSaturation = 客户端等待 Apply 的总时长 / (并发数 × 窗口时长)。
	// 接近 1 说明所有 worker 一直在等，系统已饱和（吞吐受服务端能力限制）；
	// 明显小于 1 说明吞吐只是"并发数 ÷ 延迟"，比较配置容量时不可直接用。
	ClientSaturation float64 `json:"client_saturation"`

	// DerivedAvgLatencyMs 由 Little 定律反推：W = L/λ = 并发数 / 吞吐。
	// 只用两个跨秒窗口的聚合量，因此**不受本机时钟地板影响** ——
	// 当直接测量的延迟低于时钟分辨率时，这是唯一还能用的延迟口径。
	// 仅在 ClientSaturation ≈ 1 时有意义。
	DerivedAvgLatencyMs float64 `json:"derived_avg_latency_ms"`
}

type result struct {
	Label        string  `json:"label"`
	Mode         string  `json:"mode"`
	N            int     `json:"n"`
	Q1           int     `json:"q1"`
	Q2           int     `json:"q2"`
	Majority     int     `json:"majority"`
	Ops          int     `json:"ops"`
	Concurrency  int     `json:"concurrency"`
	PayloadBytes int     `json:"payload_bytes"`
	Repeat       int     `json:"repeat"`
	Throughput   float64 `json:"throughput_ops_per_sec_median"`
	ThroughputLo float64 `json:"throughput_ops_per_sec_min"`
	ThroughputHi float64 `json:"throughput_ops_per_sec_max"`

	// 注入的网络参数 —— 必须随结果一起披露，否则数字会被误读成生产网络实测。
	InjectedDelayUs     int64   `json:"injected_delay_us"`
	InjectedJitterSigma float64 `json:"injected_jitter_sigma"`

	// 本机时钟地板。低于它的延迟数字没有意义。
	ClockFloorUs  int64 `json:"clock_floor_us"`
	LatencyUsable bool  `json:"latency_usable"`

	// GCPercent 本轮的 GC 触发阈值（见 -gogc）。影响内存占用，不影响对照公平性。
	GCPercent int `json:"gc_percent"`

	// MaxAppendEntries 生效值（0 时是上游默认 64）。
	MaxAppendEntries int `json:"max_append_entries"`

	// ReplicationCeilingOps 单个 follower 的复制追赶速度上限估计 ≈ mae / 注入延迟。
	// 注入延迟为 0 时该估计无意义（返回 0）。
	//
	// 这个数是本组实验最有用的一条"可行性条件"：|Q2| 变小把 leader 从
	// 等待 follower 中解放出来，但 follower 仍要以某个速率收日志。
	// 一旦 leader 的产生速率超过这个上限，差额就只能在 leader 上堆积，
	// 表现为 runs[].replication_gap_entries 一路上涨。
	ReplicationCeilingOps float64 `json:"replication_ceiling_ops_per_sec"`

	// 各轮实测 p50 的最小值，用于判断延迟是否真的高过时钟地板。
	ObservedP50Ms float64 `json:"observed_p50_ms"`

	// 客户端饱和度区间：接近 1 才说明测到的是服务端容量。
	ClientSaturationLo float64 `json:"client_saturation_min"`
	ClientSaturationHi float64 `json:"client_saturation_max"`

	Runs []runStats `json:"runs"`

	// 原始延迟样本（仅 -dump 时填充），单位纳秒，用于排查。
	RawLatencyNs []int64 `json:"raw_latency_ns,omitempty"`

	// 一致性检查：各节点 (count, lastIndex, hash) 必须完全一致。
	AppliedPerNode []uint64 `json:"applied_per_node"`
	LastIdxPerNode []uint64 `json:"last_index_per_node"`
	HashPerNode    []uint64 `json:"hash_per_node"`
	Consistent     bool     `json:"consistent"`
	ConsistencyMsg string   `json:"consistency_msg,omitempty"`

	// avail 模式
	//
	// 刻意不加 omitempty：0% 成功是本实验最重要的读数之一
	// （存活数 < |Q1| 时必然选不出 leader），被省略掉就看不见了。
	AvailTrials  int     `json:"avail_trials"`
	AvailSuccess int     `json:"avail_success"`
	AvailKilled  int     `json:"avail_killed"`
	AvailRate    float64 `json:"avail_success_rate"`

	// lag 模式：Apply 成功那一刻已应用该日志的副本数分布
	LagSamples      int            `json:"lag_samples"`
	LagHistogram    map[string]int `json:"lag_histogram,omitempty"`
	LagMeanReplicas float64        `json:"lag_mean_replicas"`

	// fail 模式：故障注入下的安全性实证
	//
	// 这一组要回答的问题只有一个，但它是整篇论文的底线：
	// **故障切换之后，客户端已经收到「成功」的写，有没有丢？**
	// FPaxos 用 |Q1|+|Q2|>N 在理论上保证不回滚；这里把它变成可复现的实测。
	FailAckedWrites  int      `json:"fail_acked_writes"`
	FailAckedBefore  int      `json:"fail_acked_before_kill"`
	FailAckedAfter   int      `json:"fail_acked_after_kill"`
	FailLostWrites   int      `json:"fail_lost_acked_writes"`
	FailMissingMax   uint64   `json:"fail_missing_max_seq"`
	FailSafetyOK     bool     `json:"fail_safety_ok"`
	FailoverMs       float64  `json:"failover_ms"`
	FailNewLeader    string   `json:"fail_new_leader"`
	FailNewLeaderOK  bool     `json:"fail_new_leader_elected"`
	FailVictim       string   `json:"fail_victim"`
	FailNodesWithAll []bool   `json:"fail_nodes_with_all_acked"`
	FailNodeMissing  []int    `json:"fail_node_missing_counts"`
	FailNodeNames    []string `json:"fail_node_names"`
	FailVictimBefore uint64   `json:"fail_victim_last_index_at_kill"`
	FailVictimAfter  uint64   `json:"fail_victim_last_index_after"`
	// FailNewLeaderMissing: 切换前已确认、但**新 leader 没有**的写数量。
	// 这是真正的回滚指标 —— 非 0 就意味着选举限制没起作用。
	FailNewLeaderMissing int `json:"fail_new_leader_missing"`
	// FailVictimMissing: 被杀的 leader 缺少多少条它自己确认过的写。
	// 这个数**必须**是 0：它就是"崩溃重启后数据仍在"的直接检验。
	FailVictimMissing int `json:"fail_victim_missing"`

	// burst 模式：同步突发负载（千卡 checkpoint 的形状）
	//
	// 主指标是 drain time —— 发起到全部完成的耗时，也就是训练侧的停顿时间。
	BurstCount      int                `json:"burst_count"`
	BurstOpsEach    int                `json:"burst_ops_each"`
	BurstDrainMs    []float64          `json:"burst_drain_ms"`
	BurstDrainMin   float64            `json:"burst_drain_min_ms"`
	BurstDrainP50   float64            `json:"burst_drain_p50_ms"`
	BurstDrainP99   float64            `json:"burst_drain_p99_ms"`
	BurstDrainMax   float64            `json:"burst_drain_max_ms"`
	BurstThrP50     float64            `json:"burst_throughput_p50_ops_per_sec"`
	PerBurstLatency []bench.Snapshot   `json:"per_burst_latency_ms,omitempty"`

	// shards 模式：多分片 + 整域故障（见 shards.go 开头的说明）
	ShardCount       int     `json:"shard_count"`
	ShardDomains     int     `json:"shard_domains"`
	ShardKilled      int     `json:"shard_domains_killed"`
	ShardKilledNodes int     `json:"shard_replicas_killed"`
	ShardAliveHist   string  `json:"shard_alive_histogram"`
	ShardAvailable   int     `json:"shard_available"`
	ShardAvailRate   float64 `json:"shard_available_rate"`
	// ShardEverAvail: 恢复窗口内**曾经**选出过 leader 的分片数。
	// 与 ShardAvailable（窗口末快照）分开报，是为了把「集群挂了」与
	// 「集群在反复重选、时而可用」区分开 —— 前者是失效，后者是摇摆。
	ShardEverAvail   int     `json:"shard_ever_available"`
	ShardEverRate    float64 `json:"shard_ever_available_rate"`
	// ShardTransient: 注入故障后**首个采样**的 leader 占比。
	// 它反映「原 leader 在 LeaderLeaseTimeout 内尚未退位」这个瞬态 ——
	// 是一个真实的短暂服务窗口，但**不是恢复**，所以必须与 ever 分开报。
	ShardTransient   float64 `json:"shard_transient_peak"`
	ShardLeaderFrac  float64 `json:"shard_leader_fraction_mean"`
	ShardVoteRecover float64 `json:"shard_requestvote_per_sec_recover"`
	ShardPredicted   int     `json:"shard_predicted_available"`
	ShardPredRate    float64 `json:"shard_predicted_rate"`
	ShardBuildMs     float64 `json:"shard_build_ms"`
	ShardReadyMs     float64 `json:"shard_all_leaders_ms"`
	ShardReadyCount  int     `json:"shard_leaders_ready"`
	ShardHeartbeat   float64 `json:"shard_heartbeat_rpc_per_sec"`
	ShardVoteRPS     float64 `json:"shard_requestvote_per_sec"`

	Notes []string `json:"notes"`
}

func main() {
	mode := flag.String("mode", "bench", "bench | avail | lag | fail | burst | shards | sweep")
	label := flag.String("label", "", "结果标签（默认自动生成）")
	n := flag.Int("n", 5, "副本数")
	q1 := flag.Int("q1", 0, "election quorum |Q1|（0 = 多数）")
	q2 := flag.Int("q2", 0, "data quorum |Q2|（0 = 多数）")
	ops := flag.Int("ops", 20000, "每轮的提交命令数上限")
	conc := flag.Int("concurrency", 64, "客户端并发")
	payload := flag.Int("payload", 64, "每条命令的 payload 字节数（至少 8，前 8 字节是序号）")
	warmup := flag.Int("warmup", 2000, "预热命令数（不计入统计）")
	repeat := flag.Int("repeat", 3, "测量窗口重复次数，报告吞吐中位数/极值")
	duration := flag.Duration("duration", 3*time.Second, "每轮测量窗口的目标时长（0 = 只按 ops 计）")
	delay := flag.Duration("delay", 0, "注入的单程 RPC 延迟中位数（0 = 不注入）")
	sigma := flag.Float64("sigma", 0.6, "注入延迟的对数正态形状参数（0 = 常量延迟）")
	timeout := flag.Duration("timeout", 10*time.Second, "单次 Apply 的超时")
	electTimeout := flag.Duration("electtimeout", 5*time.Second, "等 leader 的超时")
	trials := flag.Int("trials", 5, "avail 模式的重复次数")
	kill := flag.Int("kill", 0, "avail 模式杀掉的额外节点数（leader 之外的）")
	seed := flag.Int64("seed", 42, "随机种子")
	out := flag.String("out", "", "结果 JSON 输出路径；留空则打到 stdout（sweep 模式必填）")
	dump := flag.Bool("dump", false, "把每次提交的原始延迟（纳秒）写进结果，用于排查")
	delays := flag.String("delays", "0s,2ms", "sweep 模式：逗号分隔的注入延迟列表")
	lagOps := flag.Int("lagops", 2000, "lag 模式的串行采样条数")
	settle := flag.Duration("settle", 60*time.Second, "测量结束后等各节点追平的最长时间（|Q2| 变小时 follower 会明显落后）")
	gogc := flag.Int("gogc", 400, "GC 触发百分比。InmemStore 不截断日志，一轮测量会留下几十万个活跃 Log；"+
		"默认 GOGC=100 会让 GC 频繁扫描它们，把噪声打进吞吐。设 -1 关闭 GC（内存换稳定）")
	mae := flag.Int("mae", 0, "MaxAppendEntries：每条 AppendEntries 最多带几条日志（0 = 上游默认 64）。"+
		"它决定单个 follower 的复制追赶上限 ≈ mae/注入延迟")
	failLoad := flag.Duration("failload", 3*time.Second, "fail 模式：杀 leader 之前先施压多久")
	failAft := flag.Duration("failaft", 3*time.Second, "fail 模式：选出新 leader 之后再施压多久（检验切换后仍能服务）")
	bursts := flag.Int("bursts", 10, "burst 模式：突发次数")
	opsPerBurst := flag.Int("opsperburst", 50000, "burst 模式：每个突发提交多少条")
	idle := flag.Duration("idle", 2*time.Second, "burst 模式：突发之间的静默时长（模拟两个 checkpoint 之间的训练步）")
	shardCount := flag.Int("shards", 1000, "shards 模式：分片数")
	domains := flag.Int("domains", 5, "shards 模式：故障域数")
	killDomains := flag.Int("killdomains", 1, "shards 模式：注入故障时杀掉几个域")
	shardSteady := flag.Duration("shardsteady", 3*time.Second, "shards 模式：注入故障前先观察多久（量心跳开销）")
	shardRecover := flag.Duration("shardrecover", 10*time.Second, "shards 模式：注入故障后等多久再统计可用性")
	hbTO := flag.Duration("hb", 0, "覆盖 HeartbeatTimeout（0 = 50ms）。分片上千时要放大，否则定时器本身吃满 CPU")
	shardPoll := flag.Duration("shardpoll", 100*time.Millisecond, "shards 模式：恢复期轮询间隔。调小会显著增加扫描开销")
	maxInst := flag.Int("maxinstances", 0, "shards 模式：覆盖 Raft 实例总数上限（默认 3000，见 shards.go 的实测依据）")
	shardSettle := flag.Duration("shardsettle", 2*time.Second, "shards 模式：剔除「原 leader 退位延迟」的起始偏移（见 shards.go 第 4 步注释）")
	elecTO := flag.Duration("electto", 0, "覆盖 ElectionTimeout（0 = 50ms）")
	flag.Parse()

	// 本机噪声本来就大（同一段忙循环十次测量差 ±40%），再叠加 GC 就是双重噪声。
	// 提高 GC 阈值把这一层压掉 —— 对所有配置一视同仁，不偏向任何一组。
	// 代价是内存占用上升，所以把生效值记进结果。
	debug.SetGCPercent(*gogc)

	if *payload < 8 {
		fmt.Fprintln(os.Stderr, "payload 必须 >= 8（前 8 字节是命令序号）")
		os.Exit(2)
	}

	// 时钟地板对所有模式都重要，先量。
	floor := probeClockFloor()
	progress("本机时钟地板 = %v", floor)

	var res *result
	var err error

	switch *mode {
	case "bench":
		res, err = runBench(benchOpts{
			buildOpts: buildOpts{n: *n, q1: *q1, q2: *q2, delay: *delay, sigma: *sigma, seed: *seed, payload: *payload, mae: *mae},
			label:     *label, ops: *ops, conc: *conc, warmup: *warmup, repeat: *repeat,
			duration: *duration, timeout: *timeout, electTimeout: *electTimeout,
			settle: *settle, dump: *dump, clockFloor: floor, gogc: *gogc,
		})
	case "avail":
		res, err = runAvail(availOpts{
			buildOpts: buildOpts{n: *n, q1: *q1, q2: *q2, delay: *delay, sigma: *sigma, seed: *seed, payload: *payload, mae: *mae},
			label:     *label, trials: *trials, kill: *kill, electTimeout: *electTimeout, clockFloor: floor,
		})
	case "lag":
		res, err = runLag(lagOpts{
			buildOpts:    buildOpts{n: *n, q1: *q1, q2: *q2, delay: *delay, sigma: *sigma, seed: *seed, payload: *payload, mae: *mae},
			label:        *label,
			samples:      *lagOps,
			timeout:      *timeout,
			electTimeout: *electTimeout,
			clockFloor:   floor,
		})
	case "fail":
		res, err = runFail(failOpts{
			buildOpts:    buildOpts{n: *n, q1: *q1, q2: *q2, delay: *delay, sigma: *sigma, seed: *seed, payload: *payload, mae: *mae},
			label:        *label,
			loadTime:     *failLoad,
			aftTime:      *failAft,
			conc:         *conc,
			timeout:      *timeout,
			electTimeout: *electTimeout,
			settle:       *settle,
			clockFloor:   floor,
			gogc:         *gogc,
		})
	case "burst":
		res, err = runBurst(burstOpts{
			buildOpts:    buildOpts{n: *n, q1: *q1, q2: *q2, delay: *delay, sigma: *sigma, seed: *seed, payload: *payload, mae: *mae},
			label:        *label,
			bursts:       *bursts,
			opsPerBurst:  *opsPerBurst,
			idle:         *idle,
			conc:         *conc,
			timeout:      *timeout,
			electTimeout: *electTimeout,
			clockFloor:   floor,
			gogc:         *gogc,
			mae:          *mae,
		})
	case "shards":
		res, err = runShards(shardsOpts{
			label: *label,
			n:     *n, shards: *shardCount, domains: *domains,
			q1: *q1, q2: *q2,
			delay: *delay, sigma: *sigma, seed: *seed, payload: *payload, mae: *mae,
			kill: *killDomains, steady: *shardSteady, recover: *shardRecover,
			electT: *electTimeout, hbTO: *hbTO, elecTO: *elecTO,
			clockUs: floor.Microseconds(), gogc: *gogc,
			pollEvery: *shardPoll, maxInstances: *maxInst, settle: *shardSettle,
		})
	case "sweep":
		var ds []time.Duration
		for _, s := range strings.Split(*delays, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			d, derr := time.ParseDuration(s)
			if derr != nil {
				fmt.Fprintln(os.Stderr, "解析 -delays 失败:", derr)
				os.Exit(2)
			}
			ds = append(ds, d)
		}
		err = runSweep(sweepOpts{
			out: *out, n: *n, ops: *ops, conc: *conc, payload: *payload, warmup: *warmup,
			repeat: *repeat, duration: *duration, timeout: *timeout, electTimeout: *electTimeout,
			seed: *seed, sigma: *sigma, delays: ds, clockFloor: floor, settle: *settle, gogc: *gogc, mae: *mae,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "sweep 失败:", err)
			os.Exit(1)
		}
		return
	default:
		fmt.Fprintf(os.Stderr, "未知 mode %q\n", *mode)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "运行失败:", err)
		os.Exit(1)
	}

	data, merr := json.MarshalIndent(res, "", "  ")
	if merr != nil {
		fmt.Fprintln(os.Stderr, "序列化失败:", merr)
		os.Exit(1)
	}
	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "建目录失败:", err)
			os.Exit(1)
		}
		if err := os.WriteFile(*out, append(data, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "写文件失败:", err)
			os.Exit(1)
		}
		fmt.Printf("→ %s\n", *out)
	}
	fmt.Println(string(data))
}

// ─────────────────────────────────────────────────────────────────────
// bench 模式
// ─────────────────────────────────────────────────────────────────────

type benchOpts struct {
	buildOpts
	label        string
	ops, conc    int
	warmup       int
	repeat       int
	duration     time.Duration
	timeout      time.Duration
	electTimeout time.Duration
	settle       time.Duration
	dump         bool
	clockFloor   time.Duration
	gogc         int
}

// makeCmd 造一条带全局唯一序号的命令。
//
// ⚠️ 必须**每次新分配**，不能复用缓冲区。
// InmemStore.StoreLog 与 InmemTransport 传的都是 *Log / *AppendEntriesRequest
// 的引用，Log.Data 只是同一个底层 slice 的视图；复用缓冲区会在下一次
// 迭代时静默改写"已经提交"的日志内容。本 harness 的早期版本正是这么写的，
// 症状是各节点 applied 条数与 lastIndex 完全一致、但状态指纹 hash 不一致 ——
// 因为有的 follower 在缓冲区被改写前收到了条目，有的在之后。
// 真实系统里 FSM 也必须保证 Apply 后不再修改 payload。
func makeCmd(payload int, seq uint64) []byte {
	buf := make([]byte, payload)
	binary.BigEndian.PutUint64(buf[:8], seq)
	return buf
}

func runBench(o benchOpts) (*result, error) {
	progress("[bench] n=%d |Q1|=%d |Q2|=%d delay=%v sigma=%.2f ops=%d conc=%d repeat=%d",
		o.n, effectiveQuorum(o.n, o.q1), effectiveQuorum(o.n, o.q2), o.delay, o.sigma, o.ops, o.conc, o.repeat)

	cl, err := buildCluster(o.buildOpts)
	if err != nil {
		return nil, err
	}
	defer cl.shutdown()

	leader, err := cl.waitLeader(o.electTimeout)
	if err != nil {
		return nil, err
	}
	progress("集群已建立，leader = %s", leader.id)

	if o.warmup > 0 {
		if err := warmupRun(cl, o.warmup, 8, o.payload, o.timeout); err != nil {
			return nil, fmt.Errorf("预热失败: %w", err)
		}
		progress("预热完成（%d 条）", o.warmup)
	}

	var runs []runStats
	var rawLat []int64
	seq := uint64(o.warmup)

	for r := 0; r < o.repeat; r++ {
		st, raw, nextSeq, err := measureWindow(cl, o, seq)
		if err != nil {
			return nil, err
		}
		seq = nextSeq
		st.Index = r
		runs = append(runs, st)
		rawLat = append(rawLat, raw...)
		progress("  轮 %d：%d 条 / %.3fs = %.0f ops/s（p50=%.3fms p99=%.3fms）",
			r, st.Ops, st.ElapsedSec, st.Throughput, st.Latency.P50Ms, st.Latency.P99Ms)
	}

	res := assembleResult(cl, runs, o)
	res.RawLatencyNs = rawLat

	return res, nil
}

// measureWindow 跑一个测量窗口：并发提交直到 ops 上限或 duration 到期。
func measureWindow(cl *cluster, o benchOpts, startSeq uint64) (runStats, []int64, uint64, error) {
	cl.counters.appendEntries.Store(0)
	cl.counters.pipelineAE.Store(0)
	cl.counters.requestVote.Store(0)

	hist := bench.NewHistogram()
	var retries atomic.Int64
	var submitted atomic.Int64
	var rawLat []int64
	var rawMu sync.Mutex

	deadline := time.Time{}
	if o.duration > 0 {
		deadline = time.Now().Add(o.duration)
	}

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < o.conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if !deadline.IsZero() && time.Now().After(deadline) {
					return
				}
				idx := int(submitted.Add(1))
				if idx > o.ops {
					return
				}
				// leader 可能因法定人数不足而退位，需要重新解析。
				cur := cl.leader()
				if cur == nil {
					retries.Add(1)
					time.Sleep(time.Millisecond)
					continue
				}
				cmd := makeCmd(o.payload, startSeq+uint64(idx))
				t0 := time.Now()
				fut := cur.raft.Apply(cmd, o.timeout)
				if err := fut.Error(); err != nil {
					retries.Add(1)
					continue
				}
				ns := time.Since(t0).Nanoseconds()
				hist.Observe(uint64(ns))
				if o.dump {
					rawMu.Lock()
					rawLat = append(rawLat, ns)
					rawMu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	st := runStats{
		Ops:            int(hist.Count()),
		ElapsedSec:     elapsed.Seconds(),
		Latency:        hist.Snapshot(),
		LeaderRetries:  retries.Load(),
		AppendEntries:  cl.counters.appendEntries.Load(),
		PipelineAE:     cl.counters.pipelineAE.Load(),
		RequestVoteRPC: cl.counters.requestVote.Load(),
	}
	// 窗口结束瞬间的复制落后量：leader 的日志末尾 vs 最慢 follower 的日志末尾。
	//
	// 这是「leader 跑在多快、复制跟在多后面」的直接读数，与 §lag 模式的
	// 「ack 那一刻有几个副本有数据」互补：那个是逐条的瞬时值，这个是
	// 窗口末尾的累计落后。|Q2|=1 时 leader 不等任何人，这个数会明显为正。
	st.ReplicationGapMax = replicationGap(cl)
	if elapsed > 0 {
		st.Throughput = float64(st.Ops) / elapsed.Seconds()
	}
	denom := int64(st.Ops)
	if denom < 1 {
		denom = 1
	}
	st.AERPCPerOp = float64(st.AppendEntries+st.PipelineAE) / float64(denom)

	// 客户端饱和度：等待 Apply 的总时长 ÷ (并发 × 窗口)。
	sumLatNs := hist.Mean() * float64(hist.Count())
	if capNs := float64(o.conc) * elapsed.Seconds() * 1e9; capNs > 0 {
		st.ClientSaturation = sumLatNs / capNs
	}
	// Little 定律：W = L/λ。只用聚合量，不受时钟地板影响。
	if st.Throughput > 0 {
		st.DerivedAvgLatencyMs = float64(o.conc) / st.Throughput * 1000
	}

	// 重置复制落后量的采样：下一轮的窗口从新的基线开始。
	return st, rawLat, startSeq + uint64(submitted.Load()), nil
}

func warmupRun(cl *cluster, n, conc, payload int, timeout time.Duration) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	ch := make(chan uint64, conc)
	go func() {
		for i := 0; i < n; i++ {
			ch <- uint64(i)
		}
		close(ch)
	}()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range ch {
				cur := cl.leader()
				if cur == nil {
					continue
				}
				if err := cur.raft.Apply(makeCmd(payload, s), timeout).Error(); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func assembleResult(cl *cluster, runs []runStats, o benchOpts) *result {
	res := &result{
		Label:               o.label,
		Mode:                "bench",
		N:                   o.n,
		Q1:                  cl.q1,
		Q2:                  cl.q2,
		Majority:            majority(o.n),
		Concurrency:         o.conc,
		PayloadBytes:        o.payload,
		Repeat:              len(runs),
		InjectedDelayUs:     o.delay.Microseconds(),
		InjectedJitterSigma: o.sigma,
		ClockFloorUs:        o.clockFloor.Microseconds(),
		GCPercent:           o.gogc,
		Runs:                runs,
	}
	// MaxAppendEntries 生效值：未设时是上游默认 64。
	res.MaxAppendEntries = o.mae
	if res.MaxAppendEntries <= 0 {
		res.MaxAppendEntries = 64
	}
	// 单个 follower 的复制追赶上限 ≈ mae / 延迟（延迟为 0 时无意义）。
	// 这不是精确模型（还取决于 pipeline 深度、批次是否装满），
	// 但量级对得上就足够判断"leader 是否跑赢了复制"。
	if o.delay > 0 {
		res.ReplicationCeilingOps = float64(res.MaxAppendEntries) / o.delay.Seconds()
	}
	if res.Label == "" {
		res.Label = fmt.Sprintf("raftbench-n%d-q1%d-q2%d-d%v", o.n, cl.q1, cl.q2, o.delay)
	}
	if len(runs) > 0 {
		tps := make([]float64, 0, len(runs))
		totalOps := 0
		for _, r := range runs {
			tps = append(tps, r.Throughput)
			totalOps += r.Ops
		}
		sort.Float64s(tps)
		res.ThroughputLo = tps[0]
		res.ThroughputHi = tps[len(tps)-1]
		res.Throughput = tps[len(tps)/2]
		res.Ops = totalOps
	}

	// 延迟是否可信：用**实测分位数**判断，而不是只看注入延迟。
	//
	// 判据：p50 必须至少是时钟地板的 3 倍。低于它，样本会大量读成 0
	// 或一次跳变，分位数没有意义。
	//
	// 注意这与"注入延迟"不是一回事：在 -delay 0（纯内存介质）下，
	// 只要并发足够高，排队延迟本身就会远超时钟地板，此时 p50 是**可信**的
	// （它反映的是队列长度，不是单次 RPC 耗时）。所以判据必须看实测值。
	res.LatencyUsable = false
	if len(runs) > 0 {
		best := math.MaxFloat64
		for _, r := range runs {
			if r.Latency.P50Ms < best {
				best = r.Latency.P50Ms
			}
		}
		res.ObservedP50Ms = best
		floorMs := float64(max64(1, res.ClockFloorUs)) / 1000.0
		res.LatencyUsable = best >= 3*floorMs
	}

	res.AppliedPerNode, res.LastIdxPerNode, res.HashPerNode, res.Consistent, res.ConsistencyMsg = checkConsistency(cl, o.settle)

	res.Notes = []string{
		"传输为进程内 in-memory，日志在内存中，单机单进程：这是共识层微基准，不是端到端集群吞吐。",
		fmt.Sprintf("每条 AppendEntries RPC 注入单程延迟 LogNormal(中位数=%v, sigma=%.2f)；这是**注入**的，不是实测网络。", o.delay, o.sigma),
		fmt.Sprintf("|Q1|=%d, |Q2|=%d, n=%d, |Q1|+|Q2|=%d > n 满足 Flexible Paxos 安全性条件。", cl.q1, cl.q2, o.n, cl.q1+cl.q2),
		"q1=0/q2=0 时 flexiraft 与上游 hashicorp/raft 逐行等价，因此 majority 基线无实现差异混淆。",
		fmt.Sprintf("本机 Go 单调时钟地板 = %dµs（实测最小跳变）。低于此量级的延迟读数不可信。", res.ClockFloorUs),
	}
	if !res.LatencyUsable {
		res.Notes = append(res.Notes,
			fmt.Sprintf("⚠️ 实测 p50=%.3fms < 3×时钟地板 %.3fms：本轮**延迟分位数不可信**"+
				"（样本会大量读成 0 或一次时钟跳变）。请只看吞吐（跨秒窗口，噪声可平均掉）"+
				"或 runs[].derived_avg_latency_ms（Little 定律反推）。",
				res.ObservedP50Ms, float64(max64(1, res.ClockFloorUs))/1000.0))
	}
	if res.InjectedDelayUs == 0 {
		res.Notes = append(res.Notes,
			"本轮注入延迟为 0：这是**纯内存介质**的对照组。介质里没有网络等待，"+
				"因此 |Q2| 变小应当几乎没有收益 —— 这正是用来证伪「收益来自实现差异」的对照条件。")
	}
	// 复制可行性检查：|Q2| 变小的收益只在复制跟得上时才成立。
	if res.ReplicationCeilingOps > 0 && len(runs) > 0 {
		gapMax := uint64(0)
		for _, r := range runs {
			if r.ReplicationGapMax > gapMax {
				gapMax = r.ReplicationGapMax
			}
		}
		res.Notes = append(res.Notes,
			fmt.Sprintf("复制追赶上限估计 = MaxAppendEntries(%d) / 注入延迟(%v) ≈ %.0f ops/s（单个 follower）。"+
				"本轮实测吞吐 %.0f ops/s，窗口末最大落后 %d 条。",
				res.MaxAppendEntries, o.delay, res.ReplicationCeilingOps, res.Throughput, gapMax))
		if res.Throughput > res.ReplicationCeilingOps {
			res.Notes = append(res.Notes,
				fmt.Sprintf("⚠️ 实测吞吐超过复制追赶上限：**落后量会无界累积**。"+
					"这不是「更快」，是「欠债」—— 差额全压在 leader 的日志上。"+
					"要让 |Q2| 的收益真正成立，需要提高 -mae（批更多）或降低目标速率。"),
			)
		}
	}
	if len(runs) > 0 {
		lo, hi := 1.0, 0.0
		for _, r := range runs {
			if r.ClientSaturation < lo {
				lo = r.ClientSaturation
			}
			if r.ClientSaturation > hi {
				hi = r.ClientSaturation
			}
		}
		res.ClientSaturationLo, res.ClientSaturationHi = lo, hi
		if lo < 0.5 {
			res.Notes = append(res.Notes,
				fmt.Sprintf("⚠️ 客户端饱和度最低只有 %.2f：并发数不足以压满服务端，此时吞吐 ≈ 并发数 ÷ 延迟，"+
					"**不能**当作服务端容量来比较不同配置。要么提高 -concurrency，要么只看延迟。", lo))
		}
	}
	return res
}

// checkConsistency 等各节点追平，然后比对 (count, lastIndex, hash)。
//
// hash 一致比 count 一致强得多：它同时排除"两个节点在同一 index 应用了
// 不同命令"。这仍然不是形式化证明，但足以抓住 quorum 不相交、
// commit 规则写错这类灾难性缺陷。
//
// ⚠️ 必须给足追平时间。|Q2| 小于多数时，leader 会**跑在复制前面**
// （这正是它的收益来源），测量窗口结束时 follower 天然落后几万条；
// 而窗口结束后 leader 空闲，复制只能靠 CommitTimeout 心跳推进，
// 每个 AppendEntries 最多带 MaxAppendEntries（默认 64）条。
// 早先把上限设成 3 秒，于是所有 |Q2|=1 的配置都被误判为"不一致" ——
// 那不是数据丢失，是**还没抄完**。默认给 60 秒，并且区分
// 「收敛了」与「给了时间仍不收敛」。
func checkConsistency(cl *cluster, settle time.Duration) (applied, lastIdx, hashes []uint64, consistent bool, msg string) {
	deadline := time.Now().Add(settle)
	var gapAtStart uint64
	first := true
	for {
		applied = applied[:0]
		lastIdx = lastIdx[:0]
		minC, maxC := uint64(0), uint64(0)
		for i, nd := range cl.nodes {
			c := nd.fsm.applied.Load()
			applied = append(applied, c)
			lastIdx = append(lastIdx, nd.fsm.lastIdx.Load())
			if i == 0 || c < minC {
				minC = c
			}
			if i == 0 || c > maxC {
				maxC = c
			}
		}
		if first {
			gapAtStart = maxC - minC
			first = false
		}
		if minC == maxC {
			break
		}
		if time.Now().After(deadline) {
			return applied, lastIdx, hashOf(cl), false,
				fmt.Sprintf("给足 %v 仍未追平：已应用条数 min=%d max=%d（差 %d 条，"+
					"起始差 %d）。这更可能是复制吞吐不足而非数据丢失 —— "+
					"看 runs[].replication_gap_entries 与 |Q2| 的关系。",
					settle, minC, maxC, maxC-minC, gapAtStart)
		}
		time.Sleep(20 * time.Millisecond)
	}

	hashes = hashOf(cl)
	for i := 1; i < len(hashes); i++ {
		if hashes[i] != hashes[0] {
			return applied, lastIdx, hashes, false,
				fmt.Sprintf("节点间状态指纹不一致：node0 hash=%d node%d hash=%d", hashes[0], i, hashes[i])
		}
	}
	return applied, lastIdx, hashes, true, ""
}

func hashOf(cl *cluster) []uint64 {
	out := make([]uint64, 0, len(cl.nodes))
	for _, nd := range cl.nodes {
		out = append(out, nd.fsm.hash.Load())
	}
	return out
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// replicationGap 返回 leader 日志末尾与最慢存活 follower 日志末尾之差。
//
// 这个量只在 |Q2| 小于多数时才有意义：|Q2| 不小于多数时，leader 每次提交
// 都必须等够副本，日志末尾天然被压在一起；|Q2|=1 时 leader 完全不等人，
// 于是可以一路领先，落后的部分就是「已提交但尚未复制的日志」。
func replicationGap(cl *cluster) uint64 {
	var leaderLast, minFollowerLast uint64
	found := false
	for _, nd := range cl.nodes {
		last := nd.raft.LastIndex()
		if nd.raft.State() == fraft.Leader {
			leaderLast = last
			continue
		}
		if !found || last < minFollowerLast {
			minFollowerLast = last
			found = true
		}
	}
	if !found || leaderLast <= minFollowerLast {
		return 0
	}
	return leaderLast - minFollowerLast
}

// ─────────────────────────────────────────────────────────────────────
// avail 模式：不同 |Q1| 下，集群在杀掉 k 个节点后还能不能选出 leader
// ─────────────────────────────────────────────────────────────────────

type availOpts struct {
	buildOpts
	label        string
	trials       int
	kill         int
	electTimeout time.Duration
	clockFloor   time.Duration
}

func runAvail(o availOpts) (*result, error) {
	q1 := effectiveQuorum(o.n, o.q1)
	q2 := effectiveQuorum(o.n, o.q2)
	total := o.kill + 1 // 含 leader
	if total >= o.n {
		return nil, fmt.Errorf("kill+1 = %d 必须 < n = %d", total, o.n)
	}

	success, attempts := 0, 0
	var notes []string
	for t := 0; t < o.trials; t++ {
		opts := o.buildOpts
		opts.seed = o.seed + int64(t)*104729
		cl, err := buildCluster(opts)
		if err != nil {
			return nil, err
		}
		leader, err := cl.waitLeader(o.electTimeout)
		if err != nil {
			cl.shutdown()
			attempts++
			notes = append(notes, fmt.Sprintf("trial %d：初始选主就失败", t))
			continue
		}

		victims := map[*node]bool{leader: true}
		rnd := rand.New(rand.NewSource(o.seed + int64(t)))
		for len(victims) < total {
			nd := cl.nodes[rnd.Intn(len(cl.nodes))]
			victims[nd] = true
		}
		for nd := range victims {
			_ = nd.raft.Shutdown().Error()
			nd.raw.Close()
		}

		attempts++
		ok := false
		deadline := time.Now().Add(o.electTimeout)
		for time.Now().Before(deadline) {
			for _, nd := range cl.nodes {
				if !victims[nd] && nd.raft.State() == fraft.Leader {
					ok = true
				}
			}
			if ok {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if ok {
			success++
		}
		cl.shutdown()
	}

	rate := 0.0
	if attempts > 0 {
		rate = float64(success) / float64(attempts)
	}
	survivors := o.n - total
	res := &result{
		Mode:         "avail",
		Label:        o.label,
		N:            o.n,
		Q1:           q1,
		Q2:           q2,
		Majority:     majority(o.n),
		ClockFloorUs: o.clockFloor.Microseconds(),
		AvailTrials:  attempts,
		AvailSuccess: success,
		AvailKilled:  total,
		AvailRate:    rate,
		Notes: []string{
			fmt.Sprintf("杀 %d 个节点（含原 leader）后，%d 次试验中 %d 次成功选出新 leader（%.0f%%）。",
				total, attempts, success, rate*100),
			fmt.Sprintf("|Q1|=%d 需要 %d 票才能当选；存活节点数 = %d。存活数 < |Q1| 时**必然**选不出 leader。",
				q1, q1, survivors),
			"这一列是 FlexiRaft 取舍的代价侧：|Q2| 变小换来的写路径收益，是用 |Q1| 变大、选主更脆弱换的。",
			"选举试验用的注入延迟与 bench 模式相同；in-memory 介质下选举本身很快，结果主要由 |Q1| 与存活数的算术关系决定。",
		},
	}
	if o.label == "" {
		res.Label = fmt.Sprintf("raftbench-avail-n%d-q1%d-kill%d", o.n, q1, total)
	}
	res.Notes = append(res.Notes, notes...)
	return res, nil
}

// ─────────────────────────────────────────────────────────────────────
// lag 模式：量化「已提交」与「已复制」之间的差
//
// 为什么必须测这个 —— 它是 |Q2| 变小的**代价侧**，也是最容易被忽略的一点。
//
// |Q2|=1 意味着 leader 在**任何 follower 都还没有这条日志**的时候，
// 就把成功返回给客户端。安全性仍然成立：
// |Q1|+|Q2|>N 且 |Q1|=N，所以没有旧 leader 参与就选不出新 leader，
// 已提交的日志不会被回滚。但是：
//
//   - 这条数据此刻只存在于**一个副本**上。leader 的磁盘坏掉 = 真丢。
//     （崩溃重启不丢；介质丢失才丢。这是 FPaxos 安全性论证覆盖不到的部分。）
//   - leader 一旦挂掉，集群会一直不可用，直到它回来。
//
// 所以"|Q2| 变小换吞吐"这句话必须带上"提交时只有 k 个副本有数据"这个注脚。
// 本模式把那个 k 的分布测出来。
//
// 注意本模式是**串行提交**（并发 1）：并发下 leader 的流水线会让 follower
// 追上，测出来的 k 会被高估 —— 那样量的是稳态而不是提交瞬间。
// ─────────────────────────────────────────────────────────────────────

type lagOpts struct {
	buildOpts
	label        string
	samples      int
	timeout      time.Duration
	electTimeout time.Duration
	clockFloor   time.Duration
}

func runLag(o lagOpts) (*result, error) {
	cl, err := buildCluster(o.buildOpts)
	if err != nil {
		return nil, err
	}
	defer cl.shutdown()

	leader, err := cl.waitLeader(o.electTimeout)
	if err != nil {
		return nil, err
	}
	progress("lag: leader = %s（|Q1|=%d |Q2|=%d n=%d）", leader.id, cl.q1, cl.q2, o.n)
	if err := warmupRun(cl, 200, 4, o.payload, o.timeout); err != nil {
		return nil, err
	}

	hist := map[int]int{}
	sum, ok := 0, 0
	var seq uint64 = 1 << 40

	for i := 0; i < o.samples; i++ {
		seq++
		cur := cl.leader()
		if cur == nil {
			continue
		}
		fut := cur.raft.Apply(makeCmd(o.payload, seq), o.timeout)
		if err := fut.Error(); err != nil {
			continue
		}
		idx := fut.Index()

		// Apply 返回的**那一刻**，有几个副本的**日志里**已经有这条记录。
		//
		// 用 LastIndex()（日志已落盘到哪）而不是 AppliedIndex()（FSM 应用到哪）：
		// durability 关心的是"这条日志在几个副本上存在"，不是"有几个副本把它
		// 喂给了状态机"。FSM 是异步的，用 AppliedIndex 会连 leader 自己都算不进去
		// —— 早先版本就是这么写的，结果是三组配置全部读出 1.0，毫无区分度。
		replicas := 0
		for _, nd := range cl.nodes {
			if nd.raft.LastIndex() >= idx {
				replicas++
			}
		}
		hist[replicas]++
		sum += replicas
		ok++
	}

	res := &result{
		Mode:         "lag",
		N:            o.n,
		Q1:           cl.q1,
		Q2:           cl.q2,
		Majority:     majority(o.n),
		ClockFloorUs: o.clockFloor.Microseconds(),
		LagSamples:   ok,
	}
	if o.label == "" {
		res.Label = fmt.Sprintf("raftbench-lag-n%d-q1%d-q2%d", o.n, cl.q1, cl.q2)
	} else {
		res.Label = o.label
	}
	res.LagHistogram = make(map[string]int, len(hist))
	keys := make([]int, 0, len(hist))
	for k := range hist {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		res.LagHistogram[fmt.Sprintf("%d", k)] = hist[k]
	}
	if ok > 0 {
		res.LagMeanReplicas = float64(sum) / float64(ok)
	}

	res.Notes = []string{
		"串行提交（并发 1）、每条命令独立 payload；量的是 **Apply 返回成功那一瞬间**，" +
			"日志里已经有这条记录的副本数（用 LastIndex，不是 FSM 的 AppliedIndex）。",
		"并发下的流水线会让 follower 追上，所以这里刻意用串行 —— 量的是提交瞬间，不是稳态。",
		fmt.Sprintf("|Q1|=%d |Q2|=%d n=%d：|Q2| 就是提交所需的最小副本数（含 leader 自己），"+
			"分布的下界应当等于它。", cl.q1, cl.q2, o.n),
		"|Q2|=1 时下界是 1 —— 也就是说客户端收到成功时，数据可能只在一个副本上。",
		"这不违反 FPaxos 安全性（|Q1|+|Q2|>N 保证不回滚），但意味着**介质丢失**会丢已提交的数据，" +
			"且 leader 挂掉后集群会一直不可用直到它回来。这是 |Q2| 变小的真实代价。",
	}
	return res, nil
}

// ─────────────────────────────────────────────────────────────────────
// fail 模式：故障切换之后，已确认的写还在不在？
//
// 这是整篇论文的底线问题，也是唯一一个"错了就全盘皆输"的实验。
// FPaxos 用 |Q1|+|Q2| > N 从理论上保证已提交的日志不会被回滚
// （Howard et al., OPODIS 2016 §4.2）；这里把它变成可复现的实测：
//
//  1. 持续施压，客户端把**每一条收到成功响应的序号**记下来（acked 集合）
//  2. 在负载中途硬停 leader（Shutdown + 关传输），模拟真实故障
//  3. 等新 leader 选出（选不出就如实记录 —— 那本身就是 |Q1|=n 的代价）
//  4. 收尾后扫描**每个节点的日志**，检查 acked 集合是否是它的子集
//
// 判据是集合包含，不是条数比较：只比条数抓不住"换了内容"。
// 被杀的 leader 的日志也要查 —— 它的 store 在内存里还活着，
// 这正好直接验证"崩溃重启后数据仍在"。
//
// 注意这个实验**不能**证明安全性（那需要形式化证明或大量随机交错），
// 它能做的是**证伪**：只要有一次 acked 的写不见了，方案就是错的。
// 论文里必须这样表述。
// ─────────────────────────────────────────────────────────────────────

type failOpts struct {
	buildOpts
	label        string
	loadTime     time.Duration
	aftTime      time.Duration
	conc         int
	timeout      time.Duration
	electTimeout time.Duration
	settle       time.Duration
	clockFloor   time.Duration
	gogc         int
}

// collectAckedSeqs 扫描一个节点的日志，返回 seq -> 该 seq 所在的日志下标。
//
// 只认 LogCommand，且要求 Data 至少 8 字节 —— 配置项与 noop 的 Data
// 不是我们的序号编码，混进来会污染集合。
func collectAckedSeqs(nd *node) map[uint64]uint64 {
	out := make(map[uint64]uint64)
	last, err := nd.store.LastIndex()
	if err != nil {
		return out
	}
	var l fraft.Log
	for i := uint64(1); i <= last; i++ {
		if err := nd.store.GetLog(i, &l); err != nil {
			continue
		}
		if l.Type != fraft.LogCommand || len(l.Data) < 8 {
			continue
		}
		out[binary.BigEndian.Uint64(l.Data[:8])] = i
	}
	return out
}

func runFail(o failOpts) (*result, error) {
	progress("[fail] n=%d |Q1|=%d |Q2|=%d delay=%v 施压 %v 后杀 leader",
		o.n, effectiveQuorum(o.n, o.q1), effectiveQuorum(o.n, o.q2), o.delay, o.loadTime)

	cl, err := buildCluster(o.buildOpts)
	if err != nil {
		return nil, err
	}
	defer cl.shutdown()

	if _, err := cl.waitLeader(o.electTimeout); err != nil {
		return nil, err
	}
	if err := warmupRun(cl, 500, 8, o.payload, o.timeout); err != nil {
		return nil, fmt.Errorf("预热失败: %w", err)
	}
	seqBase := uint64(1) << 40 // 与预热的序号区间分开，避免混淆
	var nextSeq atomic.Uint64
	nextSeq.Store(seqBase)

	// ── 1. 持续施压，记录每一条收到成功的序号 ──
	var ackMu sync.Mutex
	acked := make(map[uint64]bool)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < o.conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				cur := cl.leader()
				if cur == nil {
					time.Sleep(time.Millisecond)
					continue
				}
				s := nextSeq.Add(1)
				if err := cur.raft.Apply(makeCmd(o.payload, s), o.timeout).Error(); err != nil {
					continue
				}
				ackMu.Lock()
				acked[s] = true
				ackMu.Unlock()
			}
		}()
	}

	time.Sleep(o.loadTime)

	// ── 2. 注入故障：硬停 leader ──
	//
	// 关键：必须在**杀之前**把已确认集合整份快照下来。杀了之后还会继续
	// 产生新的确认（切换成功时），那些写被杀掉的节点当然不会有 ——
	// 混在一起会得出错误的"丢数据"结论。
	victim := cl.leader()
	if victim == nil {
		close(stop)
		wg.Wait()
		return nil, fmt.Errorf("施压期间没有 leader，故障注入无法进行")
	}
	victimLastBefore := victim.raft.LastIndex()

	ackMu.Lock()
	ackedBeforeSet := make(map[uint64]bool, len(acked))
	for s := range acked {
		ackedBeforeSet[s] = true
	}
	ackedBeforeCount := len(ackedBeforeSet)
	ackMu.Unlock()

	progress("故障注入：硬停 leader %s（此前已确认 %d 条写，其日志末尾 %d）",
		victim.id, ackedBeforeCount, victimLastBefore)
	if err := victim.raft.Shutdown().Error(); err != nil {
		progress("  警告：Shutdown 返回 %v", err)
	}
	victim.raw.Close()

	// ── 3. 等新 leader；选不出来就如实记录 ──
	failStart := time.Now()
	var newLeader *node
	deadline := time.Now().Add(o.electTimeout)
	for time.Now().Before(deadline) {
		for _, nd := range cl.nodes {
			if nd != victim && nd.raft.State() == fraft.Leader {
				newLeader = nd
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	failoverMs := float64(time.Since(failStart).Microseconds()) / 1000.0
	if newLeader != nil {
		progress("  新 leader = %s，耗时 %.1fms", newLeader.id, failoverMs)
	} else {
		progress("  %.0fms 内没有选出新 leader（存活 %d < |Q1|=%d 时这是**必然**的）",
			failoverMs, o.n-1, cl.q1)
	}

	// ── 4. 若已选出新 leader，再压一段时间，检验切换后仍能正常服务 ──
	if newLeader != nil && o.aftTime > 0 {
		time.Sleep(o.aftTime)
	}
	close(stop)
	wg.Wait()

	ackMu.Lock()
	ackedFinal := make([]uint64, 0, len(acked))
	for s := range acked {
		ackedFinal = append(ackedFinal, s)
	}
	ackMu.Unlock()

	// ── 5. 等存活节点追平，然后判定安全性 ──
	//
	// ⚠️ 判据必须分三层，混在一起会得出错误的"丢数据"结论。
	// 第一版就是这么错的：它拿**全时段**的 acked 集合去查每个节点，
	// 于是被杀掉的 leader 必然"缺"了它死后才被确认的那些写 ——
	// 那不是丢数据，那是它已经死了。
	//
	//   ① 无丢失（durability）：任何 acked 的写必须存在于**至少一个**节点的日志里
	//   ② 无回滚（consistency）：**杀之前**已确认的写必须都在**新 leader** 的日志里
	//   ③ 崩溃可恢复：被杀 leader 自己的日志必须包含它确认过的**全部**写
	//
	// ③ 是 |Q2| 变小时最该盯的一条：|Q2|=1 时 leader 确认的写可能只有它自己有，
	// 所以它一挂，那些写就"寄存在一个死节点上"，直到它回来。
	// 这不是丢失，但确实是 |Q2|=1 的真实风险面。
	settleDeadline := time.Now().Add(o.settle)
	for {
		converged := true
		for _, nd := range cl.nodes {
			if nd == victim {
				continue
			}
			seqs := collectAckedSeqs(nd)
			for s := range ackedBeforeSet {
				if _, ok := seqs[s]; !ok {
					converged = false
					break
				}
			}
			if !converged {
				break
			}
		}
		if converged || time.Now().After(settleDeadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	names := make([]string, 0, len(cl.nodes))
	for _, nd := range cl.nodes {
		names = append(names, string(nd.id))
	}

	// 收集每个节点的日志序号集合，只扫一次。
	perNode := make([]map[uint64]uint64, len(cl.nodes))
	withAll := make([]bool, len(cl.nodes))
	missingCounts := make([]int, len(cl.nodes))
	for i, nd := range cl.nodes {
		perNode[i] = collectAckedSeqs(nd)
		missing := 0
		for _, s := range ackedFinal {
			if _, ok := perNode[i][s]; !ok {
				missing++
			}
		}
		missingCounts[i] = missing
		withAll[i] = missing == 0
	}

	// ① 无丢失：每条 acked 的写至少存在于一个节点。
	lostTotal := 0
	var missingMax uint64
	for _, s := range ackedFinal {
		found := false
		for i := range cl.nodes {
			if _, ok := perNode[i][s]; ok {
				found = true
				break
			}
		}
		if !found {
			lostTotal++
			if s > missingMax {
				missingMax = s
			}
		}
	}

	victimIdx := -1
	for i, nd := range cl.nodes {
		if nd == victim {
			victimIdx = i
		}
	}

	// ② 无回滚：杀之前确认的写必须都在新 leader 的日志里。
	newLeaderMissing := -1 // -1 表示没有新 leader，该项不适用
	if newLeader != nil {
		newLeaderMissing = 0
		nl := collectAckedSeqs(newLeader)
		for s := range ackedBeforeSet {
			if _, ok := nl[s]; !ok {
				newLeaderMissing++
			}
		}
	}

	// ③ 崩溃可恢复：被杀 leader 的日志必须含它确认过的全部写。
	victimMissing := 0
	if victimIdx >= 0 {
		for s := range ackedBeforeSet {
			if _, ok := perNode[victimIdx][s]; !ok {
				victimMissing++
			}
		}
	}

	victimLastAfter := victim.raft.LastIndex()

	res := &result{
		Mode:             "fail",
		N:                o.n,
		Q1:               cl.q1,
		Q2:               cl.q2,
		Majority:         majority(o.n),
		Ops:              len(ackedFinal),
		Concurrency:      o.conc,
		PayloadBytes:     o.payload,
		InjectedDelayUs:  o.delay.Microseconds(),
		ClockFloorUs:     o.clockFloor.Microseconds(),
		GCPercent:        o.gogc,
		MaxAppendEntries: o.mae,

		FailAckedWrites:      len(ackedFinal),
		FailAckedBefore:      ackedBeforeCount,
		FailAckedAfter:       len(ackedFinal) - ackedBeforeCount,
		FailLostWrites:       lostTotal,
		FailMissingMax:       missingMax,
		FailNewLeaderMissing: newLeaderMissing,
		FailVictimMissing:    victimMissing,
		FailSafetyOK:         lostTotal == 0 && newLeaderMissing <= 0 && victimMissing == 0,
		FailoverMs:           failoverMs,
		FailNewLeaderOK:      newLeader != nil,
		FailVictim:           string(victim.id),
		FailNodesWithAll:     withAll,
		FailNodeMissing:      missingCounts,
		FailNodeNames:        names,
		FailVictimBefore:     victimLastBefore,
		FailVictimAfter:      victimLastAfter,
	}
	if newLeader != nil {
		res.FailNewLeader = string(newLeader.id)
	}
	if o.label == "" {
		res.Label = fmt.Sprintf("raftbench-fail-n%d-q1%d-q2%d-d%v", o.n, cl.q1, cl.q2, o.delay)
	} else {
		res.Label = o.label
	}

	res.Notes = []string{
		"故障注入：负载中途硬停 leader（Shutdown + 关传输），随后分三层判定安全性。",
		"① 无丢失：每条已确认的写必须存在于**至少一个**节点的日志里。",
		"② 无回滚：**杀之前**已确认的写必须都在**新 leader** 的日志里。",
		"③ 崩溃可恢复：被杀 leader 自己的日志必须包含它确认过的**全部**写。",
		"⚠️ 三层必须分开看。第一版实现把它们混在一起，于是被杀 leader 必然" +
			"「缺」了它死后才确认的写 —— 那不是丢数据，是它已经死了。",
		"⚠️ 这个实验**不能证明**安全性（那需要形式化证明或大量随机交错），它能做的是**证伪**：" +
			"只要三层里任何一层出问题，方案就是错的。论文里必须这样表述。",
		fmt.Sprintf("|Q1|=%d |Q2|=%d n=%d：|Q1|+|Q2|=%d > n，满足 FPaxos 安全性条件。",
			cl.q1, cl.q2, o.n, cl.q1+cl.q2),
	}
	switch {
	case lostTotal > 0:
		res.Notes = append(res.Notes,
			fmt.Sprintf("❌ **① 无丢失被证伪**：%d 条已确认的写在任何节点的日志里都找不到（最大缺失序号 %d）。"+
				"这是灾难性缺陷 —— quorum 不相交或提交规则有错。", lostTotal, missingMax))
	case newLeaderMissing > 0:
		res.Notes = append(res.Notes,
			fmt.Sprintf("❌ **② 无回滚被证伪**：新 leader %s 缺少 %d 条切换前已确认的写。"+
				"选举限制没起作用。", newLeader.id, newLeaderMissing))
	case victimMissing > 0:
		res.Notes = append(res.Notes,
			fmt.Sprintf("❌ **③ 崩溃可恢复被证伪**：被杀的 leader %s 缺少 %d 条它自己确认过的写。",
				victim.id, victimMissing))
	default:
		res.Notes = append(res.Notes,
			fmt.Sprintf("✅ 三层全部通过：%d 条已确认的写（切换前 %d / 切换后 %d）没有任何一条丢失或被回滚。",
				len(ackedFinal), ackedBeforeCount, len(ackedFinal)-ackedBeforeCount))
	}
	if newLeader == nil {
		res.Notes = append(res.Notes,
			fmt.Sprintf("本轮没有选出新 leader：存活 %d 个副本 < |Q1|=%d（%.0fms 内未选出）。"+
				"这是 |Q1| 变大的**代价**，不是缺陷 —— 已确认的写仍然安全"+
				"（在被杀 leader 的日志里，③ 已验证），但集群在此期间不可用。", o.n-1, cl.q1, failoverMs))
	}
	// |Q2| 小于多数时，leader 会跑在复制前面 —— 它一挂，那些"已确认但还没
	// 复制出去"的写就寄存在一个死节点上，直到它回来。这不是丢失（① 已验），
	// 但它是 |Q2| 变小的真实风险面，量出来给读者看。
	if victimIdx >= 0 && newLeader == nil {
		res.Notes = append(res.Notes,
			fmt.Sprintf("被杀 leader %s 的日志里有 %d 条命令，其余节点因无人可复制而停在原处。"+
				"|Q2| 小于多数时，这部分就是「寄存在死节点上的已确认写」——"+
				"安全性没问题（原 leader 重启后数据仍在），但这段时间集群既不可用、"+
				"数据也只有一份。", victim.id, len(perNode[victimIdx])))
	}
	return res, nil
}

// ─────────────────────────────────────────────────────────────────────
// burst 模式：同步突发负载 —— 千卡训练里 checkpoint 的形状
//
// 为什么必须单独做这个模式：真实千卡训练**不是稳态 QPS**。
// 每 N 步所有 rank 同时写 checkpoint，几千个客户端在几秒内一起打，
// 打完就安静。稳态基准测不到这种形状，而它恰恰是 quorum 几何最吃紧的时刻：
// 突发期间 leader 是唯一瓶颈，|Q2| 直接决定每个写要等多久才能提交。
//
// 具体做法：把施压切成 K 个突发。每个突发内并发提交固定条数，
// 记录「发起到全部完成」的耗时（drain time，也就是训练侧的停顿时间）；
// 突发之间完全静默（模拟两个 checkpoint 之间的训练步）。
//
// 主指标是 **drain time**，不是 ops/s —— 因为它直接对应
// 「checkpoint 让训练停了多久」这个运维真正关心的数。
// ─────────────────────────────────────────────────────────────────────

type burstOpts struct {
	buildOpts
	label        string
	bursts       int
	opsPerBurst  int
	idle         time.Duration
	conc         int
	timeout      time.Duration
	electTimeout time.Duration
	clockFloor   time.Duration
	gogc         int
	mae          int
}

func percentileOfSorted(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func runBurst(o burstOpts) (*result, error) {
	progress("[burst] n=%d |Q1|=%d |Q2|=%d delay=%v 突发 %d×%d 条 间隔 %v 并发 %d",
		o.n, effectiveQuorum(o.n, o.q1), effectiveQuorum(o.n, o.q2), o.delay,
		o.bursts, o.opsPerBurst, o.idle, o.conc)

	cl, err := buildCluster(o.buildOpts)
	if err != nil {
		return nil, err
	}
	defer cl.shutdown()

	if _, err := cl.waitLeader(o.electTimeout); err != nil {
		return nil, err
	}
	if err := warmupRun(cl, 500, 8, o.payload, o.timeout); err != nil {
		return nil, fmt.Errorf("预热失败: %w", err)
	}

	var seqBase uint64 = 1 << 41
	drains := make([]float64, 0, o.bursts)
	thrs := make([]float64, 0, o.bursts)
	perBurstLat := make([]bench.Snapshot, 0, o.bursts)

	for b := 0; b < o.bursts; b++ {
		hist := bench.NewHistogram()
		var wg sync.WaitGroup
		var issued atomic.Int64
		burstBase := seqBase + uint64(b)*uint64(o.opsPerBurst+o.conc+16)

		start := time.Now()
		for w := 0; w < o.conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(issued.Add(1)) - 1
					if i >= o.opsPerBurst {
						return
					}
					cur := cl.leader()
					if cur == nil {
						time.Sleep(time.Millisecond)
						continue
					}
					t0 := time.Now()
					if err := cur.raft.Apply(makeCmd(o.payload, burstBase+uint64(i)), o.timeout).Error(); err != nil {
						continue
					}
					hist.Observe(uint64(time.Since(t0).Nanoseconds()))
				}
			}()
		}
		wg.Wait()
		drain := time.Since(start).Seconds() * 1000.0

		drains = append(drains, drain)
		if drain > 0 {
			thrs = append(thrs, float64(hist.Count())/(drain/1000.0))
		}
		perBurstLat = append(perBurstLat, hist.Snapshot())
		progress("  突发 %d：%d 条 / %.1fms = %.0f ops/s（p50=%.2fms p99=%.2fms）",
			b, hist.Count(), drain, float64(hist.Count())/(drain/1000.0),
			hist.Snapshot().P50Ms, hist.Snapshot().P99Ms)

		if b < o.bursts-1 && o.idle > 0 {
			time.Sleep(o.idle)
		}
	}

	dSorted := append([]float64(nil), drains...)
	sort.Float64s(dSorted)
	tSorted := append([]float64(nil), thrs...)
	sort.Float64s(tSorted)

	res := &result{
		Mode:             "burst",
		N:                o.n,
		Q1:               cl.q1,
		Q2:               cl.q2,
		Majority:         majority(o.n),
		Concurrency:      o.conc,
		PayloadBytes:     o.payload,
		InjectedDelayUs:  o.delay.Microseconds(),
		ClockFloorUs:     o.clockFloor.Microseconds(),
		GCPercent:        o.gogc,
		MaxAppendEntries: o.mae,
		BurstCount:       o.bursts,
		BurstOpsEach:     o.opsPerBurst,
		BurstDrainMs:     drains,
		BurstDrainP50:    percentileOfSorted(dSorted, 0.50),
		BurstDrainP99:    percentileOfSorted(dSorted, 0.99),
		BurstDrainMin:    percentileOfSorted(dSorted, 0),
		BurstDrainMax:    percentileOfSorted(dSorted, 1),
		BurstThrP50:      percentileOfSorted(tSorted, 0.50),
		PerBurstLatency:  perBurstLat,
	}
	if o.label == "" {
		res.Label = fmt.Sprintf("raftbench-burst-n%d-q1%d-q2%d-d%v", o.n, cl.q1, cl.q2, o.delay)
	} else {
		res.Label = o.label
	}

	res.AppliedPerNode, res.LastIdxPerNode, res.HashPerNode, res.Consistent, res.ConsistencyMsg = checkConsistency(cl, o.settleFor())

	res.Notes = []string{
		"负载形状：同步突发（每个突发内并发提交固定条数，突发之间静默）—— 模拟千卡训练的 checkpoint。",
		"主指标是 **drain time**（发起到全部完成的耗时），因为它直接对应「checkpoint 让训练停了多久」。",
		"稳态基准测不到这个形状：突发期间 leader 是唯一瓶颈，|Q2| 直接决定每个写要等多久。",
		fmt.Sprintf("|Q1|=%d |Q2|=%d n=%d，注入单程延迟 %v。", cl.q1, cl.q2, o.n, o.delay),
	}
	return res, nil
}

func (o burstOpts) settleFor() time.Duration { return 60 * time.Second }

// ─────────────────────────────────────────────────────────────────────
// sweep：扫 (|Q1|,|Q2|) × 注入延迟，输出 JSON 数组
// ─────────────────────────────────────────────────────────────────────

type sweepOpts struct {
	out          string
	n, ops, conc int
	payload      int
	warmup       int
	repeat       int
	duration     time.Duration
	timeout      time.Duration
	electTimeout time.Duration
	seed         int64
	sigma        float64
	delays       []time.Duration
	clockFloor   time.Duration
	settle       time.Duration
	gogc         int
	mae          int
}

// configs 生成满足 |Q1|+|Q2| > n 的 (Q1,Q2) 组合。
//
// |Q2| 从多数一路降到 1，|Q1| 相应升到 n-|Q2|+1 —— 这是 Flexible Paxos
// 允许的极值方向：热路径（写）越来越便宜，冷路径（选主）越来越贵。
// 另外补上 (n, 多数) 这一档：|Q2| 保持多数、|Q1| 升到 n，
// 用来看"只把选举 quorum 放大"的纯代价。
func sweepConfigs(n int) [][2]int {
	var out [][2]int
	for q2 := majority(n); q2 >= 1; q2-- {
		q1 := n - q2 + 1
		if q1 < majority(n) {
			q1 = majority(n)
		}
		if q1 > n {
			q1 = n
		}
		if q1+q2 <= n {
			continue
		}
		out = append(out, [2]int{q1, q2})
	}
	// 去重
	seen := map[[2]int]bool{}
	uniq := out[:0]
	for _, c := range out {
		if !seen[c] {
			seen[c] = true
			uniq = append(uniq, c)
		}
	}
	return uniq
}

func runSweep(o sweepOpts) error {
	if o.out == "" {
		return fmt.Errorf("sweep 模式必须指定 -out")
	}
	cfgs := sweepConfigs(o.n)
	var all []*result

	for _, d := range o.delays {
		progress("=== 注入延迟 %v ===", d)
		for _, cfg := range cfgs {
			r, err := runBench(benchOpts{
				buildOpts:    buildOpts{n: o.n, q1: cfg[0], q2: cfg[1], delay: d, sigma: o.sigma, seed: o.seed, payload: o.payload, mae: o.mae},
				ops:          o.ops,
				conc:         o.conc,
				warmup:       o.warmup,
				repeat:       o.repeat,
				duration:     o.duration,
				timeout:      o.timeout,
				electTimeout: o.electTimeout,
				settle:       o.settle,
				clockFloor:   o.clockFloor,
				gogc:         o.gogc,
			})
			if err != nil {
				return fmt.Errorf("q1=%d q2=%d delay=%v: %w", cfg[0], cfg[1], d, err)
			}
			all = append(all, r)
			progress("  |Q1|=%d |Q2|=%d  吞吐 %.0f ops/s（%.0f–%.0f）  RPC/op %.2f  p50=%.3fms",
				r.Q1, r.Q2, r.Throughput, r.ThroughputLo, r.ThroughputHi,
				r.Runs[len(r.Runs)-1].AERPCPerOp, r.Runs[len(r.Runs)-1].Latency.P50Ms)
		}
	}

	if err := os.MkdirAll(filepath.Dir(o.out), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(all, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(o.out, append(data, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("→ %s（%d 条配置）\n", o.out, len(all))
	return nil
}
