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
	"math/rand"
	"os"
	"path/filepath"
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
		cfg := newConfig(nd.id, q1, q2)
		if err := fraft.BootstrapCluster(cfg, nd.store, nd.store, nd.snaps, nd.trans, configuration); err != nil {
			cl.shutdown()
			return nil, fmt.Errorf("引导 %s 失败: %w", nd.id, err)
		}
	}

	// 4. 启动。
	for _, nd := range cl.nodes {
		cfg := newConfig(nd.id, q1, q2)
		r, err := fraft.NewRaft(cfg, nd.fsm, nd.store, nd.store, nd.snaps, nd.trans)
		if err != nil {
			cl.shutdown()
			return nil, fmt.Errorf("启动 %s 失败: %w", nd.id, err)
		}
		nd.raft = r
	}
	return cl, nil
}

func newConfig(id fraft.ServerID, q1, q2 int) *fraft.Config {
	cfg := fraft.DefaultConfig()
	cfg.LocalID = id
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.LeaderLeaseTimeout = 25 * time.Millisecond
	cfg.SnapshotThreshold = 1 << 22 // 基准期间不触发快照
	cfg.SnapshotInterval = time.Hour
	cfg.TrailingLogs = 1 << 22
	cfg.NoSnapshotRestoreOnStart = true
	cfg.LogLevel = "ERROR"
	cfg.DataQuorumSize = q2
	cfg.ElectionQuorumSize = q1
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
	AvailTrials  int     `json:"avail_trials,omitempty"`
	AvailSuccess int     `json:"avail_success,omitempty"`
	AvailKilled  int     `json:"avail_killed,omitempty"`
	AvailRate    float64 `json:"avail_success_rate,omitempty"`

	Notes []string `json:"notes"`
}

func main() {
	mode := flag.String("mode", "bench", "bench | avail | sweep")
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
	flag.Parse()

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
			buildOpts: buildOpts{n: *n, q1: *q1, q2: *q2, delay: *delay, sigma: *sigma, seed: *seed, payload: *payload},
			label:     *label, ops: *ops, conc: *conc, warmup: *warmup, repeat: *repeat,
			duration: *duration, timeout: *timeout, electTimeout: *electTimeout,
			dump: *dump, clockFloor: floor,
		})
	case "avail":
		res, err = runAvail(availOpts{
			buildOpts: buildOpts{n: *n, q1: *q1, q2: *q2, delay: *delay, sigma: *sigma, seed: *seed, payload: *payload},
			label:     *label, trials: *trials, kill: *kill, electTimeout: *electTimeout, clockFloor: floor,
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
			seed: *seed, sigma: *sigma, delays: ds, clockFloor: floor,
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
	dump         bool
	clockFloor   time.Duration
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
	if elapsed > 0 {
		st.Throughput = float64(st.Ops) / elapsed.Seconds()
	}
	denom := int64(st.Ops)
	if denom < 1 {
		denom = 1
	}
	st.AERPCPerOp = float64(st.AppendEntries+st.PipelineAE) / float64(denom)
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
		Runs:                runs,
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

	// 延迟是否可信：注入延迟必须显著高于本机时钟地板。
	// 门槛取 3× 地板 —— 低于这个量级，分位数的桶宽和时钟跳变同阶。
	res.LatencyUsable = res.InjectedDelayUs >= 3*max64(1, res.ClockFloorUs)

	res.AppliedPerNode, res.LastIdxPerNode, res.HashPerNode, res.Consistent, res.ConsistencyMsg = checkConsistency(cl)

	res.Notes = []string{
		"传输为进程内 in-memory，日志在内存中，单机单进程：这是共识层微基准，不是端到端集群吞吐。",
		fmt.Sprintf("每条 AppendEntries RPC 注入单程延迟 LogNormal(中位数=%v, sigma=%.2f)；这是**注入**的，不是实测网络。", o.delay, o.sigma),
		fmt.Sprintf("|Q1|=%d, |Q2|=%d, n=%d, |Q1|+|Q2|=%d > n 满足 Flexible Paxos 安全性条件。", cl.q1, cl.q2, o.n, cl.q1+cl.q2),
		"q1=0/q2=0 时 flexiraft 与上游 hashicorp/raft 逐行等价，因此 majority 基线无实现差异混淆。",
		fmt.Sprintf("本机 Go 单调时钟地板 = %dµs（实测最小跳变）。低于此量级的延迟读数不可信。", res.ClockFloorUs),
	}
	if !res.LatencyUsable {
		res.Notes = append(res.Notes,
			fmt.Sprintf("⚠️ 注入延迟 %dµs < 3×时钟地板 %dµs：本轮**延迟分位数不可信**（大量样本会读成 0）。"+
				"请只看吞吐（跨秒窗口，噪声可平均掉），或把 -delay 提到 1ms 以上。",
				res.InjectedDelayUs, res.ClockFloorUs))
	}
	return res
}

// checkConsistency 等各节点追平，然后比对 (count, lastIndex, hash)。
//
// hash 一致比 count 一致强得多：它同时排除"两个节点在同一 index 应用了
// 不同命令"。这仍然不是形式化证明，但足以抓住 quorum 不相交、
// commit 规则写错这类灾难性缺陷。
func checkConsistency(cl *cluster) (applied, lastIdx, hashes []uint64, consistent bool, msg string) {
	deadline := time.Now().Add(3 * time.Second)
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
		if minC == maxC || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	hashes = hashes[:0]
	for _, nd := range cl.nodes {
		hashes = append(hashes, nd.fsm.hash.Load())
	}
	for i := 1; i < len(hashes); i++ {
		if hashes[i] != hashes[0] {
			return applied, lastIdx, hashes, false,
				fmt.Sprintf("节点间状态指纹不一致：node0 hash=%d node%d hash=%d", hashes[0], i, hashes[i])
		}
	}
	for i := 1; i < len(applied); i++ {
		if applied[i] != applied[0] {
			return applied, lastIdx, hashes, false,
				fmt.Sprintf("追平后已应用条数仍不一致：node0=%d node%d=%d", applied[0], i, applied[i])
		}
	}
	return applied, lastIdx, hashes, true, ""
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
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
				buildOpts:    buildOpts{n: o.n, q1: cfg[0], q2: cfg[1], delay: d, sigma: o.sigma, seed: o.seed, payload: o.payload},
				ops:          o.ops,
				conc:         o.conc,
				warmup:       o.warmup,
				repeat:       o.repeat,
				duration:     o.duration,
				timeout:      o.timeout,
				electTimeout: o.electTimeout,
				clockFloor:   o.clockFloor,
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
