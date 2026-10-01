// pkg/sim/sim.go
//
// 离散事件模拟器：把实测结论外推到无法真实部署的规模（1000 节点）。
//
// ⚠️ 定位与边界（这一段决定了模拟器结果能不能写进论文）：
//
// 本模拟器**不是**性能预测器。它建模的是**成本结构**：
// 消息数、fsync 次数、跨域字节数、各路径的可用性。
// 它**不**建模 CPU 调度、页面缓存、TCP 拥塞、锁竞争、GC 停顿。
//
// 因此它只能回答"趋势与量级"问题，例如：
//   - 分片数从 16 增到 1000 时，放置策略的可用性收益是放大还是衰减？
//   - 消息数上限随 n 如何变化？
// 它**不能**回答"这个配置能跑多少 QPS"。
//
// 三类结果必须严格区分（docs/DESIGN.md §4.6）：
//  1. 实测（pkg/cluster + cmd/kvbench）—— 可作绝对性能声明
//  2. 模拟（本包）                      —— 只能作趋势预测
//  3. 解析（pkg/model + pkg/faft）      —— 只能作成本界
//
// 校准要求：模拟器必须在**能实测的规模**上与实测对照（见 calib.go），
// 给出误差曲线与开始偏离的规模点，并在论文中披露。
package sim

import (
	"fmt"
	"math"
	"sort"

	"github.com/distributed-kv/kvstore/pkg/faft"
)

// Config 模拟配置。
type Config struct {
	// Shards 分片数（每个分片是独立的 Raft group）。
	Shards int
	// Replicas 每分片副本数。
	Replicas int
	// Domains 故障域数。
	Domains int

	// LogBytes 单条日志载荷字节数。
	LogBytes int
	// FsyncLatencyUs 单次 fsync 延迟（微秒）。
	FsyncLatencyUs float64
	// BatchSize 单个日志条目打包的命令数。
	BatchSize int
	// LeaderNICGbps leader 网卡速率。
	LeaderNICGbps float64
	// HeartbeatIntervalMs 心跳间隔。
	HeartbeatIntervalMs float64
	// CrossDomainRTTMs 跨域往返延迟。
	CrossDomainRTTMs float64
	// ReplicaFailProb 单副本失效率。
	ReplicaFailProb float64
	// RegionalEventProb 区域事件概率。
	RegionalEventProb float64
	// RegionalEventDomains 区域事件影响域数。
	RegionalEventDomains int

	// ReadRatio 读操作占比（用于加权延迟）。
	ReadRatio float64
	// TargetOpsPerSec 目标总速率，用于判断哪条路径先饱和。
	TargetOpsPerSec float64
}

func (c Config) withDefaults() Config {
	if c.Shards <= 0 {
		c.Shards = 1
	}
	if c.Replicas <= 0 {
		c.Replicas = 5
	}
	if c.Domains <= 0 {
		c.Domains = 3
	}
	if c.LogBytes <= 0 {
		c.LogBytes = 256
	}
	if c.FsyncLatencyUs <= 0 {
		c.FsyncLatencyUs = 100
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1
	}
	if c.LeaderNICGbps <= 0 {
		c.LeaderNICGbps = 1
	}
	if c.HeartbeatIntervalMs <= 0 {
		c.HeartbeatIntervalMs = 100
	}
	if c.ReplicaFailProb <= 0 {
		c.ReplicaFailProb = 1e-4
	}
	if c.ReadRatio < 0 {
		c.ReadRatio = 0
	}
	if c.ReadRatio > 1 {
		c.ReadRatio = 1
	}
	return c
}

// Result 模拟结果。每个字段都标注了它是"成本结构"还是"趋势量"，
// 以免被误当作性能预测。
type Result struct {
	Config Config `json:"config"`

	// ---- 成本结构（可精确计算，与实测对照的是这几项）----

	// FsyncPerOp 每条命令平均触发的 fsync 次数 = 1/BatchSize（写路径）。
	FsyncPerOp float64 `json:"fsync_per_op"`
	// WriteLatencyMs 稳态写延迟估计（一个到 Q2 的往返 + 一次批 fsync）。
	WriteLatencyMs float64 `json:"write_latency_ms"`
	// ReadLatencyMs 稳态读延迟估计（readIndex 语义下无额外往返）。
	ReadLatencyMs float64 `json:"read_latency_ms"`

	// WriteMsgsPerOp 一次写的消息数 = 2|Q2|。
	WriteMsgsPerOp int `json:"write_msgs_per_op"`
	// HeartbeatMsgsPerSec 全集群心跳消息速率。
	HeartbeatMsgsPerSec float64 `json:"heartbeat_msgs_per_sec"`
	// HeartbeatBytesPerSec 全集群心跳字节速率。
	HeartbeatBytesPerSec float64 `json:"heartbeat_bytes_per_sec"`

	// PerLeaderEgressCeiling 单分片 leader 出口带宽上限（写路径，ops/s）。
	PerLeaderEgressCeiling float64 `json:"per_leader_egress_ceiling"`
	// PerLeaderDiskCeiling 单分片 leader 磁盘上限（写路径，ops/s）。
	PerLeaderDiskCeiling float64 `json:"per_leader_disk_ceiling"`
	// PerLeaderWriteCeiling 单分片写吞吐上限 = min(出口带宽, 磁盘)。
	PerLeaderWriteCeiling float64 `json:"per_leader_write_ceiling"`
	// ClusterWriteCeiling 全集群写吞吐上限 = 分片数 × 单分片写上限。
	ClusterWriteCeiling float64 `json:"cluster_write_ceiling"`

	// ClusterReadCeiling 全集群读吞吐上限。
	//
	// 读路径与写路径的模型完全不同，必须分开报：
	//   - 写要付一次批 fsync（b/t_fsync）与一次到 Q2 的往返；
	//   - readIndex 语义下的读**不落盘**，只受"每批一次心跳往返"限制。
	// 用写路径的 fsync 上限去预测读吞吐会低估好几个数量级 ——
	// 实测中读密集场景的吞吐超过写路径上限 5.4 倍，就是这个错误的直接证据。
	ClusterReadCeiling float64 `json:"cluster_read_ceiling"`
	// ReadHeartbeatRTTMs 读路径每批一次心跳往返的延迟。
	ReadHeartbeatRTTMs float64 `json:"read_heartbeat_rtt_ms"`

	// BindingConstraint 写路径哪一项先饱和。
	BindingConstraint string `json:"binding_constraint"`

	// WANBytesPerOp 一次写跨域字节数。
	WANBytesPerOp float64 `json:"wan_bytes_per_op"`
	// ClusterWANBytesPerSec 目标速率下的跨域字节速率。
	ClusterWANBytesPerSec float64 `json:"cluster_wan_bytes_per_sec"`

	// ---- 可用性（由 pkg/faft 精确计算）----

	DataAvailability    float64 `json:"data_availability"`
	ControlAvailability float64 `json:"control_availability"`
	// ShardsFullyAvailable 期望同时可用的分片数（分片间独立时）。
	ShardsFullyAvailable float64 `json:"shards_fully_available"`
	// ClusterAvailability 整个集群（全部分片）都可用所需的概率上界。
	ClusterAvailability float64 `json:"cluster_availability"`

	// ---- 输出时必须随附的说明 ----

	// Caveats 本结果不覆盖的因素。
	Caveats []string `json:"caveats"`
}

// Run 执行一次模拟。
func Run(cfg Config) Result {
	c := cfg.withDefaults()
	n := c.Replicas

	// quorum 几何：多数 quorum（基线）。FAFT 的几何由 pkg/faft 决定，
	// 这里用基线以便与实测对照。
	q2 := n/2 + 1

	res := Result{Config: c}
	res.WriteMsgsPerOp = 2 * q2

	// ---- 延迟 ----
	fsyncMs := c.FsyncLatencyUs / 1000.0
	// 一批 BatchSize 条命令共用一次 fsync，因此单条摊到的 fsync 成本是 fsyncMs/BatchSize。
	res.FsyncPerOp = 1.0 / float64(c.BatchSize)
	batchFsyncMs := fsyncMs
	rtt := c.CrossDomainRTTMs
	if rtt <= 0 {
		rtt = 0.1 // 局域网量级
	}
	res.WriteLatencyMs = rtt + batchFsyncMs
	// readIndex 语义下读不需要额外往返（申请一次后可服务整批）。
	res.ReadLatencyMs = rtt

	// ---- 心跳 ----
	followers := float64(n - 1)
	hbPerSecPerShard := followers * (1000.0 / c.HeartbeatIntervalMs)
	res.HeartbeatMsgsPerSec = hbPerSecPerShard * float64(c.Shards)
	// 每次心跳约 64 字节。
	res.HeartbeatBytesPerSec = res.HeartbeatMsgsPerSec * 64

	// ---- 吞吐上限 ----
	//
	// 写路径与读路径必须分开算，原因见 ClusterReadCeiling 的注释。
	egressBps := c.LeaderNICGbps * 1e9 / 8.0
	res.PerLeaderEgressCeiling = egressBps / (followers * float64(c.LogBytes))
	res.PerLeaderDiskCeiling = float64(c.BatchSize) / (c.FsyncLatencyUs * 1e-6)
	res.PerLeaderWriteCeiling = math.Min(res.PerLeaderEgressCeiling, res.PerLeaderDiskCeiling)

	// 全集群：每个分片一个独立 leader，因此上限是分片数倍。
	res.ClusterWriteCeiling = res.PerLeaderWriteCeiling * float64(c.Shards)
	if res.PerLeaderEgressCeiling <= res.PerLeaderDiskCeiling {
		res.BindingConstraint = "leader_egress_bandwidth"
	} else {
		res.BindingConstraint = "leader_disk_fsync"
	}

	// 读路径上限：readIndex 语义下，一次心跳往返可服务整批读。
	// 因此读吞吐 ≈ 每分片并发数 / 往返延迟。这里用"每分片可并发的读批数"
	// 估计：以 1 / rtt 为每批速率，乘以分片数。
	//
	// 注意这是**上限**，且不含 HTTP 栈、序列化、CPU 等开销 ——
	// 实测值会显著低于它（校准结果里会明确给出差额）。
	res.ReadHeartbeatRTTMs = rtt
	if rtt > 0 {
		perShardReads := 1000.0 / rtt
		res.ClusterReadCeiling = perShardReads * float64(c.Shards)
	}

	// ---- WAN 流量 ----
	res.WANBytesPerOp = followers*float64(c.LogBytes) + float64(c.LogBytes)
	if c.TargetOpsPerSec > 0 {
		res.ClusterWANBytesPerSec = res.WANBytesPerOp * c.TargetOpsPerSec
	}

	// ---- 可用性 ----
	res.DataAvailability, res.ControlAvailability = availability(c, q2, n-q2+1)

	// 分片间独立时，期望可用分片数 = 分片数 × 单分片可用性。
	// 注意：这是**独立假设**下的结果，真实系统中跨分片的相关故障
	// （同一台宿主机承载多个分片的副本）会让实际值更低。
	res.ShardsFullyAvailable = float64(c.Shards) * res.DataAvailability
	res.ClusterAvailability = math.Pow(res.DataAvailability, float64(c.Shards))

	res.Caveats = []string{
		"本结果建模的是成本结构（消息数 / fsync / 字节 / 可用性），不是性能预测",
		"未建模：CPU 调度、页面缓存、TCP 拥塞、锁竞争、GC 停顿",
		"可用性按分片间独立计算；宿主共享导致的相关故障会让实际值更低",
		"吞吐上限来自解析模型，未经实测校准",
	}
	return res
}

// availability 用 pkg/faft 计算两条路径的可用性。
//
// 逐域失效率按"域均摊"处理：把 n 个副本轮流放到各域上。
// 区域事件按 0/1 处理（事件打掉 k 个域），与 pkg/faft 的
// RegionalEventScenarios 保持同一模型。
func availability(c Config, q2, q1 int) (dataAvail, controlAvail float64) {
	topo := faft.NewUniformTopology(c.Domains, c.CrossDomainRTTMs)
	for i := range topo.Domains {
		topo.Domains[i].FailProb = c.ReplicaFailProb
	}
	topo.RegionalEventProb = c.RegionalEventProb
	topo.RegionalEventDomains = c.RegionalEventDomains

	placement := make(faft.Placement, c.Replicas)
	for i := range placement {
		placement[i] = i % c.Domains
	}

	av := faft.AvailabilityModel{
		Model:    faft.IndependentFailure{P: c.ReplicaFailProb},
		Topology: topo,
	}
	// 用 faft 的导出接口：把成员集合交给它算。
	// 这里构造"前 q2 个副本"作为 Q2 成员集合 —— 与多数 quorum 的语义一致。
	q2mem := make([]int, 0, q2)
	for i := 0; i < q2 && i < c.Replicas; i++ {
		q2mem = append(q2mem, i)
	}
	q1mem := make([]int, 0, q1)
	for i := 0; i < q1 && i < c.Replicas; i++ {
		q1mem = append(q1mem, i)
	}
	return faft.MemberAvailability(q2mem, placement, av),
		faft.MemberAvailability(q1mem, placement, av)
}

// ScaleRow 规模扫描的一行。
type ScaleRow struct {
	Shards   int     `json:"shards"`
	Replicas int     `json:"replicas"`
	Nodes    int     `json:"nodes"`
	Domains  int     `json:"domains"`

	ClusterWriteCeiling   float64 `json:"cluster_write_ceiling"`
	ClusterReadCeiling    float64 `json:"cluster_read_ceiling"`
	HeartbeatMsgsPerSec   float64 `json:"heartbeat_msgs_per_sec"`
	HeartbeatBytesPerSec  float64 `json:"heartbeat_bytes_per_sec"`
	ClusterWANBytesPerSec float64 `json:"cluster_wan_bytes_per_sec"`
	BindingConstraint     string  `json:"binding_constraint"`

	DataAvailability    float64 `json:"data_availability"`
	ShardsFullyAvailable float64 `json:"shards_fully_available"`

	// ---- 应用开销因子后的**估计值** ----
	//
	// 这些是"模拟上限 × 实测拟合的开销因子"，用于给出可比绝对量。
	// 仍属外推，必须标注为估计值并随附区间。
	EstimatedWriteThroughput float64 `json:"estimated_write_throughput"`
	EstimatedWriteLow        float64 `json:"estimated_write_low"`
	EstimatedWriteHigh       float64 `json:"estimated_write_high"`
	EstimatedReadThroughput  float64 `json:"estimated_read_throughput"`
}

// Scale 对一组分片数扫描，用于 RQ3（收益是否随规模放大或衰减）。
func Scale(base Config, shardCounts []int) []ScaleRow {
	out := make([]ScaleRow, 0, len(shardCounts))
	for _, s := range shardCounts {
		c := base
		c.Shards = s
		r := Run(c)
		out = append(out, ScaleRow{
			Shards:   s,
			Replicas: r.Config.Replicas,
			Nodes:    s * r.Config.Replicas,
			Domains:  r.Config.Domains,

			ClusterWriteCeiling:   r.ClusterWriteCeiling,
			ClusterReadCeiling:    r.ClusterReadCeiling,
			HeartbeatMsgsPerSec:   r.HeartbeatMsgsPerSec,
			HeartbeatBytesPerSec:  r.HeartbeatBytesPerSec,
			ClusterWANBytesPerSec: r.ClusterWANBytesPerSec,
			BindingConstraint:     r.BindingConstraint,

			DataAvailability:     r.DataAvailability,
			ShardsFullyAvailable: r.ShardsFullyAvailable,
		})
	}
	return out
}

// FormatScale 渲染规模扫描表。
func FormatScale(rows []ScaleRow) string {
	out := ""
	out += fmt.Sprintf("%-8s %-8s %-9s %-15s %-15s %-16s %-15s %-12s\n",
		"分片", "副本", "节点数", "写上限", "写估计", "读上限", "心跳字节/s", "数据可用性")
	out += repeatStr("-", 118) + "\n"
	for _, r := range rows {
		est := "—"
		if r.EstimatedWriteThroughput > 0 {
			est = fmt.Sprintf("%.0f", r.EstimatedWriteThroughput)
		}
		out += fmt.Sprintf("%-8d %-8d %-9d %-15.0f %-15s %-16.0f %-15.0f %-12.9f\n",
			r.Shards, r.Replicas, r.Nodes, r.ClusterWriteCeiling, est,
			r.ClusterReadCeiling, r.HeartbeatBytesPerSec, r.DataAvailability)
	}
	return out
}

// SortedByNodes 按节点数升序排列。
func SortedByNodes(rows []ScaleRow) []ScaleRow {
	out := append([]ScaleRow(nil), rows...)
	sort.Slice(out, func(i, j int) bool { return out[i].Nodes < out[j].Nodes })
	return out
}
