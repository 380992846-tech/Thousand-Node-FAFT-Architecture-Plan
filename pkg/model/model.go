// pkg/model/model.go
//
// 解析成本模型：把"千节点共识"的成本拆成可计算的项，让论文里的每条论断都能被复算。
//
// 为什么需要它：原始 README 直接断言"百万级 QPS、P99<10ms、线性扩展"。这些数字
// 没有任何模型或测量支撑。本包给出三件事：
//
//  1. leader 出口带宽上限。单 leader 必须把每条日志发给 n-1 个 follower，
//     因此吞吐 ∝ C/((n-1)*β)。注意：这一条不是我们的发现——Mencius（OSDI'08）
//     已经明确写出 "throughput is in proportion to 1/(n-1)"；Leopard 与 Carnot Bound
//     给出同形式的界。本包只是把它与磁盘/CPU 上限组合，并在 n=1000 处求值。
//  2. 共识消息量。用 Lamport, "Lower Bounds on Consensus"（MSR 2000 /
//     Distributed Computing 19:104-125, 2006）的三个精确结论，
//     以及 Flexible Paxos（Howard et al., OPODIS 2016）的 |Q1|+|Q2| > N 约束。
//  3. WAN 往返。稳态下一次客户端操作至少需要一个到 quorum 的往返。
//
// 重要声明：本包不含任何"新定理"。第 2、3 项直接标出出处；第 1 项的组合形式
// 是工程模型（组合本身未见已发表文献给出四变量闭式），代码中标注 Derived。
package model

import (
	"fmt"
	"math"
	"sort"
)

// Config 模型输入。
type Config struct {
	// Replicas 单个 Raft group 的副本数 n。
	Replicas int
	// Groups 分片（Raft group）数量。
	Groups int
	// PayloadBytes 单条日志载荷 β（字节）。
	PayloadBytes int
	// LeaderNICGbps leader 网卡速率（Gb/s）。
	LeaderNICGbps float64
	// FsyncLatencyUs 单次 fsync 延迟（微秒）。
	FsyncLatencyUs float64
	// BatchSize 批大小 b。
	BatchSize int
	// CPUCostPerEntryNs leader 处理"每个 follower 每条日志"的 CPU 成本（纳秒）。
	CPUCostPerEntryNs float64
	// CrossZoneRTTMs 跨域单程 RTT（毫秒）；0 表示局域网。
	CrossZoneRTTMs float64
	// HeartbeatIntervalMs 心跳间隔（毫秒）。
	HeartbeatIntervalMs float64
	// Dedup enables the "disseminate ids, not payloads" optimization (S-Paxos style).
	// When true, the leader's per-entry egress drops from β to a small id size.
	DisseminateIDsOnly bool
	// IDBytes 开启 DisseminateIDsOnly 时 per-entry 的 id 大小。
	IDBytes int
}

func (c Config) withDefaults() Config {
	if c.Replicas <= 0 {
		c.Replicas = 5
	}
	if c.Groups <= 0 {
		c.Groups = 1
	}
	if c.PayloadBytes <= 0 {
		c.PayloadBytes = 256
	}
	if c.LeaderNICGbps <= 0 {
		c.LeaderNICGbps = 1
	}
	if c.FsyncLatencyUs <= 0 {
		c.FsyncLatencyUs = 100
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1
	}
	if c.CPUCostPerEntryNs <= 0 {
		c.CPUCostPerEntryNs = 200
	}
	if c.HeartbeatIntervalMs <= 0 {
		c.HeartbeatIntervalMs = 100
	}
	if c.IDBytes <= 0 {
		c.IDBytes = 24
	}
	return c
}

// Ceiling 一个吞吐上限及其来源标注。
type Ceiling struct {
	Name  string  `json:"name"`
	Value float64 `json:"value_ops_per_sec"`
	// Formula 便于人工复算。
	Formula string `json:"formula"`
	// Source 为 "Published" 或 "Derived"。
	Source string `json:"source"`
	Binding bool   `json:"binding"`
}

// MessageBound 一条消息复杂度结果。
type MessageBound struct {
	Label   string `json:"label"`
	Value   int64  `json:"value"`
	Delays  int    `json:"message_delays"`
	Note    string `json:"note"`
	Source  string `json:"source"`
}

// Report 模型输出。
type Report struct {
	Config Config `json:"config"`

	// Majorities 最小多数集合大小 m = floor(n/2)+1。
	MajoritySize int `json:"majority_size"`

	Ceilings []Ceiling `json:"ceilings"`
	// ThroughputBound 为各上限的最小值，即系统的有效吞吐上界。
	ThroughputBound float64 `json:"throughput_bound_ops_per_sec"`
	Binding         string  `json:"binding_constraint"`

	Messages []MessageBound `json:"messages"`

	// SteadyStateLatencyMs 稳态下一次客户端操作的延迟下界。
	SteadyStateLatencyMs float64 `json:"steady_state_latency_ms"`

	// WANBytesPerOp 单次写在各副本间的总字节数（用于带宽成本核算）。
	WANBytesPerOp int64 `json:"wan_bytes_per_op"`

	// HeartbeatBytesPerSec 全集群心跳总带宽。
	HeartbeatBytesPerSec float64 `json:"heartbeat_bytes_per_sec"`

	// Availability 可用性分析。
	Availability Availability `json:"availability"`
}

// Availability 失效容忍分析。
type Availability struct {
	// CrashTolerated 多数派模型下可容忍的副本故障数。
	CrashTolerated int `json:"crash_tolerated"`
	// QuorumNeeded 达成一次决策需要的副本数。
	QuorumNeeded int `json:"quorum_needed"`
	// OversizingFactor 相对"仅需容忍 f 个故障"的冗余倍数。
	OversizingFactor float64 `json:"oversizing_factor"`
	// FlexiblePaxos 若把阶段二 quorum 缩到 f+1，阶段一需要多大。
	FlexibleQuorum2 int `json:"flexible_quorum2"`
	FlexibleQuorum1 int `json:"flexible_quorum1"`
	// ElectionAvailability 上述配置下选主阶段可容忍的故障数。
	ElectionAvailability int `json:"election_availability"`
	// ReplicationAvailability 已有 leader 时可容忍的故障数。
	ReplicationAvailability int `json:"replication_availability"`
}

// Analyze 运行模型。
func Analyze(cfg Config) Report {
	c := cfg.withDefaults()
	n := c.Replicas

	rep := Report{Config: c}
	rep.MajoritySize = n/2 + 1

	// ---- 吞吐上限 ----
	// 每个 follower 的 per-entry 出口成本（字节）。
	followers := float64(n - 1)
	perEntryBytes := float64(c.PayloadBytes)
	if c.DisseminateIDsOnly {
		perEntryBytes = float64(c.IDBytes)
	}

	egressBps := c.LeaderNICGbps * 1e9 / 8.0 // bytes/s

	// (1) leader 出口带宽：C / ((n-1)*β)
	// 出处：Mencius, OSDI 2008 §7："Since Paxos is limited by the leader's total
	// outgoing bandwidth, its throughput is in proportion to 1/(n-1)."
	netCeiling := egressBps / (followers * perEntryBytes)

	formulaNet := fmt.Sprintf("C / ((n-1) * beta) = %.6g / (%.0f * %.0f)", egressBps, followers, perEntryBytes)
	sourceNet := "Published (Mencius OSDI'08; Leopard; Carnot Bound)"
	if c.DisseminateIDsOnly {
		sourceNet += " + S-Paxos-style id-only dissemination"
	}

	// (2) 磁盘：b / t_fsync（批处理可线性摊薄）
	diskCeiling := float64(c.BatchSize) / (c.FsyncLatencyUs * 1e-6)
	formulaDisk := fmt.Sprintf("b / t_fsync = %d / %.6g", c.BatchSize, c.FsyncLatencyUs*1e-6)

	// (3) CPU：leader 为每个 follower 每条日志付出的处理成本。
	cpuCeiling := 1.0 / (followers * c.CPUCostPerEntryNs * 1e-9)
	formulaCPU := fmt.Sprintf("1 / ((n-1) * c_cpu) = 1 / (%.0f * %.6g)", followers, c.CPUCostPerEntryNs*1e-9)

	rep.Ceilings = []Ceiling{
		{Name: "leader_egress_bandwidth", Value: netCeiling, Formula: formulaNet, Source: sourceNet},
		{Name: "leader_disk_fsync", Value: diskCeiling, Formula: formulaDisk, Source: "Derived (batching model)"},
		{Name: "leader_cpu_per_follower", Value: cpuCeiling, Formula: formulaCPU, Source: "Derived (Mencius states the qualitative form)"},
	}

	best := rep.Ceilings[0]
	for _, cl := range rep.Ceilings[1:] {
		if cl.Value < best.Value {
			best = cl
		}
	}
	rep.ThroughputBound = best.Value
	rep.Binding = best.Name
	for i := range rep.Ceilings {
		rep.Ceilings[i].Binding = rep.Ceilings[i].Name == best.Name
	}

	// ---- 共识消息量（Lamport 精确界）----
	m := int64(rep.MajoritySize)
	nn := int64(n)
	rep.Messages = []MessageBound{
		{
			Label: "lamport_2_delay_optimal", Value: nn * (m - 1), Delays: 2,
			Note: "达成 2 个消息延迟所必需的消息数（在 n=1000,m=501 时约 5e5，实际不可用）",
			Source: "Lamport, Lower Bounds on Consensus, MSR 2000 / Distrib. Comput. 19:104-125 (2006), Thm 2",
		},
		{
			Label: "lamport_min_messages", Value: m + nn - 2, Delays: int(m),
			Note: "消息数最少，但需要 m 个串行消息延迟（n=1000 时 501 个延迟，同样不可用）",
			Source: "同上",
		},
		{
			Label: "lamport_classic_synod", Value: 2*m + nn - 3, Delays: 3,
			Note: "经典 synod：3 个消息延迟与 2m+n-3 条消息之间的折中，即 Paxos 的位置",
			Source: "同上 (Thm 3)",
		},
		{
			Label: "multipaxos_steady_state", Value: 2 * m, Delays: 1,
			Note: "稳态下 Phase-1 被摊销，只剩 Phase-2 的 2|Q2| 条消息",
			Source: "Multi-Paxos / Raft 稳态",
		},
	}

	// ---- Flexible Paxos：把 Phase-2 quorum 压到 f+1 ----
	// 约束：|Q1| + |Q2| > N（Howard, Malkhi, Spiegelman, OPODIS 2016, §4.2）
	for f := 1; f <= 5 && f < n; f++ {
		q2 := f + 1
		if q2 >= n {
			continue
		}
		q1 := n - q2 + 1
		rep.Messages = append(rep.Messages, MessageBound{
			Label:  fmt.Sprintf("flexible_paxos_f%d", f),
			Value:  int64(2 * q2),
			Delays: 1,
			Note: fmt.Sprintf("|Q2|=%d（容忍 %d 个故障），需 |Q1|=%d > N-|Q2|；选主需 %d/%d 节点在线",
				q2, f, q1, q1, n),
			Source: "Howard, Malkhi, Spiegelman, Flexible Paxos, OPODIS 2016",
		})
	}

	// ---- 可用性 ----
	f := (n - 1) / 2
	rep.Availability.CrashTolerated = f
	rep.Availability.QuorumNeeded = rep.MajoritySize
	if f > 0 {
		rep.Availability.OversizingFactor = float64(rep.MajoritySize) / float64(f+1)
	}
	// 若把阶段二收窄到 f+1，阶段一必须 >= n-f（即 n-(f+1)+1）。
	fp := 5
	if fp >= n {
		fp = n / 2
	}
	if fp > 0 && fp < n {
		rep.Availability.FlexibleQuorum2 = fp + 1
		rep.Availability.FlexibleQuorum1 = n - (fp + 1) + 1
		rep.Availability.ElectionAvailability = n - rep.Availability.FlexibleQuorum1
		rep.Availability.ReplicationAvailability = n - rep.Availability.FlexibleQuorum2
	}

	// ---- 稳态延迟 ----
	fsyncMs := c.FsyncLatencyUs / 1000.0
	// 局域网：客户端→leader→quorum→客户端 ≈ 1 个 RTT；此处用 fsync + 一个往返近似。
	if c.CrossZoneRTTMs > 0 {
		rep.SteadyStateLatencyMs = c.CrossZoneRTTMs + fsyncMs
	} else {
		// 无跨域参数时，用 fsync + 局域网单程估算（0.1ms 量级）。
		rep.SteadyStateLatencyMs = fsyncMs + 0.1
	}

	// ---- 带宽成本 ----
	rep.WANBytesPerOp = int64(followers)*int64(perEntryBytes) + int64(c.PayloadBytes)
	// 心跳：每分片 leader 每秒发 (n-1) * interval 次心跳，每次约 64 字节。
	hbPerSecPerGroup := followers * (1000.0 / c.HeartbeatIntervalMs) * 64.0
	rep.HeartbeatBytesPerSec = hbPerSecPerGroup * float64(c.Groups)

	return rep
}

// ScaleTable 生成 n 扫描表（用于论文中的 scaling 表）。
func ScaleTable(cfg Config, ns []int) []Report {
	out := make([]Report, 0, len(ns))
	for _, n := range ns {
		c := cfg
		c.Replicas = n
		out = append(out, Analyze(c))
	}
	return out
}

// FormatCeilings 把上限表格式化为定宽文本。
func FormatCeilings(reps []Report) string {
	var sb []string
	sb = append(sb, fmt.Sprintf("%-8s %-14s %14s %14s %14s %14s",
		"n", "binding", "egress", "disk", "cpu", "bound"))
	sb = append(sb, repeat("-", 90))
	for _, r := range reps {
		var egress, disk, cpu float64
		for _, c := range r.Ceilings {
			switch c.Name {
			case "leader_egress_bandwidth":
				egress = c.Value
			case "leader_disk_fsync":
				disk = c.Value
			case "leader_cpu_per_follower":
				cpu = c.Value
			}
		}
		sb = append(sb, fmt.Sprintf("%-8d %-14s %14.0f %14.0f %14.0f %14.0f",
			r.Config.Replicas, r.Binding, egress, disk, cpu, r.ThroughputBound))
	}
	return join(sb, "\n")
}

// SortedCeilings 按上限值升序返回（最紧的在前）。
func SortedCeilings(reps []Report) []Ceiling {
	var out []Ceiling
	for _, r := range reps {
		out = append(out, r.Ceilings...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Value < out[j].Value })
	return out
}

// Ratio 两个数的比值，避免除零。
func Ratio(a, b float64) float64 {
	if b == 0 {
		if a == 0 {
			return 1
		}
		return math.Inf(1)
	}
	return a / b
}

func repeat(s string, n int) string {
	b := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		b = append(b, s...)
	}
	return string(b)
}

func join(parts []string, sep string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += sep
		}
		out += p
	}
	return out
}
