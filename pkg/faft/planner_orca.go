// pkg/faft/planner_orca.go
//
// Orca (PVLDB vol 19, 2026) 的 quorum 构造。
//
// ⚠️ 实现依据与不确信之处（必须在论文中如实披露）
//
// Orca 的定位是"Flexible Quorums Meet Dynamic Quorums"：把 Flexible Paxos 的
// 静态 quorum 几何与"动态 quorum"结合 —— 后者指 quorum 可以**排除已被检测到
// 故障的节点**，从而在部分节点失效时仍能形成足够大的 quorum。
//
// 本实现覆盖其**结构性部分**：
//   (1) Raft 日志复制 + Flexible-Paxos 风格的 quorum 构造；
//   (2) commit quorum 固定在 k+1（k 为容错目标），不随节点故障缩小；
//   (3) 已被检测为故障的节点从 quorum 计算中排除。
//
// **未实现**：动态 quorum 的完整运行时机制（故障检测器的状态机、
// 排除后的安全性论证、排除集合的收敛条件）。
// 这些需要故障检测器与运行时状态，超出本对照框架。
//
// 关于出处：Orca 的 DOI 为 10.14778/3836663.3836709。
// 本实现依据检索到的摘要描述（"Raft log replication + Flexible-Paxos quorum
// 构造；replicas quorum 固定为 k+1；故障节点作为被检测到后从 quorum 排除；
// 比 Raft 与 FlexiRaft 容忍更多故障且开销可忽略"）。
// **我未能获得论文全文**，因此上述结构性描述的具体参数语义
// （尤其 k 的定义、排除集合的维护方式）可能与原文有差异。
// 论文中必须披露这一点，并给出敏感性分析。
package faft

import "fmt"

// OrcaPlanner Orca 的 quorum 构造（结构性部分）。
type OrcaPlanner struct {
	// FaultTolerance k：commit quorum 固定为 k+1。
	//
	// 0 表示用默认值 min(2, n/2)。
	//
	// 为什么默认取小值而不是 n/2：Orca 的卖点正是"动态 quorum 在部分节点
	// 失效时仍能形成足够大的 quorum"，因此它的 commit quorum 本就小于多数。
	// 若默认取 n/2，则 k+1 = n/2+1 恰好等于多数 quorum，本 planner 与
	// MajorityPlanner 完全重合、毫无区分度（实测确实如此）。
	// 小 k + 动态排除故障节点，才是 Orca 与 Raft/FlexiRaft 的真正差异所在。
	FaultTolerance int
	// FailedReplicas 已被检测为故障、应从 quorum 计算中排除的副本下标。
	//
	// 这是"动态 quorum"的输入。在真实系统中它来自故障检测器；
	// 本对照框架中由调用方显式给出，因为框架内没有运行时故障检测器。
	FailedReplicas []int
	// UseAvailabilityAware 是否按可用性选成员。默认 false ——
	// Orca 论文里没有成员选择这一步（与 FlexiRaft 同理），
	// 副本分散由部署者保证。设为 true 就变成混合体而非原始 baseline。
	UseAvailabilityAware bool
}

// effectiveK 返回实际使用的容错目标 k。
func (p OrcaPlanner) effectiveK(n int) int {
	if p.FaultTolerance > 0 {
		return p.FaultTolerance
	}
	k := 2
	if k > n/2 {
		k = n / 2
	}
	if k < 1 {
		k = 1
	}
	return k
}

// Name 实现 QuorumPlanner。
func (p OrcaPlanner) Name() string {
	if p.UseAvailabilityAware {
		return "orca+availaware"
	}
	if len(p.FailedReplicas) > 0 {
		return "orca+dynamic"
	}
	return "orca"
}

// Source 实现 QuorumPlanner。
func (p OrcaPlanner) Source() string {
	return "Orca: Flexible Quorums Meet Dynamic Quorums, PVLDB vol 19, 2026, " +
		"DOI 10.14778/3836663.3836709；quorum 几何约束见 Flexible Paxos (OPODIS 2016 §4.2)"
}

// Notes 实现 QuorumPlanner。
func (p OrcaPlanner) Notes() string {
	return "实现结构性部分：commit quorum 固定 k+1、故障节点从 quorum 排除。" +
		"未实现：动态 quorum 的完整运行时机制（故障检测器状态、排除集合收敛）。" +
		"未获得论文全文，参数语义可能与原文有差异，论文中须披露并做敏感性分析。"
}

// Plan 实现 QuorumPlanner。
func (p OrcaPlanner) Plan(topo *Topology, cfg Config, av AvailabilityModel) FailurePlan {
	cfg = cfg.withDefaults(topo)
	n := cfg.Replicas

	// commit quorum：固定为 k+1。
	k := p.effectiveK(n)
	q2 := k + 1
	if q2 > n {
		q2 = n
	}

	// election quorum：按 Flexible Paxos 约束 |Q1| + |Q2| > n 推导。
	q1 := n - q2 + 1
	if q1 > n {
		q1 = n
	}
	if q1 < 1 {
		q1 = 1
	}

	// 构造候选集合：排除已被检测为故障的副本。
	failed := map[int]bool{}
	for _, f := range p.FailedReplicas {
		failed[f] = true
	}
	candidates := make([]int, 0, n)
	for i := 0; i < n; i++ {
		if !failed[i] {
			candidates = append(candidates, i)
		}
	}

	pick := func(size int) []int {
		if p.UseAvailabilityAware {
			return pickMaxAvailability(candidates, cfg.Placement, av, size)
		}
		out := make([]int, 0, size)
		for i := 0; i < size && i < len(candidates); i++ {
			out = append(out, candidates[i])
		}
		return out
	}

	fp := planWithPicker(topo, cfg, av, p, func(int) (int, int) { return q1, q2 }, pick)
	if len(p.FailedReplicas) > 0 {
		fp.Diagnostics = fmt.Sprintf(
			"%s：|Q2|=k+1=%d |Q1|=%d；已排除 %d 个故障副本 %v",
			p.Name(), q2, q1, len(p.FailedReplicas), p.FailedReplicas)
	}
	return fp
}

// ---------------------------------------------------------------------------
// TiKV PD 启发式 placement
// ---------------------------------------------------------------------------

// TiKVPDPlanner TiKV Placement Driver 的调度启发式。
//
// ⚠️ 重要说明：PD 是**placement 调度器**，不是 quorum 规划器。
// 它不决定 quorum 大小（那由 Raft 固定为多数），只决定：
//   - 把 region 的副本放到哪些 store（balance-region）
//   - 把 leadership 转移到哪个 store（balance-leader）
//   - 热点 region 如何分散（balance-hot-region）
//
// 在本对照框架中，"quorum 几何"维度上 PD 等价于多数 quorum。
// 因此本 planner 在 quorum 大小上与 MajorityPlanner 相同；
// 它存在的作用是**明确记录 PD 在 quorum 几何维度上没有自由度**，
// 从而使"FAFT 相对 PD 的增量"这个说法有据可依。
//
// 依据（TiKV 官方文档与源码，非论文）：
//   - PD 只有三个原语：AddReplica / RemoveReplica / TransferLeader
//   - balance-leader-scheduler 按"该 store 上 region 大小之和"打分
//   - leader-schedule-policy 默认 count
//   - PD 通过 region heartbeat 下发 operator 建议，region leader 可以拒绝
//
// PD 的放置优化是**跨分片**的（把不同 region 的副本摊到不同 store），
// 而本对照框架只做**单个分片内**的 quorum 决策。
// 这个维度差异必须披露 —— 它意味着 PD 的收益不在本框架的度量范围内。
type TiKVPDPlanner struct{}

// Name 实现 QuorumPlanner。
func (p TiKVPDPlanner) Name() string { return "tikv-pd" }

// Source 实现 QuorumPlanner。
func (p TiKVPDPlanner) Source() string {
	return "TiKV Placement Driver 官方文档与源码（balance-region / balance-leader / " +
		"balance-hot-region scheduler）。**非论文** —— PD 的调度策略没有学术发表，" +
		"这也是本项目识别出的文献空白之一"
}

// Notes 实现 QuorumPlanner。
func (p TiKVPDPlanner) Notes() string {
	return "PD 是 placement 调度器而非 quorum 规划器：quorum 几何上无自由度（同多数 quorum）。" +
		"其优化是跨分片的（把不同 region 的副本摊到不同 store），" +
		"而本框架只做单分片内的 quorum 决策 —— 该维度差异须在论文中披露"
}

// Plan 实现 QuorumPlanner。
func (p TiKVPDPlanner) Plan(topo *Topology, cfg Config, av AvailabilityModel) FailurePlan {
	fp := MajorityPlanner{}.Plan(topo, cfg, av)
	fp.Diagnostics = "TiKV PD：quorum 几何无自由度（同多数 quorum）；" +
		"其调度优化在跨分片维度，本框架未度量"
	return fp
}

// ---------------------------------------------------------------------------
// 全部 baseline 的默认集合
// ---------------------------------------------------------------------------

// DefaultBaselines 返回论文对照应包含的 baseline 集合。
//
// 每个 planner 的 Source 与 Notes 都必须能被核对；
// 无法溯源的一律不纳入（见 docs/DESIGN.md §附录 B 的数据可信度分级）。
func DefaultBaselines(targets AvailabilityTargets) []QuorumPlanner {
	return []QuorumPlanner{
		MajorityPlanner{},
		TiKVPDPlanner{},
		FlexiRaftPlanner{},
		FlexiRaftPlanner{DataQuorum: 2},
		OrcaPlanner{},
		FaftPlanner{Targets: targets},
		UniformFaftPlanner{Targets: targets},
	}
}
