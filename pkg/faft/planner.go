// pkg/faft/planner.go
//
// Quorum 规划器：把不同系统的 quorum 决策逻辑统一成一个接口，
// 以便在同一故障模型下做 apples-to-apples 的对照。
//
// 为什么需要这一层（docs/DESIGN.md §4.2）：
//
// 论文的核心主张是"联合优化 quorum 几何与副本落点优于现有做法"。
// 要评估这个主张，必须把各个 baseline 的 quorum 决策放在**同一套
// 可用性模型与同一套代价模型**下比较。若各自用自己论文里的数字，
// 就是拿不同口径的数据对比 —— 这正是 EPaxos Revisited (NSDI'21) 批评的
// 那类问题：原评测方法有缺陷，重测后结论反转。
//
// 本文件实现四个规划器：
//
//	MajorityPlanner   —— Raft / Multi-Paxos：Q1 = Q2 = ⌊n/2⌋+1
//	FlexiRaftPlanner  —— FlexiRaft (CIDR'23)：固定 Q2，自动推导 Q1
//	FaftPlanner       —— 本项目：在可用性约束下最小化 2|Q2|
//	UniformFaftPlanner—— FAFT 的几何 + 按域计数轮转（消融：隔离"成员选择"的贡献）
//
// 关于 FlexiRaft 的实现依据（必须写明，否则是伪引用）：
//
//	FlexiRaft (Yadav & Rahut, CIDR 2023) 的核心是：
//	  (1) 引入可配置的 **data-commit quorum**（比多数更小），
//	  (2) 由它**自动推导** leader-election quorum，
//	  (3) 给出 static 与 dynamic 两种模式。
//
//	本实现覆盖其 static 模式：给定 dataQuorum（即 |Q2|），
//	取 |Q1| = n - |Q2| + 1，满足 Flexible Paxos 的 |Q1|+|Q2| > N
//	（Howard et al., OPODIS 2016 §4.2）。
//	dynamic 模式（按检测到的故障动态排除故障节点）**未实现**，
//	因为它需要故障检测器的运行时状态，属于本对照框架之外的部分。
//	论文中必须披露这一范围差异。
package faft

import (
	"fmt"
	"math"
)

// QuorumPlanner 一个系统的 quorum 决策逻辑。
type QuorumPlanner interface {
	// Name 系统名（用于结果表）。
	Name() string
	// Plan 给定拓扑、配置与故障模型，返回该系统的 quorum 方案。
	Plan(topo *Topology, cfg Config, av AvailabilityModel) FailurePlan
	// Source 决策依据的出处；论文里必须可核对。
	Source() string
	// Notes 实现范围与该系统的差异（未实现的部分必须写明）。
	Notes() string
}

// ---------------------------------------------------------------------------
// Baseline 1：多数 quorum（Raft / Multi-Paxos）
// ---------------------------------------------------------------------------

// MajorityPlanner Raft / Multi-Paxos 的做法：两个阶段都用多数 quorum。
//
// 成员选择：把副本按放置摊开后取前 q 个。
// 注意 Raft 本身**没有**"按可用性选成员"的概念 —— 它只要求
// 副本分散到不同故障域（由部署者保证），不参与协议决策。
// 因此这里用最朴素的"按序取前 q 个"，不引入任何优化。
type MajorityPlanner struct{}

// Name 实现 QuorumPlanner。
func (MajorityPlanner) Name() string { return "majority" }

// Source 实现 QuorumPlanner。
func (MajorityPlanner) Source() string {
	return "Raft (Ongaro & Ousterhout, USENIX ATC 2014)；Multi-Paxos"
}

// Notes 实现 QuorumPlanner。
func (MajorityPlanner) Notes() string {
	return "无 quorum 几何自由度：Q1 = Q2 = ⌊n/2⌋+1"
}

// Plan 实现 QuorumPlanner。
func (p MajorityPlanner) Plan(topo *Topology, cfg Config, av AvailabilityModel) FailurePlan {
	return planWithSizes(topo, cfg, av, 0, p, func(n int) (int, int) {
		m := n/2 + 1
		return m, m
	})
}

// ---------------------------------------------------------------------------
// Baseline 2：FlexiRaft（CIDR'23，static 模式）
// ---------------------------------------------------------------------------

// FlexiRaftPlanner FlexiRaft 的 static 模式：固定 data-commit quorum，
// 由它推导 leader-election quorum。
//
// DataQuorum 为 0 时取一个默认值（n/4，不小于 2）—— 论文中应改为
// 扫描该参数并报告整条曲线，因为 FlexiRaft 的收益取决于这个取值。
type FlexiRaftPlanner struct {
	// DataQuorum 配置的 data-commit quorum 大小（即 |Q2|）。
	// 0 表示用默认值 max(2, n/4)。
	DataQuorum int
	// UseAvailabilityAware 是否用"按可用性选成员"来构造 quorum。
	//
	// 默认 false —— 因为 FlexiRaft 论文里**没有**成员选择这一步：
	// 它假设副本已由部署者按故障域分散好，协议只决定 quorum 大小。
	// 设为 true 会变成"FlexiRaft 几何 + FAFT 成员选择"的混合体，
	// 那是消融实验而不是原始 baseline。默认关闭以保证 baseline 的忠实性。
	UseAvailabilityAware bool
}

// Name 实现 QuorumPlanner。
//
// 钉死 DataQuorum 的实例必须带后缀：否则结果表里会出现两个都叫
// "flexiraft" 的列（默认 max(2,n/4) 与钉死 2 各一个），
// 按名字做的统计会把两者的计数加在一起 —— 实测汇总里
// "未达可达目标 72" 就是这么来的（36 个点被数了两遍）。
func (p FlexiRaftPlanner) Name() string {
	if p.UseAvailabilityAware {
		return "flexiraft+availaware"
	}
	if p.DataQuorum > 0 {
		return fmt.Sprintf("flexiraft-dq%d", p.DataQuorum)
	}
	return "flexiraft"
}

// Source 实现 QuorumPlanner。
func (p FlexiRaftPlanner) Source() string {
	return "FlexiRaft: Flexible Quorums with Raft (Yadav & Rahut, CIDR 2023)；" +
		"quorum 相交条件见 Flexible Paxos (Howard et al., OPODIS 2016 §4.2)"
}

// Notes 实现 QuorumPlanner。
func (p FlexiRaftPlanner) Notes() string {
	return "实现 static 模式。dynamic 模式（按运行时故障动态排除节点）未实现 —— " +
		"它需要故障检测器状态，超出本对照框架。原论文亦未开源代码。"
}

// Plan 实现 QuorumPlanner。
func (p FlexiRaftPlanner) Plan(topo *Topology, cfg Config, av AvailabilityModel) FailurePlan {
	cfg = cfg.withDefaults(topo)
	n := cfg.Replicas

	pick := func(size int) []int {
		all := make([]int, n)
		for i := range all {
			all[i] = i
		}
		if p.UseAvailabilityAware {
			return pickMaxAvailability(all, cfg.Placement, av, size)
		}
		out := make([]int, 0, size)
		for i := 0; i < size && i < n; i++ {
			out = append(out, i)
		}
		return out
	}

	return planWithPicker(topo, cfg, av, p, func(n int) (int, int) {
		q2 := p.DataQuorum
		if q2 <= 0 {
			q2 = n / 4
			if q2 < 2 {
				q2 = 2
			}
		}
		if q2 > n {
			q2 = n
		}
		q1 := n - q2 + 1 // |Q1| + |Q2| > n 的最小解
		if q1 > n {
			q1 = n
		}
		return q1, q2
	}, pick)
}

// ---------------------------------------------------------------------------
// 本项目：FAFT
// ---------------------------------------------------------------------------

// FaftPlanner FAFT：在可用性约束下最小化稳态写消息数 2|Q2|，
// 且成员按边际可用性增益选择。
type FaftPlanner struct {
	// Targets 两条路径的可用性目标。零值用 DefaultTargets()。
	Targets AvailabilityTargets
}

// Name 实现 QuorumPlanner。
func (p FaftPlanner) Name() string { return "faft" }

// Source 实现 QuorumPlanner。
func (p FaftPlanner) Source() string {
	return "本项目。约束来自 Flexible Paxos (OPODIS 2016 §4.2)，" +
		"可用性模型见 pkg/faft/failure.go"
}

// Notes 实现 QuorumPlanner。
func (p FaftPlanner) Notes() string {
	return "目标是最小化稳态写消息数；成员按边际可用性增益贪心选择"
}

// Plan 实现 QuorumPlanner。
func (p FaftPlanner) Plan(topo *Topology, cfg Config, av AvailabilityModel) FailurePlan {
	t := p.Targets
	if t.Data <= 0 && t.Control <= 0 {
		t = DefaultTargets()
	}
	fp := SolveWithFailures(topo, cfg, av, t)
	fp.PlacementAware = true
	return fp
}

// ---------------------------------------------------------------------------
// 消融：FAFT 的几何 + 按域计数成员选择
// ---------------------------------------------------------------------------

// UniformFaftPlanner 用 FAFT 的 quorum 大小（与 FaftPlanner 相同），
// 但成员按"域计数轮转"选择而非按可用性。
//
// 用途：**消融实验**。它隔离出"成员选择准则"单独的贡献 ——
// 若该 planner 与 FaftPlanner 结果相同，说明收益全来自 quorum 大小；
// 若不同，差额就是成员选择的价值。
type UniformFaftPlanner struct {
	Targets AvailabilityTargets
}

// Name 实现 QuorumPlanner。
func (p UniformFaftPlanner) Name() string { return "faft-countselect" }

// Source 实现 QuorumPlanner。
func (p UniformFaftPlanner) Source() string { return "本项目的消融配置" }

// Notes 实现 QuorumPlanner。
func (p UniformFaftPlanner) Notes() string {
	return "与 faft 相同的 quorum 大小，但成员按域计数轮转 —— 隔离成员选择准则的贡献"
}

// Plan 实现 QuorumPlanner。
func (p UniformFaftPlanner) Plan(topo *Topology, cfg Config, av AvailabilityModel) FailurePlan {
	// 先用 FAFT 求出 quorum 大小，再换成按域计数的成员集合。
	base := FaftPlanner{Targets: p.Targets}.Plan(topo, cfg, av)
	q1, q2 := base.Quorum.Size()
	cfg = cfg.withDefaults(topo)

	pick := func(size int) []int {
		return pickMaxDomains(topo, cfg.Placement, size, cfg.Replicas)
	}
	fp := planWithPicker(topo, cfg, av, p, func(int) (int, int) { return q1, q2 }, pick)
	fp.PlacementAware = false
	fp.Diagnostics = "消融：quorum 大小取自 faft，成员改按域计数轮转；" +
		"与 faft 的差异即成员选择准则的贡献"
	return fp
}

// ---------------------------------------------------------------------------
// 通用执行
// ---------------------------------------------------------------------------

// planWithSizes 用给定的"n -> (|Q1|,|Q2|)"规则构造方案，成员按序取前 q 个。
func planWithSizes(topo *Topology, cfg Config, av AvailabilityModel,
	_ int, p QuorumPlanner, sizes func(n int) (q1, q2 int)) FailurePlan {
	cfg = cfg.withDefaults(topo)
	pick := func(size int) []int {
		out := make([]int, 0, size)
		for i := 0; i < size && i < cfg.Replicas; i++ {
			out = append(out, i)
		}
		return out
	}
	return planWithPicker(topo, cfg, av, p, sizes, pick)
}

// planWithPicker 构造方案并完备用用性、代价与出处信息。
func planWithPicker(topo *Topology, cfg Config, av AvailabilityModel,
	p QuorumPlanner, sizes func(n int) (q1, q2 int), pick func(size int) []int,
) FailurePlan {
	cfg = cfg.withDefaults(topo)
	n := cfg.Replicas

	q1size, q2size := sizes(n)
	if q1size < 1 {
		q1size = 1
	}
	if q2size < 1 {
		q2size = 1
	}
	if q1size > n {
		q1size = n
	}
	if q2size > n {
		q2size = n
	}

	q := Quorum{Q1Members: pick(q1size), Q2Members: pick(q2size)}
	plan := Evaluate(topo, cfg, q)

	fp := FailurePlan{
		Plan:                plan,
		DataAvailability:    av.availOf(q.Q2Members, cfg.Placement),
		ControlAvailability: av.availOf(q.Q1Members, cfg.Placement),
		FailureModel:        modelName(av.Model),
	}
	fp.MeetsData = fp.DataAvailability >= DefaultTargets().Data
	fp.MeetsControl = fp.ControlAvailability >= DefaultTargets().Control
	fp.Meets = fp.MeetsData && fp.MeetsControl
	fp.Diagnostics = fmt.Sprintf("%s：|Q2|=%d |Q1|=%d 写消息=%d",
		p.Name(), q2size, q1size, plan.WriteMsgsPerOp)
	return fp
}

func modelName(m FailureModel) string {
	if m == nil {
		return "none"
	}
	return m.Name()
}

// ---------------------------------------------------------------------------
// 对照
// ---------------------------------------------------------------------------

// PlannerComparison 一个 planner 在给定场景下的结果。
type PlannerComparison struct {
	Planner string `json:"planner"`
	Source  string `json:"source"`
	Notes   string `json:"notes"`

	Q2Size int `json:"q2_size"`
	Q1Size int `json:"q1_size"`

	WriteMsgsPerOp int     `json:"write_msgs_per_op"`
	WriteLatencyMs float64 `json:"write_latency_ms"`

	DataAvailability    float64 `json:"data_availability"`
	ControlAvailability float64 `json:"control_availability"`

	MeetsData    bool `json:"meets_data"`
	MeetsControl bool `json:"meets_control"`
	// Feasible 两条路径都达标。**这是评估的第一道门槛**：
	// 消息数再低，若可用性不达标就没有可比性。
	Feasible bool `json:"feasible"`

	// MsgReductionVsMajority 写消息数相对多数 quorum 的降幅倍数。
	MsgReductionVsMajority float64 `json:"msg_reduction_vs_majority"`
}

// ComparePlanners 在同一拓扑、配置与故障模型下对照多个 planner。
//
// 评估顺序（论文中应明确这个判据）：
//  1. **先看可行性**：可用性不达标的方案无论消息数多低都不参与比较；
//  2. 在可行方案中比写消息数；
//  3. 消息数相同时比延迟。
func ComparePlanners(topo *Topology, cfg Config, av AvailabilityModel, planners ...QuorumPlanner) []PlannerComparison {
	cfg = cfg.withDefaults(topo)

	out := make([]PlannerComparison, 0, len(planners))
	var majorityMsgs int

	for _, p := range planners {
		fp := p.Plan(topo, cfg, av)
		q1, q2 := fp.Quorum.Size()

		cmp := PlannerComparison{
			Planner:             p.Name(),
			Source:              p.Source(),
			Notes:               p.Notes(),
			Q2Size:              q2,
			Q1Size:              q1,
			WriteMsgsPerOp:      fp.WriteMsgsPerOp,
			WriteLatencyMs:      fp.WriteLatencyMs,
			DataAvailability:    fp.DataAvailability,
			ControlAvailability: fp.ControlAvailability,
			MeetsData:           fp.MeetsData,
			MeetsControl:        fp.MeetsControl,
			Feasible:            fp.Meets,
		}
		if p.Name() == "majority" {
			majorityMsgs = fp.WriteMsgsPerOp
		}
		out = append(out, cmp)
	}

	// 计算相对降幅（多数 quorum 作为基准）。
	if majorityMsgs > 0 {
		for i := range out {
			if out[i].WriteMsgsPerOp > 0 {
				out[i].MsgReductionVsMajority = float64(majorityMsgs) / float64(out[i].WriteMsgsPerOp)
			}
		}
	}
	return out
}

// BestFeasible 在可行方案中取写消息数最小者。
//
// 若没有任何可行方案，返回 ok=false —— 调用方必须显式处理这种情况，
// 不能默默回退到"消息数最小的不可行方案"。
func BestFeasible(cmps []PlannerComparison) (PlannerComparison, bool) {
	var best PlannerComparison
	bestMsgs := math.MaxInt
	found := false
	for _, c := range cmps {
		if !c.Feasible {
			continue
		}
		if c.WriteMsgsPerOp < bestMsgs {
			bestMsgs = c.WriteMsgsPerOp
			best = c
			found = true
		}
	}
	return best, found
}

// FormatPlannerComparison 渲染对照表。
func FormatPlannerComparison(cmps []PlannerComparison) string {
	out := ""
	out += fmt.Sprintf("%-22s %-6s %-6s %-10s %-10s %-16s %-16s %-8s\n",
		"planner", "|Q2|", "|Q1|", "写消息", "相对降幅", "数据可用性", "控制可用性", "可行")
	out += "------------------------------------------------------------------------------------------------------------\n"
	for _, c := range cmps {
		feas := "否"
		if c.Feasible {
			feas = "是"
		}
		red := "—"
		if c.MsgReductionVsMajority > 0 {
			red = fmt.Sprintf("%.2fx", c.MsgReductionVsMajority)
		}
		out += fmt.Sprintf("%-22s %-6d %-6d %-10d %-10s %-16.6g %-16.6g %-8s\n",
			c.Planner, c.Q2Size, c.Q1Size, c.WriteMsgsPerOp, red,
			c.DataAvailability, c.ControlAvailability, feas)
	}
	return out
}
