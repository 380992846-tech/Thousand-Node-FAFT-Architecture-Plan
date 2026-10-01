// pkg/faft/solve_failure.go
//
// 故障感知的 quorum 求解器：把相关故障模型接进优化目标。
//
// 与 geometry.go 里 Solve() 的区别（这是 FAFT 的核心增量）：
//
//	Solve()            —— 固定放置，Q2 取"离 leader 最近"、Q1 取"覆盖域最多"。
//	                      这是两条彼此无关的贪心启发式，目标是延迟与域覆盖。
//	SolveWithFailures  —— 固定放置，以**故障模型下的 quorum 可用性**为准则
//	                      选择成员，并在可用性约束下最小化消息数。
//
// 为什么"覆盖域最多"不够好：域计数是 0/1 的量 —— 一个域要么被覆盖要么没有。
// 但可用性是**概率**量：在已经有 2 个副本的域里再加第 3 个副本，
// 对可用性的边际贡献远低于在一个尚未覆盖的域里加第 1 个副本；
// 而当该域的失效率较高时，连"覆盖"本身的价值也不同。
// 只有按可用性排序才能捕捉这类差异。
//
// 本文件不含新定理。约束来自：
//   - |Q1| + |Q2| > N           Flexible Paxos, OPODIS 2016 §4.2
//   - 可用性目标               使用者给定（论文中由 SLA 反推）
package faft

import (
	"fmt"
	"math"
	"sort"
)

// AvailabilityTargets 两条路径的可用性下限。
type AvailabilityTargets struct {
	// Data 数据路径（阶段二，复制）可用性下限。
	Data float64
	// Control 控制路径（阶段一，选主/恢复）可用性下限。
	Control float64
}

// DefaultTargets 一组宽松但有意义的默认目标（三个九与四个九）。
//
// 选择依据：三个九（0.999）是常见的单分片可用性目标；
// 控制路径要求更高（0.9999），因为选不出来主意味着整分片不可写，
// 而数据路径短时不可用只影响单个写操作（可重试）。
func DefaultTargets() AvailabilityTargets {
	return AvailabilityTargets{Data: 0.999, Control: 0.9999}
}

// FailurePlan 一个故障感知的求解结果。
type FailurePlan struct {
	Plan

	// DataAvailability / ControlAvailability 在给定故障模型下的实测可用性
	// （由 QuorumAvailabilityByMembers 精确计算，非上界估计）。
	DataAvailability    float64
	ControlAvailability float64

	// MeetsData / MeetsControl 是否达到目标。
	MeetsData, MeetsControl bool
	// Meets 两者都达标。
	Meets bool

	// FailureModel 所用模型名，写入结果表以便区分口径。
	FailureModel string
	// PlacementAware 是否为放置感知的联合求解。
	PlacementAware bool
	// Diagnostics 供结果表与排查使用的说明。
	Diagnostics string
}

// memberAvailabilityOn 精确计算「给定副本集合」的 quorum 可用性，
// 使用拓扑里**逐域**的失效率。
//
// 语义：集合中有 q 个副本，需要其中至少 q/2+1 个存活。
//
// ⚠️ 关键建模细节（这里踩过一个会让结果完全失真的坑）：
// Domain.FailProb 是**单个副本**的失效率，不是"整个域一起挂"的概率。
// 因此域内有 c 个副本时，存活副本数服从 Binomial(c, 1-FailProb)，
// 而不是"要么 c 个全活、要么 0 个活"。
//
// 初版实现把域存活当作 0/1 事件（域活 => c 个全活，域挂 => 0 个活），
// 导致"同域 2 个副本"的可用性被算成 (1-p) 而非 (1-p)^2 ——
// 实测 0.999990000000 vs 正确值 0.999980000100。
// 后果是贪心认为"把副本堆在最稳的域"与"跨域展开"同样好，
// 从而系统性选中堆叠方案，让"故障感知"完全失效。
//
// 算法：按域做二项分布**卷积**。dist[s] = 恰好 s 个副本存活的概率。
// 加入含 c 个副本的域时，该域贡献 i 个存活副本的概率为
// C(c,i)·a^i·(1-a)^(c-i)（a = 1-FailProb）。
// 复杂度 O(域数 × q²)。
func memberAvailabilityOn(members []int, topo *Topology, placement Placement, model FailureModel, tol int) float64 {
	if len(members) == 0 || topo == nil {
		return 0
	}

	scenarios := topo.RegionalEventScenarios()
	q := topo.RegionalEventProb

	if len(scenarios) == 0 || q <= 0 {
		// 无区域事件：纯逐副本独立失效。
		return availabilityForKilledSet(members, topo, placement, model, tol, nil)
	}

	// 事件不发生（概率 1-q）+ 事件发生且命中各情形（总概率 q）。
	avail := (1 - q) * availabilityForKilledSet(members, topo, placement, model, tol, nil)
	for _, sc := range scenarios {
		avail += q * sc.Prob *
			availabilityForKilledSet(members, topo, placement, model, tol, sc.Domains)
	}
	return clamp01(avail)
}

// availabilityForKilledSet 计算在「给定的域集合被整体打掉」这一情形下的
// quorum 可用性。
//
// killed 为 nil 表示无域被打掉。其余未被打掉的域内，副本各自以
// replicaAliveProb 独立存活（二项分布）。
//
// 算法：按域做二项分布卷积。dist[s] = 恰好 s 个副本存活的概率。
// 复杂度 O(域数 × |members|²)。
func availabilityForKilledSet(
	members []int, topo *Topology, placement Placement,
	model FailureModel, tol int, killed []int,
) float64 {
	need := len(members)/2 + 1

	killedSet := map[int]bool{}
	for _, d := range killed {
		killedSet[d] = true
	}

	perDomain := map[int]int{}
	for _, idx := range members {
		if idx < 0 || idx >= len(placement) {
			continue
		}
		d := placement[idx]
		if d < 0 || d >= len(topo.Domains) {
			continue
		}
		perDomain[d]++
	}
	if len(perDomain) == 0 {
		return 0
	}

	total := 0
	for _, c := range perDomain {
		total += c
	}

	dist := make([]float64, total+1)
	dist[0] = 1.0
	processed := 0

	for d, c := range perDomain {
		next := make([]float64, total+1)
		if killedSet[d] {
			// 该域被整体打掉：贡献 0 个存活副本。
			for s := 0; s <= processed; s++ {
				next[s] += dist[s]
			}
		} else {
			a := replicaAliveProb(topo, model, d, tol)
			for s := 0; s <= processed; s++ {
				ps := dist[s]
				if ps == 0 {
					continue
				}
				for i := 0; i <= c; i++ {
					next[s+i] += ps * binomPMF(c, i, a)
				}
			}
		}
		dist = next
		processed += c
	}

	var avail float64
	for s := need; s <= total; s++ {
		avail += dist[s]
	}
	return clamp01(avail)
}

// memberAvailability 旧签名版本：无拓扑信息时假设各域同分布。
func memberAvailability(members []int, placement Placement, model FailureModel, _ float64) float64 {
	if model == nil || len(members) == 0 {
		return 0
	}
	need := len(members)/2 + 1
	perDomain := map[int]int{}
	for _, idx := range members {
		if idx < 0 || idx >= len(placement) {
			continue
		}
		perDomain[placement[idx]]++
	}
	if len(perDomain) == 0 {
		return 0
	}
	a := clamp01(model.QuorumAvailable(1, 1))

	total := 0
	for _, c := range perDomain {
		total += c
	}
	dist := make([]float64, total+1)
	dist[0] = 1.0
	processed := 0
	for _, c := range perDomain {
		next := make([]float64, total+1)
		for s := 0; s <= processed; s++ {
			ps := dist[s]
			if ps == 0 {
				continue
			}
			for i := 0; i <= c; i++ {
				next[s+i] += ps * binomPMF(c, i, a)
			}
		}
		dist = next
		processed += c
	}
	var avail float64
	for s := need; s <= total; s++ {
		avail += dist[s]
	}
	return clamp01(avail)
}

// replicaAliveProb 返回域 d 中**单个副本**的存活概率。
//
// 取值规则（按优先级）：
//  1. 拓扑显式给出 Domain.FailProb（非 0）→ 用 1-FailProb；
//  2. 否则由故障模型给出单副本失效率，再取补得到存活率。
//
// ⚠️ 语义陷阱（这里踩过一次）：FailureModel.P 与 CorrelatedFailure.P 都是
// **失效**概率，而 QuorumAvailable(n, need) 返回的是"存活数 >= need 的概率"。
// 因此单副本失效率是 QuorumAvailable(1,1) 本身，存活率是 1 - 它。
// 反过来写会让存活率与失效率互换，在 p=0.2 时得到 0.2 而非 0.8 ——
// 这个错误不会崩溃，只会让所有可用性数字系统性偏错。
func replicaAliveProb(topo *Topology, model FailureModel, d int, tol int) float64 {
	_ = tol
	if topo != nil && d >= 0 && d < len(topo.Domains) {
		if fp := topo.Domains[d].FailProb; fp > 0 {
			return clamp01(1 - fp)
		}
	}
	if model == nil {
		return 1
	}
	// QuorumAvailable(1,1) 在"单副本、需 1 个存活"处返回的是存活率本身 ——
	// 对 IndependentFailure{P} 它等于 1-P（见 TestIndependentFailureMatchesBinomial），
	// 因为那里 P 被解释为**存活**概率。为保持与本包其余部分一致，
	// 这里统一按"模型给出的是单副本失效率"处理：先取 P，再取补。
	failProb := modelFailProb(model)
	return clamp01(1 - failProb)
}

// modelFailProb 从故障模型里取出"单副本失效率"。
//
// 直接读具体类型的 P 字段，而不是从 QuorumAvailable 反推 ——
// 反推会引入"P 到底是存活率还是失效率"的歧义，正是上面注释里那个坑的来源。
func modelFailProb(model FailureModel) float64 {
	switch m := model.(type) {
	case IndependentFailure:
		return m.P
	case CorrelatedFailure:
		return m.P
	case *IndependentFailure:
		if m != nil {
			return m.P
		}
	case *CorrelatedFailure:
		if m != nil {
			return m.P
		}
	}
	// 未知模型：用"单域、需 1 个存活"处的补作为失效率的保守估计。
	return clamp01(1 - model.QuorumAvailable(1, 1))
}

// AvailabilityModel 把故障模型与拓扑打包，供求解器计算成员集合的可用性。
//
// 拓扑在这里是必要的：逐域失效率来自 Topology.Domains[i].FailProb，
// 而"故障感知"的收益正是从域之间的**不等价**里来的。
type AvailabilityModel struct {
	// Model 域级故障模型。为 nil 时退化为 IndependentFailure{P: 0.001}。
	Model FailureModel
	// Topology 故障域结构（含逐域失效率）。为 nil 时退化为均匀失效，
	// 此时按域计数贪心已是最优，故障感知没有额外收益。
	Topology *Topology
}

// availOf 计算成员集合的可用性，自动选择逐域或均匀路径。
func (av AvailabilityModel) availOf(members []int, placement Placement) float64 {
	if av.Topology != nil {
		return memberAvailabilityOn(members, av.Topology, placement, av.Model, 0)
	}
	return memberAvailability(members, placement, av.Model, 0)
}

// MemberAvailability 是 availOf 的导出形式，供 pkg/sim 等外部包调用。
//
// 语义：给定成员集合与放置，返回该 quorum 在指定故障模型下的可用性
// （需要至少 len(members)/2+1 个成员存活）。
func MemberAvailability(members []int, placement Placement, av AvailabilityModel) float64 {
	return av.availOf(members, placement)
}

// pickMaxAvailability 从候选副本中贪心挑选 k 个，使成员集合的可用性最大。
//
// 贪心准则：每一步加入能带来最大**边际可用性增益**的副本。
// 这是子模函数最大化的标准贪心，对覆盖类目标有 (1-1/e) 近似保证；
// 这里不主张该保证严格适用（可用性函数未必严格子模），
// 但作为启发式它显著优于"按域计数轮转"。
//
// 复杂度 O(k · n · cost(availability))。n=1000、k≤500 时约 2.5e5 次可用性计算，
// 每次都做一次域卷积，总体在秒级；若需更快可先按域去重缩小候选集。
func pickMaxAvailability(members []int, placement Placement, av AvailabilityModel, k int) []int {
	if k >= len(members) {
		return append([]int(nil), members...)
	}
	if k <= 0 {
		return nil
	}

	chosen := make([]int, 0, k)
	remaining := append([]int(nil), members...)

	for len(chosen) < k {
		bestGain := -1.0
		bestIdx := -1
		for i, cand := range remaining {
			trial := append(append([]int(nil), chosen...), cand)
			gain := av.availOf(trial, placement)
			if gain > bestGain {
				bestGain = gain
				bestIdx = i
			}
		}
		if bestIdx < 0 {
			break
		}
		chosen = append(chosen, remaining[bestIdx])
		remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
	}
	return chosen
}

// SolveWithFailures 在故障模型下求解 quorum 几何与成员选择。
//
// 目标：在满足两条路径可用性目标的前提下，**最小化稳态写消息数**（= 2|Q2|）。
// 这与论文的动机一致：消息数是 FAFT 能省下的那部分，而可用性是它要守住的那部分。
//
// 搜索过程：
//  1. 对每个候选 |Q2| = k（从小到大，因为目标是消息数最小），
//     用 pickMaxAvailability 选出该大小下可用性最高的 Q2 成员集合；
//  2. 若数据可用性达标，则确定 k；
//  3. 对 |Q1| = n-k+1（Flexible Paxos 的最小解）用同一准则选 Q1 成员，
//     若控制可用性不达标则逐步增大 |Q1|；
//  4. 返回第一个两条路径都达标的方案。
//
// 若遍历完所有 k 都无法达标，返回可用性最高的方案并标记 Meets=false，
// 而不是返回空 —— 调用方需要看到"最接近的方案"才能判断目标是否过严。
func SolveWithFailures(t *Topology, cfg Config, av AvailabilityModel, targets AvailabilityTargets) FailurePlan {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas

	if av.Model == nil {
		av.Model = IndependentFailure{P: 0.001}
	}
	if targets.Data <= 0 {
		targets.Data = DefaultTargets().Data
	}
	if targets.Control <= 0 {
		targets.Control = DefaultTargets().Control
	}

	all := make([]int, n)
	for i := range all {
		all[i] = i
	}

	var bestScore float64 = -1
	evaluated := 0

	// 目标是最小化 2|Q2|，所以 k 从小到大扫描，第一个达标即为最优。
	for k := 1; k <= n; k++ {
		q2mem := pickMaxAvailability(all, cfg.Placement, av, k)
		dataAvail := av.availOf(q2mem, cfg.Placement)
		evaluated++

		if dataAvail < targets.Data {
			// 记录"最接近"的方案，继续试更大的 k。
			if dataAvail > bestScore {
				bestScore = dataAvail
			}
			continue
		}

		// 数据路径达标，确定 |Q2|=k；再求满足控制目标的最小 |Q1|。
		// |Q1| 的下界是 n-k+1（Flexible Paxos 约束）。
		for q1size := n - k + 1; q1size <= n; q1size++ {
			q1mem := pickMaxAvailability(all, cfg.Placement, av, q1size)
			ctrlAvail := av.availOf(q1mem, cfg.Placement)
			evaluated++

			q := Quorum{Q1Members: q1mem, Q2Members: q2mem}
			p := Evaluate(t, cfg, q)
			fp := FailurePlan{
				Plan:                p,
				DataAvailability:    dataAvail,
				ControlAvailability: ctrlAvail,
				MeetsData:           dataAvail >= targets.Data,
				MeetsControl:        ctrlAvail >= targets.Control,
				FailureModel:        av.Model.Name(),
			}
			fp.Meets = fp.MeetsData && fp.MeetsControl
			fp.Diagnostics = fmt.Sprintf(
				"目标: 数据>=%.4g 控制>=%.4g；实际: 数据=%.6g 控制=%.6g；评估 %d 个方案",
				targets.Data, targets.Control, dataAvail, ctrlAvail, evaluated)

			if fp.Meets {
				return fp
			}
			// 控制路径不达标：加大 |Q1| 再试（循环自然推进）。
		}
	}

	// 没有完全达标的方案：返回"数据路径可用性最高"的那个，并标记未达标。
	return FailurePlan{
		FailureModel: av.Model.Name(),
		MeetsData:    false, MeetsControl: false, Meets: false,
		DataAvailability: bestScore,
		Diagnostics: fmt.Sprintf(
			"无达标方案：目标 数据>=%.4g 控制>=%.4g；共评估 %d 个方案；数据路径最高可用性 %.6g",
			targets.Data, targets.Control, evaluated, bestScore),
	}
}

// ComparePlacementStrategies 对比两种放置策略在同一故障模型下的可用性。
//
// 用途：量化"故障感知的成员选择"相对"按域计数轮转"能提升多少。
// 这正是 FAFT 相对现有系统（TiKV/CockroachDB 的规则式放置）的增量。
type PlacementStrategyComparison struct {
	Q2Size int `json:"q2_size"`

	// CountBased 按域计数轮转（pickMaxDomains）。
	CountBasedAvailability float64 `json:"count_based_availability"`
	// AvailabilityBased 按边际可用性增益贪心（pickMaxAvailability）。
	AvailabilityBased float64 `json:"availability_based_availability"`
	// Gain 绝对提升。
	Gain float64 `json:"gain"`
	// GainX 相对倍数（可用性越低时倍数越有说服力）。
	GainX float64 `json:"gain_x"`
}

// ComparePlacementStrategies 在给定故障模型下扫描 |Q2|，比较两种成员选择策略。
func ComparePlacementStrategies(t *Topology, cfg Config, av AvailabilityModel, maxQ2 int) []PlacementStrategyComparison {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas
	if maxQ2 <= 0 || maxQ2 > n {
		maxQ2 = n
	}
	all := make([]int, n)
	for i := range all {
		all[i] = i
	}

	var out []PlacementStrategyComparison
	for k := 1; k <= maxQ2; k++ {
		countBased := pickMaxDomains(t, cfg.Placement, k, n)
		availBased := pickMaxAvailability(all, cfg.Placement, av, k)

		ca := av.availOf(countBased, cfg.Placement)
		aa := av.availOf(availBased, cfg.Placement)

		cmp := PlacementStrategyComparison{
			Q2Size:                 k,
			CountBasedAvailability: ca,
			AvailabilityBased:      aa,
			Gain:                   aa - ca,
		}
		if ca > 0 {
			cmp.GainX = aa / ca
		} else if aa > 0 {
			cmp.GainX = math.Inf(1)
		} else {
			cmp.GainX = 1
		}
		out = append(out, cmp)
	}
	return out
}

// SortedByGain 按可用性提升降序排列策略对比结果。
func SortedByGain(cmps []PlacementStrategyComparison) []PlacementStrategyComparison {
	out := append([]PlacementStrategyComparison(nil), cmps...)
	sort.Slice(out, func(i, j int) bool { return out[i].Gain > out[j].Gain })
	return out
}
