// pkg/faft/lowerbound.go
//
// **可证的下界**：任意放置 + 任意 quorum 选择下，写消息数最少能到多少。
//
// 消融阶梯只能说"A4 比可分子臂好"，说不出"离理论最优还差多少"。
// 没有下界，"我们的解是最优的"这句话没法证，只能说"我们试过的都更好"。
// 小规模穷举（ExhaustiveJoint，D^n ≤ 20 万）只能覆盖到 D ≤ 8；
// 下界把"最优性"推广到任意规模。
//
// ── 方向必须记牢（这里写反过一次）──────────────────────────────────
// 求**下界**要用可用性的**上界**：如果连"最理想的放置 + 最理想的 quorum"
// 都达不到目标，任何真实方案也达不到。所以每一步都必须是放宽，
// 一旦某步写成"某个具体放置下的可用性"，得到的数会**低于**真实可达值，
// 报出去就成了"我们的解超过了理论最优"。
//
// 这四条松弛都用上界，且都有对应的守门测试（lowerbound_test.go
// 逐点验证 `真实可用性 ≤ 上界`）：
//
//	① 两条路径分别取上界，不要求 Q1 与 Q2 同时成立（忽略耦合）。
//	② 忽略域容量与同域堆积，允许任意多个副本进入同一个域。
//	③ 无事件项：所有成员按最可靠域的单副本存活率算（对存活概率取 max）。
//	④ 事件项：取"成员数分布"（分拆）上可用性的**最大值**，
//	   并且不被事件命中的域按最可靠域的存活率算。
//
// ⚠️ 松弛 ④ 曾经写成"成员尽可能摊开，每个域一个"—— 那是**反的**：
// 区域事件整域打掉时，把成员堆在一个域里只需要"这个域不被命中"，
// 而摊开则需要"每个成员的域都不被命中"，后者严格更难。
// 实测 D=4、K=3、q=2（need=2）时真实可用性 0.945006 而"上界"给 0.932572，
// 上界被真实值击穿 1.2e-2 —— 正是这个方向错误。
// 现在改成对分拆取 max，两个方向都覆盖，谁的可用性高取谁。
//
// ── 与"LP 下界"的关系 ──────────────────────────────────────────────
// 严格说这不是线性规划，而是**组合松弛**：可用性是放置与成员选择的
// 非线性函数（逐域二项分布卷积 + 区域事件项）。写成 LP 需要先做保守
// 线性化，而线性化之后的界比这里更松；这里直接对非线性目标逐项放宽，
// 界更紧且仍然可证。论文里称 **relaxed lower bound**，并列出上面四条松弛。
package faft

import (
	"fmt"
	"math"
)

// maxPartitionQuorum 超过该 quorum 规模时不再枚举整数分拆（p(24)=1575）。
const maxPartitionQuorum = 24

// AvailabilityUpperBound 返回**任意**放置下、大小为 q 的 quorum 能达到的
// 可用性上界。语义与 memberAvailabilityOn 对齐：q 个成员中至少 ⌊q/2⌋+1 个存活。
func AvailabilityUpperBound(t *Topology, model FailureModel, q int) float64 {
	if q <= 0 {
		return 1
	}
	need := q/2 + 1
	if t == nil || len(t.Domains) == 0 {
		return survivalProbability(q, need, modelFailProb(model))
	}
	D := len(t.Domains)

	// 松弛 ③：所有成员都按最可靠域的单副本失效率算。
	pMin := 1.0
	for d := range t.Domains {
		if p := 1 - replicaAliveProb(t, model, d, 0); p < pMin {
			pMin = p
		}
	}
	noEvent := survivalProbability(q, need, pMin)

	Q := t.RegionalEventProb
	K := t.RegionalEventDomains
	if Q <= 0 || K <= 0 {
		return clamp01(noEvent)
	}
	if K >= D {
		// 事件打掉全部域：成员全灭，need ≥ 1 故可用性为 0。
		return clamp01((1 - Q) * noEvent)
	}

	// 事件项本身也不会超过无事件项：被打掉的域只会减少存活数。
	ev := eventTermUpperBound(D, K, q, need, pMin)
	if ev > noEvent {
		ev = noEvent
	}
	return clamp01((1-Q)*noEvent + Q*ev)
}

// eventTermUpperBound 返回区域事件下可用性的上界（不含 1-Q 权重）。
//
// 事件等概率命中 K 个域（见 Topology.RegionalEventScenarios），
// 未被命中的域内副本独立存活；本次放宽为"不被命中 => 全活"，
// 即把存活数只由"被命中域里有多少成员"决定。
func eventTermUpperBound(D, K, q, need int, pMin float64) float64 {
	tol := q - need
	if D <= maxScenarioDomains && q <= maxPartitionQuorum {
		return exactEventMax(D, K, q, need, tol, pMin)
	}
	// 域数过多（轮转采样）或 quorum 过大：用与命中集合无关的解析界。
	// 记 m_i 为第 i 个命中集合覆盖的成员数，则 Σ_i m_i = K·q 且 m_i ≤ q。
	// 存活要求 m_i ≤ tol，若这样的集合有 A 个，则
	//   K·q ≤ A·tol + (D-A)·q  =>  A ≤ D - (K·q - D·tol)/need
	hMin := 0
	if num := K*q - D*tol; num > 0 {
		hMin = (num + need - 1) / need
	}
	if hMin > D {
		hMin = D
	}
	return 1 - float64(hMin)/float64(D)
}

// exactEventMax 在"D ≤ maxScenarioDomains"（命中集合为全部 C(D,K) 组合、
// 对域标号对称）时，对**所有成员数分拆**取事件项可用性的最大值。
//
// 分拆 c_1..c_m（Σc = q，m ≤ min(q,D)）覆盖了任意真实放置的成员分布，
// 因此对分拆取 max 一定 ≥ 真实放置的取值 —— 这是"上界"的来源。
func exactEventMax(D, K, q, need, tol int, pMin float64) float64 {
	denom := math.Exp(logBinom(D, K))
	if denom <= 0 {
		return 1
	}
	best := 0.0
	counts := make([]int, 0, q)

	var rec func(remaining, maxPart int)
	rec = func(remaining, maxPart int) {
		if remaining == 0 {
			if v := partitionEventValue(counts, D, K, q, need, tol, pMin, denom); v > best {
				best = v
			}
			return
		}
		if len(counts) >= D {
			return
		}
		hi := maxPart
		if remaining < hi {
			hi = remaining
		}
		for p := hi; p >= 1; p-- {
			counts = append(counts, p)
			rec(remaining-p, p)
			counts = counts[:len(counts)-1]
		}
	}
	rec(q, q)
	return clamp01(best)
}

// partitionEventValue 计算给定成员数分拆在事件下的期望可用性（松弛 ④）。
//
// 事件命中 K 个域：若命中的域里共有 s 个成员（s ≤ tol 才有活路），
// 则存活数退化为 q-s 个"全活"副本，可用性 = survivalProbability(q-s, need, pMin)。
// 按"命中 j 个已占用域、成员数之和为 s"的组合数加权。
func partitionEventValue(
	counts []int, D, K, q, need, tol int, pMin, denom float64,
) float64 {
	m := len(counts)
	// dp[j][s]：从 m 个已占用域中选 j 个、成员数之和恰为 s 的方案数。
	dp := make([][]float64, m+1)
	for i := range dp {
		dp[i] = make([]float64, tol+1)
	}
	dp[0][0] = 1
	for _, c := range counts {
		for j := m - 1; j >= 0; j-- {
			for s := 0; s+c <= tol; s++ {
				if dp[j][s] != 0 {
					dp[j+1][s+c] += dp[j][s]
				}
			}
		}
	}

	var acc float64
	for j := 0; j <= m; j++ {
		rest := K - j
		if rest < 0 || rest > D-m {
			continue
		}
		waysOut := math.Exp(logBinom(D-m, rest))
		for s := 0; s <= tol; s++ {
			if dp[j][s] == 0 {
				continue
			}
			acc += dp[j][s] * waysOut * survivalProbability(q-s, need, pMin)
		}
	}
	return acc / denom
}

// QuorumLowerBound 是下界问题的解。
type QuorumLowerBound struct {
	Q2LB int `json:"q2_lower_bound"`
	Q1LB int `json:"q1_lower_bound"`
	// WriteMsgsLB = 2·Q2LB，写消息数的下界。
	WriteMsgsLB int `json:"write_msgs_lower_bound"`
	// Feasible：松弛问题下存在可行解。若为 false，说明连最理想的放置
	// 都达不到目标，真实系统更不可能。
	Feasible bool `json:"feasible_under_relaxation"`

	DataAvailAtLB float64 `json:"data_avail_upper_bound_at_q2lb"`
	CtrlAvailAtLB float64 `json:"control_avail_upper_bound_at_q1lb"`
	Relaxations   []string `json:"relaxations"`
}

// ComputeLowerBound 求解松弛问题，给出写消息数的可证下界。
//
//	minimize 2·q2
//	s.t.  q1 + q2 > n                        （Flexible Paxos 安全性）
//	      AvailabilityUpperBound(q2) ≥ data  （松弛 ①：两条路径分开取上界）
//	      AvailabilityUpperBound(q1) ≥ ctrl
//
// 正确性：任意真实可行方案 (placement, q1, q2) 都满足
// 真实可用性 ≤ 上界，故 (q1,q2) 也在上面的候选集里，
// 于是本函数返回的 q2 不大于任何真实可行方案的 q2。
func ComputeLowerBound(t *Topology, cfg Config, model FailureModel, targets AvailabilityTargets) QuorumLowerBound {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas

	res := QuorumLowerBound{
		Relaxations: []string{
			"① 两条路径分别取可用性上界，不要求 Q1 与 Q2 同时成立（忽略耦合）",
			"② 忽略域容量与同域堆积，允许任意多个副本进入最可靠的域",
			"③ 无事件项：所有成员都按最可靠域的单副本存活率计算",
			"④ 事件项：对成员数分拆取可用性最大值（堆叠与摊开都覆盖）",
		},
	}
	if n <= 0 {
		return res
	}

	ub := make([]float64, n+1)
	for q := 1; q <= n; q++ {
		ub[q] = AvailabilityUpperBound(t, model, q)
	}

	for q2 := 1; q2 <= n; q2++ {
		if ub[q2] < targets.Data {
			continue
		}
		q1min := n - q2 + 1
		if q1min < 1 {
			q1min = 1
		}
		for q1 := q1min; q1 <= n; q1++ {
			if ub[q1] < targets.Control {
				continue
			}
			res.Q2LB = q2
			res.Q1LB = q1
			res.WriteMsgsLB = 2 * q2
			res.Feasible = true
			res.DataAvailAtLB = ub[q2]
			res.CtrlAvailAtLB = ub[q1]
			return res
		}
	}
	return res
}

// OptimalityGap 把求解器的结果与下界对照。
type OptimalityGap struct {
	SolverQ2   int `json:"solver_q2"`
	SolverMsgs int `json:"solver_write_msgs"`
	LBQ2       int `json:"lower_bound_q2"`
	LBMsgs     int `json:"lower_bound_write_msgs"`
	// GapMsgs：求解器比下界多用了多少条消息。
	GapMsgs int `json:"gap_write_msgs"`
	// Ratio：求解器消息数 ÷ 下界。1.0 表示最优。
	Ratio float64 `json:"ratio_to_lower_bound"`
	// Optimal：GapMsgs == 0 —— 求解器达到了下界，因此在**消息数**上最优。
	// "达到下界"比"没超过下界"强得多：只要有可行方案，下界就必然可达。
	Optimal bool `json:"at_lower_bound"`
	// LBInfeasible：松弛问题都不可行 —— 目标定得过高，比消息数没有意义。
	LBInfeasible bool `json:"lower_bound_infeasible"`
	// SolverInfeasible：求解器没找到可行方案。
	SolverInfeasible bool `json:"solver_infeasible"`

	LBDataAvail float64 `json:"lb_data_avail_upper_bound"`
	LBCtrlAvail float64 `json:"lb_control_avail_upper_bound"`
}

// CompareToLowerBound 把求解器给出的方案与下界对照。
func CompareToLowerBound(
	t *Topology, cfg Config, av AvailabilityModel,
	targets AvailabilityTargets, solver FailurePlan,
) OptimalityGap {
	return CompareToLowerBoundWith(ComputeLowerBound(t, cfg, av.Model, targets), solver)
}

// CompareToLowerBoundWith 用**已经算好**的下界做对照。
//
// 拆这一层是性能需要：一张对照表要对同一个参数点比 N 个 planner，
// 而下界与 planner 无关。实测 48 个参数点 × 8 个 planner 的表里，
// 重复计算下界占了主要开销（整表 8 分钟以上，且随 planner 数线性增长）。
func CompareToLowerBoundWith(lb QuorumLowerBound, solver FailurePlan) OptimalityGap {
	g := OptimalityGap{
		SolverQ2:         len(solver.Quorum.Q2Members),
		SolverMsgs:       solver.WriteMsgsPerOp,
		LBQ2:             lb.Q2LB,
		LBMsgs:           lb.WriteMsgsLB,
		LBInfeasible:     !lb.Feasible,
		SolverInfeasible: !(solver.MeetsData && solver.MeetsControl),
		LBDataAvail:      lb.DataAvailAtLB,
		LBCtrlAvail:      lb.CtrlAvailAtLB,
	}
	if !lb.Feasible {
		return g
	}
	g.GapMsgs = g.SolverMsgs - g.LBMsgs
	if g.LBMsgs > 0 {
		g.Ratio = float64(g.SolverMsgs) / float64(g.LBMsgs)
	}
	g.Optimal = g.GapMsgs <= 0
	return g
}

// FormatOptimalityGap 渲染一行对照，供结果表使用。
func FormatOptimalityGap(label string, g OptimalityGap) string {
	mark := "  "
	switch {
	case g.LBInfeasible:
		mark = "∅ "
	case g.SolverInfeasible:
		mark = "✗ "
	case g.Optimal:
		mark = "★ "
	}
	return fmt.Sprintf("%s%-22s 求解器 |Q2|=%d 消息=%d ｜ 下界 |Q2|=%d 消息=%d ｜ 差距=%d（%.2f×）",
		mark, label, g.SolverQ2, g.SolverMsgs, g.LBQ2, g.LBMsgs, g.GapMsgs, g.Ratio)
}
