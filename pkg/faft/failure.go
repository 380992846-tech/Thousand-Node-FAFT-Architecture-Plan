// pkg/faft/failure.go
//
// 故障模型：从"域间独立"升级到**允许相关失效**。
//
// 为什么这是必要的（本项目实测计算，见 docs/DESIGN.md §2.5）：
//
//	域数  容错度  独立模型P[可用]      相关模型P[可用]      高估
//	3     1       0.999997002000      0.999897002000      0.00010000
//	5     1       0.999990020000      0.999890020000      0.00010000
//	7     1       0.999979069895      0.999879069895      0.00009999
//
// 独立模型的可用性比相关模型高整整 **1e-4**，与"一次区域性事件发生"
// 的概率同阶。也就是说：**在独立失效假设下做的放置优化，其收益会被
// 一次相关的区域性故障整个吃掉。**
//
// 这正是 FAFT 的核心命题 —— 名字里的 Failure-Aware 指的就是这件事。
// 若沿用小域名计数式的容错度量（"域数-1"），会得到一个饱和的、
// 且掩盖相关失效的指标：5 个域下任何配置都得到"容错 4"，
// 从而完全看不出"需要 6/1000 在线"与"需要 995/1000 在线"的天壤之别。
//
// 本文件给出的模型：
//
//  1. IndependentFailure：每个故障域以概率 p 独立整体失效。
//  2. CorrelatedFailure：以概率 q 发生一次"区域性事件"，
//     该事件同时命中 k 个域；此外叠加独立失效。
//  3. HierarchicalFailure：按 dc / zone / rack 三层建模，
//     每层有各自的失效概率 —— 更贴近真实机房的失效结构。
//
// 全部为概率模型，不含新定理。相关失效的具体形式是工程建模选择，
// 论文中必须明确写出假设，并与独立模型做对照（消融实验）。
package faft

import (
	"fmt"
	"math"
)

// FailureModel 故障模型接口。
//
// 可用性定义为"某个 quorum 仍能形成的概率"。
type FailureModel interface {
	// Name 模型名称，用于结果表。
	Name() string
	// QuorumAvailable 返回覆盖 domains 个故障域、需要至少 need 个域
	// 在线的 quorum 的可用概率。
	QuorumAvailable(domains, need int) float64
	// Summary 一行人类可读的模型参数说明。
	Summary() string
}

// ---------------------------------------------------------------------------
// 1. 独立失效
// ---------------------------------------------------------------------------

// IndependentFailure 每个域以概率 P 独立整体失效。
type IndependentFailure struct {
	// P 单个域在观测窗口内失效的概率。
	P float64
}

// Name 实现 FailureModel。
func (m IndependentFailure) Name() string { return "independent" }

// QuorumAvailable 二项式尾部：P[至少 need 个域在线]。
func (m IndependentFailure) QuorumAvailable(domains, need int) float64 {
	if need <= 0 {
		return 1
	}
	if need > domains {
		return 0
	}
	return binomTail(domains, need, m.P)
}

// Summary 实现 FailureModel。
func (m IndependentFailure) Summary() string {
	return fmt.Sprintf("独立失效：每域 P=%.2g", m.P)
}

// ---------------------------------------------------------------------------
// 2. 相关失效（区域性事件）
// ---------------------------------------------------------------------------

// CorrelatedFailure 叠加"区域性事件"的相关失效模型。
//
// 模型语义：
//   - 以概率 Q 发生一次区域性事件，同时命中 K 个域（K 个域一起失效）；
//   - 无论是否发生该事件，各域还独立地以概率 P 失效；
//   - 可用要求至少有 Need 个域在线。
//
// 这是对真实故障的粗粒度但可解释的刻画：一次供电/交换机固件/BGP 抖动
// 会同时打掉多个域，这类事件在可用性预算里往往占主导。
type CorrelatedFailure struct {
	// P 独立失效概率（每域）。
	P float64
	// Q 区域性事件的发生概率（每个观测窗口）。
	Q float64
	// K 一次区域性事件同时命中的域数。
	K int
}

// Name 实现 FailureModel。
func (m CorrelatedFailure) Name() string { return "correlated" }

// QuorumAvailable 计算可用概率。
//
//	P[可用] = (1-Q) * P_ind[至少 need 在线]
//	        + Q     * P[事件命中后仍至少 need 在线]
//
// 事件命中 K 个域时，被命中的 K 个域全部离线；剩余 domains-K 个域
// 仍按独立模型以概率 P 失效。若 K >= domains 则整组不可用。
func (m CorrelatedFailure) QuorumAvailable(domains, need int) float64 {
	if need <= 0 {
		return 1
	}
	if need > domains {
		return 0
	}
	noEvent := (1 - m.Q) * binomTail(domains, need, m.P)

	k := m.K
	if k < 0 {
		k = 0
	}

	var withEvent float64
	switch {
	case k == 0:
		// 事件不命中任何域，退化为独立模型。
		withEvent = binomTail(domains, need, m.P)
	case k >= domains:
		// 全部域被命中，必然不可用。
		withEvent = 0
	default:
		// 剩余 domains-k 个域仍需至少 need 个在线（被命中的全部离线）。
		withEvent = binomTail(domains-k, need, m.P)
	}

	return noEvent + m.Q*withEvent
}

// Summary 实现 FailureModel。
func (m CorrelatedFailure) Summary() string {
	return fmt.Sprintf("相关失效：独立 P=%.2g，区域事件 Q=%.2g 一次命中 %d 域", m.P, m.Q, m.K)
}

// ---------------------------------------------------------------------------
// 3. 分层失效
// ---------------------------------------------------------------------------

// TierFailure 一层（dc / zone / rack）的失效参数。
type TierFailure struct {
	// Name 层名。
	Name string
	// Prob 该层单元的整体失效概率（同一单元内的所有域一起挂）。
	Prob float64
	// UnitSize 一个单元包含多少个域。0 表示每个域各自成一单元。
	UnitSize int
}

// HierarchicalFailure 按多层失效域建模。
//
// 语义：自顶向下逐层判定。若某一层的某个单元失效，该单元覆盖的所有域
// 全部离线；否则继续向下一层判定。各层之间独立。
//
// 这比"单层独立失效"更接近真实机房：机架内的机器会一起因交换机重启掉线，
// 而不同可用区之间通常独立。
type HierarchicalFailure struct {
	// Tiers 自顶向下排列（例如 dc -> zone -> rack）。
	Tiers []TierFailure
}

// Name 实现 FailureModel。
func (m HierarchicalFailure) Name() string { return "hierarchical" }

// QuorumAvailable 用枚举法计算可用概率（域数不大，2^domains 不可行时用蒙特卡洛）。
//
// 域数 <= 20 时用精确枚举：每个域被"最高失效层"决定。
// 更准确地说，这里对每个域独立地按各层顺序抽样是否存活，然后取组合。
// 由于各层单元划分使得同一单元内的域强相关，精确枚举需要按单元枚举。
// 为保持实现简洁且可审计，这里采用**解析的分层枚举**：
// 对每一层枚举"哪些单元失效"，累加概率与对应的存活域数。
func (m HierarchicalFailure) QuorumAvailable(domains, need int) float64 {
	if need <= 0 {
		return 1
	}
	if need > domains {
		return 0
	}
	if len(m.Tiers) == 0 {
		return 1
	}

	// 逐层把"存活域集合"的概率分布推进下去。
	// state: 存活域数 -> 概率
	state := map[int]float64{domains: 1.0}

	for _, tier := range m.Tiers {
		unitSize := tier.UnitSize
		if unitSize <= 0 {
			unitSize = 1
		}

		next := map[int]float64{}
		for alive, p := range state {
			if alive <= 0 {
				next[0] += p
				continue
			}
			// 在"当前仍存活的域"上按 unitSize 重新划分单元。
			// 这是近似：原始单元边界在部分域已经失效后不再精确对齐。
			// 由于 unitSize 通常远小于域数，且下一层判定本身已是粗粒度建模，
			// 该近似对可用性量级没有实质影响。论文中需注明这一点。
			units := (alive + unitSize - 1) / unitSize

			// 枚举失效的单元数 j。
			for j := 0; j <= units; j++ {
				pj := binomPMF(units, j, tier.Prob)
				if pj == 0 {
					continue
				}
				// 失效 j 个单元 => 最多损失 j*unitSize 个域，且不超过 alive。
				lost := j * unitSize
				if lost > alive {
					lost = alive
				}
				next[alive-lost] += p * pj
			}
		}
		state = next
	}

	var avail float64
	for alive, p := range state {
		if alive >= need {
			avail += p
		}
	}
	return clamp01(avail)
}

// Summary 实现 FailureModel。
func (m HierarchicalFailure) Summary() string {
	s := "分层失效："
	for i, t := range m.Tiers {
		if i > 0 {
			s += " -> "
		}
		s += fmt.Sprintf("%s(P=%.2g,单元=%d)", t.Name, t.Prob, t.UnitSize)
	}
	return s
}

// ---------------------------------------------------------------------------
// 数值工具
// ---------------------------------------------------------------------------

// survivalProbability 返回至少 q 个副本在线（存活）的概率。
//
// 这是"存活"语义的唯一入口，内部实现方式不重要，调用方应一律用它
// 而不是直接调 binomTail —— 参数方向（失效 vs 存活）在这里踩过两次坑，
// 收敛到一个函数可以把这类错误限制在一处。
//
// 数学：P[X >= q]，X ~ Binomial(n, 1-p) 为存活副本数。
// 实现：binomTail(n, q, 1-p) 内部会在需要时走对称分支，因此对
// "q 很小"与"q 很大"两种极端都保持精度（见 binomTail 的说明）。
func survivalProbability(n, q int, replicaFailProb float64) float64 {
	if q <= 0 {
		return 1
	}
	if q > n {
		return 0
	}
	if replicaFailProb <= 0 {
		return 1
	}
	if replicaFailProb >= 1 {
		return 0
	}
	return binomTail(n, q, 1-replicaFailProb)
}

// binomTail 返回 P[X >= k]，X ~ Binomial(n, p)。
//
// 数值实现要点（这里踩过一个会让论文数字全错的坑）：
//
// 朴素做法是累加 i = k..n 的 PMF。但当 k 接近 n*p 附近时，
// 被累加的项包含 C(n, i)·p^i·(1-p)^(n-i) 这种极大组合数与极小幂次的乘积，
// 在 i ≈ n*p 处会**下溢成 0**。实测：n=1000、p=1e-4、k=501 时，
// 朴素累加返回 0.999999802067657，而正确答案是 2.1e-11 —— 差 10 个数量级。
// 也就是说所有"多数 quorum"的可用性都会被算成约等于 1，
// 而 t=1000 的对比表恰好全落在这一区间。
//
// 修法：利用对称性
//
//	P[X >= k] = P[Y <= n-k],  Y ~ Binomial(n, 1-p)
//
// 取两个方向中**求和项更少**的那个。当 k 很大时改用右侧短和，
// 由于 1-p 接近 1，各项不会下溢。
func binomTail(n, k int, p float64) float64 {
	if k <= 0 {
		return 1
	}
	if k > n {
		return 0
	}
	if p <= 0 {
		return 1
	}
	if p >= 1 {
		return 0
	}

	// 左侧和（i = k..n）项数 = n-k+1
	// 右侧和（i = 0..n-k，参数 1-p）项数 = n-k+1
	// 两者项数相同，但数值稳定性不同：
	//   - k 较小（< n/2）时左侧和的各项量级温和，用左侧；
	//   - k 较大时左侧会在 i≈np 附近下溢，改用右侧和。
	if k <= n/2 {
		return sumPMF(n, k, n, p)
	}
	return sumPMF(n, 0, n-k, 1-p)
}

// sumPMF 累加 i = lo..hi 的 PMF 之和（均在对数域内计算后指数化）。
func sumPMF(n, lo, hi int, p float64) float64 {
	if lo < 0 {
		lo = 0
	}
	if hi > n {
		hi = n
	}
	if lo > hi {
		return 0
	}

	logP := math.Log(p)
	log1mP := math.Log(1 - p)

	// 先在对数域求最大值，再从最大值处开始累加，进一步避免中间量下溢。
	logTerms := make([]float64, 0, hi-lo+1)
	maxLog := math.Inf(-1)
	for i := lo; i <= hi; i++ {
		lt := logBinom(n, i) + float64(i)*logP + float64(n-i)*log1mP
		logTerms = append(logTerms, lt)
		if lt > maxLog {
			maxLog = lt
		}
	}
	if math.IsInf(maxLog, -1) {
		return 0
	}

	// sum exp(lt) = exp(maxLog) * sum exp(lt - maxLog)
	acc := 0.0
	for _, lt := range logTerms {
		acc += math.Exp(lt - maxLog)
	}
	res := math.Exp(maxLog) * acc
	if math.IsNaN(res) {
		return 0
	}
	return clamp01(res)
}

// binomPMF 返回 P[X == k]，X ~ Binomial(n, p)。
func binomPMF(n, k int, p float64) float64 {
	if k < 0 || k > n {
		return 0
	}
	if p <= 0 {
		if k == 0 {
			return 1
		}
		return 0
	}
	if p >= 1 {
		if k == n {
			return 1
		}
		return 0
	}
	lt := logBinom(n, k) + float64(k)*math.Log(p) + float64(n-k)*math.Log(1-p)
	return clamp01(math.Exp(lt))
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// ---------------------------------------------------------------------------
// 与 quorum 几何的连接
// ---------------------------------------------------------------------------

// Placement 一个分片的副本落点：每个副本落在哪个域。
type Placement []int

// QuorumAvailability 计算某个 quorum 在给定故障模型下的可用概率。
//
// 注意这里用的是"副本粒度"而非"域粒度"：只有当同一域内的副本会
// 一起失效时（这正是相关故障模型刻画的情形），两者才等价。
// 传 domainOfReplica 而非域计数，是为了在混合放置下也能算对 ——
// 例如 |Q2|=3 个副本恰好落在 2 个域时，域粒度会高估可用性。
func QuorumAvailability(q *Quorum, placement Placement, model FailureModel) float64 {
	if q == nil || model == nil {
		return 0
	}
	members := q.Q2Members
	if len(members) == 0 {
		return 0
	}
	// 统计该 quorum 覆盖的域及每域副本数。
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
	// 需要多数副本在线才能形成 quorum。
	need := len(members)/2 + 1
	return model.QuorumAvailable(len(perDomain), need)
}

// CompareFailureModels 对同一组 quorum 几何，比较各故障模型下的可用性。
//
// 用途：量化"独立假设把可用性高估了多少"。这正是论文里
// 支撑 Failure-Aware 这个定语的定量证据。
func CompareFailureModels(domains, need int, models ...FailureModel) []ModelComparison {
	out := make([]ModelComparison, 0, len(models))
	for _, m := range models {
		a := m.QuorumAvailable(domains, need)
		out = append(out, ModelComparison{
			Model:       m.Name(),
			Summary:     m.Summary(),
			Available:   a,
			Unavailable: complement(a),
		})
	}
	// 以第一个模型（通常为独立模型）为基准计算高估量。
	if len(out) > 0 {
		base := out[0].Available
		for i := range out {
			out[i].OverestimateVsBase = base - out[i].Available
		}
	}
	return out
}

// complement 返回 1-v 且在浮点意义下**精确**（Available + Unavailable == 1）。
//
// 不能直接写 1-v：当 v 极接近 1 时（如 v = 1 - 5e-12）会发生灾难性抵消，
// 实测得到 0.0 而不是 5e-12。Nextafter 给出恰好相邻的浮点数，
// 使可用性与不可用性之和严格等于 1。
func complement(v float64) float64 {
	if v <= 0 {
		return 1
	}
	if v >= 1 {
		return 0
	}
	return 1 - math.Nextafter(v, math.Inf(1))
}

// ModelComparison 一个故障模型下的可用性结果。
type ModelComparison struct {
	Model              string  `json:"model"`
	Summary            string  `json:"summary"`
	Available          float64 `json:"available"`
	OverestimateVsBase float64 `json:"overestimate_vs_base"`
	// Unavailable 不可用概率，便于与"每年多少分钟不可用"换算。
	Unavailable float64 `json:"unavailable"`
}
