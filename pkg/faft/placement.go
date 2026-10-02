// pkg/faft/placement.go
//
// **联合求解「副本放置」与「quorum 几何」** —— 补上 FAFT 核心主张里缺失的那一半。
//
// ── 为什么需要这个文件 ───────────────────────────────────────────────
// 在它之前，pkg/faft 里的 Placement **永远是输入**：
//
//	geometry.go:229-231   cfg.Placement[i] = i % nDom
//
// `SolveWithFailures` 只在**给定放置**的前提下优化 quorum 成员子集。
// 而 DESIGN.md 的 C1/C2 主张的是"把「副本放在哪些故障域」与「每个分片用多大的
// quorum」当作**同一个优化问题**来解" —— 放置那一半从未实现。
//
// 这个缺口是 `faftbench sens` 的敏感性扫描当场暴露的：修掉两个对照设计错误之后，
// 联合求解在 90 个参数点上**按写消息数 0/90 占优**。查下来不是机制不成立，
// 而是被检验的东西根本不包含放置决策。
//
// ── 放置为什么能改变消息数 ───────────────────────────────────────────
// 写消息数 = 2|Q2|，而 |Q2| 是"满足可用性目标所需的最小提交 quorum"。
// 把副本放在更可靠的域、以及不会被同一次区域事件一起打掉的域上，
// **同一个 |Q2| 能拿到更高的可用性**，于是可以选更小的 |Q2| —— 这才省下消息。
//
// 放置本身不产生收益，"放置 + 几何一起选"才产生收益。这正是联合求解的意义。
package faft

import (
	"fmt"
	"math"
	"sort"
)

// JointPlan 是联合求解的结果，额外记录选中的放置。
type JointPlan struct {
	FailurePlan
	// Placement 选中的副本放置：Placement[i] = 第 i 个副本所在的域下标。
	Placement []int
	// PlacementSearch 搜索过程的说明（贪心步数、局部搜索改善次数）。
	PlacementSearch string
	// Evaluated 评估过的候选放置数量，用于说明搜索开销。
	Evaluated int
}

// bestJoint 内部用的比较键：先看可行性，再看写消息数，最后看可用性裕度。
type jointScore struct {
	meets    bool
	msgs     int
	margin   float64
	feasible bool
}

func scoreOf(fp FailurePlan, targets AvailabilityTargets) jointScore {
	return jointScore{
		meets:    fp.Meets,
		msgs:     fp.WriteMsgsPerOp,
		margin:   minRelMargin(fp.DataAvailability, fp.ControlAvailability, targets),
		feasible: fp.MeetsData && fp.MeetsControl,
	}
}

// betterThan 判断 a 是否严格优于 b。
//
// 判据顺序（论文里要写明）：
//  1. 先看可行性 —— 不可行的方案无论消息数多低都不参与比较；
//  2. **都不可行时，比谁更接近目标（可用性裕度），而不是比谁的消息少。**
//     达不到可用性目标时，"消息数更少"没有任何意义 —— 那只是更省地做错事。
//     `SolveWithFailures` 的文档也是这个口径：不可行时返回"最接近的方案"。
//  3. 都可行时，先比写消息数，再比可用性裕度。
//
// ⚠️ 第 2 条是测试逼出来的：原实现无论可行与否都先比消息数，
// 于是 placement_test.go 的 TestGreedyMatchesExhaustive 里出现
// "贪心选了 6 条消息、穷举选了 2 条消息"—— 两者**都不可行**，
// 但贪心的方案其实离目标更近（裕度 -1.0e-4 vs -6.5e-4）。
// 按消息数判它"更差"是错的判据。
func (a jointScore) betterThan(b jointScore) bool {
	if a.feasible != b.feasible {
		return a.feasible
	}
	if !a.feasible {
		// 都不可行：比谁更接近目标。
		return a.margin > b.margin
	}
	if a.msgs != b.msgs {
		return a.msgs < b.msgs
	}
	return a.margin > b.margin
}

// minRelMargin 把两条路径的可用性折算成"相对各自目标的裕度"，取较小者。
//
// 用相对裕度而不是绝对可用性：数据目标与控制目标常差一个数量级
// （0.999 vs 0.9999），直接比绝对值会让控制路径永远主导。
func minRelMargin(data, ctrl float64, t AvailabilityTargets) float64 {
	dm, cm := 0.0, 0.0
	if t.Data > 0 {
		dm = (data - t.Data) / t.Data
	}
	if t.Control > 0 {
		cm = (ctrl - t.Control) / t.Control
	}
	if dm < cm {
		return dm
	}
	return cm
}

// SolveJoint 联合优化副本放置与 quorum 几何。
//
// 搜索策略：**逐个放置副本的贪心 + 局部搜索精修**。
//
//	第一阶段（贪心）：对第 i 个副本，枚举它能去的每个域，把部分放置补齐成
//	完整放置后调用 SolveWithFailures 评估，取最好的那个域。复杂度 O(n·D) 次评估。
//
//	第二阶段（局部搜索）：反复尝试"把某个副本挪到另一个域"，接受任何改进，
//	直到没有改进或达到迭代上限。这一步是为了兜住贪心的短视 ——
//	前几个副本的选择可能把后面的路堵死，贪心本身看不出来。
//
// 为什么不做穷举：D^n 在 D=9,n=5 时是 59049（可行），但 D=1000 时是 10^15。
// 论文要支持千分片，所以必须是多项式搜索。用 exhaustivePlacements 在小规模上
// 与贪心结果对照，可以量化贪心的最优性差距（见 placement_test.go）。
func SolveJoint(t *Topology, cfg Config, av AvailabilityModel, targets AvailabilityTargets) JointPlan {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas
	if n <= 0 {
		n = 1
	}
	if len(t.Domains) == 0 {
		fp := SolveWithFailures(t, cfg, av, targets)
		return JointPlan{FailurePlan: fp, Placement: nil, PlacementSearch: "无域可用"}
	}
	if av.Model == nil {
		av.Model = IndependentFailure{P: 0.001}
	}

	// 容量约束：每个域最多放 Capacity 个副本（0 = 不限）。
	capOf := func(d int) int { return t.Domains[d].Capacity }
	used := make([]int, len(t.Domains))
	evaluated := 0

	evalWith := func(placement []int) FailurePlan {
		c := cfg
		c.Placement = placement
		evaluated++
		return SolveWithFailures(t, c, av, targets)
	}

	// ── 第一阶段：贪心逐个放置 ──
	placement := make([]int, 0, n)
	var cur FailurePlan
	for len(placement) < n {
		bestD := -1
		var bestFP FailurePlan
		var bestSc jointScore
		for d := range t.Domains {
			if c := capOf(d); c > 0 && used[d] >= c {
				continue
			}
			trial := append(append([]int{}, placement...), d)
			used[d]++
			full := completePlacement(t, trial, n, used)
			used[d]--
			fp := evalWith(full)
			sc := scoreOf(fp, targets)
			if bestD < 0 || sc.betterThan(bestSc) {
				bestD, bestFP, bestSc = d, fp, sc
			}
		}
		if bestD < 0 {
			break // 所有域都满了
		}
		placement = append(placement, bestD)
		used[bestD]++
		cur = bestFP
	}

	greedyLen := len(placement)
	if greedyLen < n {
		// 容量不足，放不满 n 个副本 —— 如实报告而不是静默截断。
		fp := evalWith(completePlacement(t, placement, n, used))
		return JointPlan{
			FailurePlan:     fp,
			Placement:       completePlacement(t, placement, n, used),
			PlacementSearch: fmt.Sprintf("贪心放置受阻：容量只够放 %d/%d 个副本", greedyLen, n),
			Evaluated:       evaluated,
		}
	}

	// ── 第二阶段：局部搜索精修 ──
	curPlacement := completePlacement(t, placement, n, used)
	curFP := cur
	curSc := scoreOf(curFP, targets)
	improvements := 0
	const maxRounds = 8
	for round := 0; round < maxRounds; round++ {
		improved := false
		for i := 0; i < n; i++ {
			from := curPlacement[i]
			for d := range t.Domains {
				if d == from {
					continue
				}
				// 容量检查：挪过去之后目标域不能超容。
				cnt := 0
				for _, x := range curPlacement {
					if x == d {
						cnt++
					}
				}
				if c := capOf(d); c > 0 && cnt >= c {
					continue
				}
				trial := append([]int{}, curPlacement...)
				trial[i] = d
				fp := evalWith(trial)
				sc := scoreOf(fp, targets)
				if sc.betterThan(curSc) {
					curPlacement, curFP, curSc = trial, fp, sc
					improved = true
					improvements++
				}
			}
		}
		if !improved {
			break
		}
	}

	curFP.PlacementAware = true

	// ── 保障：消融臂的解必须落在联合搜索的解空间里 ──
	//
	// 原则："**联合优化器永远不应该输给自己的消融臂**" ——
	// A2（优化放置 + majority 几何）与 A3（默认放置 + FAFT 几何）的方案
	// 都在联合搜索的可行域内，所以联合求解至少要能找回它们。
	//
	// 不加这一步会怎样：消融表里出现「▼ 联合反而不如某臂」，看起来像方法缺陷，
	// 实际是搜索没找到那个解。实测在 skew=0、K=3、Q≥1e-2 的 3 个点上出现过 ——
	// 全是域完全等价的退化区域。
	//
	// 这里至少把**默认放置**（A3 的放置）纳入比较；贪心本身已经会评估
	// 由 `completePlacement` 生成的摊开式放置，那覆盖了 A2 的主要形态。
	defaultPl := defaultPlacement(n, len(t.Domains))
	if !samePlacement(defaultPl, curPlacement) {
		dfp := evalWith(defaultPl)
		if scoreOf(dfp, targets).betterThan(curSc) {
			curFP = dfp
			curPlacement = defaultPl
			curSc = scoreOf(dfp, targets)
			improvements++
		}
	}

	curFP.PlacementAware = true
	curFP.Diagnostics = fmt.Sprintf(
		"%s｜联合求解：放置 %v（贪心 %d 步 + 局部搜索 %d 次改进，共评估 %d 个候选）",
		curFP.Diagnostics, curPlacement, greedyLen, improvements, evaluated)

	return JointPlan{
		FailurePlan: curFP,
		Placement:   curPlacement,
		PlacementSearch: fmt.Sprintf(
			"贪心 %d 步，局部搜索 %d 次改进，共评估 %d 个候选放置",
			greedyLen, improvements, evaluated),
		Evaluated: evaluated,
	}
}

// completePlacement 把部分放置补成 n 个副本的完整放置。
//
// ⚠️ **补齐必须摊到不同域，绝不能堆在同一个域里。** 这里踩过一个会让
// 整个搜索失效的坑：
//
//	第一版按"最可靠优先"补齐，于是把剩下的副本**全部堆进最可靠的那个域**。
//	结果贪心第一步的每个候选都长成「1 个副本在域 d + 4 个副本堆在域 7」——
//	候选之间几乎没有差别，贪心失去信号，最后一路走进 [7,7,7,7,7]
//	（5 个副本全在一个域），写消息数 10，比不优化的默认放置（2）还差。
//	placement_test.go 的 TestPlacementImprovesOverDefault 当场抓到了它。
//
// 为什么堆叠是坏的：区域事件会**整体打掉 K 个域**，堆叠把所有副本放在
// 同一个篮子里，一次事件就全灭。可用性模型里这一点是显式建模的
// （见 memberAvailabilityOn 的 RegionalEventScenarios），所以堆叠会被
// 正确惩罚 —— 前提是搜索空间里存在"摊开"的候选可供比较。
//
// 正确做法：**按可靠性排序轮转**填入尚未超容的域 —— "下一个副本放到当前
// 最好的、还没被用满的域"。这既避免堆叠，又保留"优先放可靠域"的偏好。
func completePlacement(t *Topology, partial []int, n int, used []int) []int {
	out := append([]int{}, partial...)
	// used 只作为容量基线：把已占用的槽位并进来，但"摊开"的判断用 inPlace。
	_ = used

	order := make([]int, len(t.Domains))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		da, db := t.Domains[order[a]], t.Domains[order[b]]
		if da.FailProb != db.FailProb {
			return da.FailProb < db.FailProb
		}
		return order[a] < order[b]
	})

	inPlace := map[int]int{}
	for _, d := range out {
		inPlace[d]++
	}
	budget := func(d int) bool {
		if c := t.Domains[d].Capacity; c > 0 && inPlace[d] >= c {
			return false
		}
		return true
	}

	for len(out) < n {
		pick := -1
		// 第一轮：只考虑**还没被用过**的域 —— 这是"摊开"的关键。
		//
		// ⚠️ 这里踩过一个隐蔽的坑：原实现直接遍历可靠性排序取第一个通过容量检查的域。
		// 而 Capacity 默认是 0（不限），于是**每次都会取同一个最可靠的域**，
		// 把剩余槽位全部填成它 —— 所谓的"轮转"根本没发生。
		// 后果是贪心第一步的所有候选都退化成"1 个副本 + 4 个副本堆在同一域"，
		// 最终选出 [7,7,7,7,7]（5 个副本一个域），比默认放置还差。
		for _, d := range order {
			if inPlace[d] > 0 {
				continue
			}
			if !budget(d) {
				continue
			}
			pick = d
			break
		}
		// 第二轮：所有域都用过了，才按可靠性允许重复（受容量约束）。
		if pick < 0 {
			for _, d := range order {
				if budget(d) {
					pick = d
					break
				}
			}
		}
		// 第三轮：容量确实放不下 n 个副本。仍然补满长度（下游按 Replicas
		// 索引，少了会越界），挑当前最不拥挤的域，并让调用方从
		// Diagnostics 看出这个配置物理上不可行。
		if pick < 0 {
			least := order[0]
			for _, d := range order {
				if inPlace[d] < inPlace[least] {
					least = d
				}
			}
			pick = least
		}
		out = append(out, pick)
		inPlace[pick]++
	}
	return out
}

// ExhaustiveJoint 穷举所有放置（仅用于小规模对照，验证贪心的最优性差距）。
//
// 复杂度 O(D^n)。**不要在大规模上用** —— D=1000, n=5 就是 10^15。
// 它的存在是为了在论文里回答"你的贪心离最优有多远"。
func ExhaustiveJoint(t *Topology, cfg Config, av AvailabilityModel, targets AvailabilityTargets) JointPlan {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas
	D := len(t.Domains)
	if D == 0 || n <= 0 {
		return JointPlan{FailurePlan: SolveWithFailures(t, cfg, av, targets)}
	}
	// 规模保护：超过 20 万个组合就拒绝，避免误用。
	total := 1
	for i := 0; i < n; i++ {
		total *= D
		if total > 200000 {
			return JointPlan{
				FailurePlan:     SolveWithFailures(t, cfg, av, targets),
				PlacementSearch: fmt.Sprintf("穷举被拒绝：D^n = %d^%d 超过 20 万", D, n),
			}
		}
	}

	best := -1
	var bestFP FailurePlan
	var bestPl []int
	evaluated := 0
	cur := make([]int, n)
	for {
		// 容量检查
		ok := true
		cnt := map[int]int{}
		for _, d := range cur {
			cnt[d]++
			if c := t.Domains[d].Capacity; c > 0 && cnt[d] > c {
				ok = false
				break
			}
		}
		if ok {
			c := cfg
			c.Placement = append([]int{}, cur...)
			fp := SolveWithFailures(t, c, av, targets)
			evaluated++
			sc := scoreOf(fp, targets)
			if best < 0 || sc.betterThan(scoreOf(bestFP, targets)) {
				best = 1
				bestFP = fp
				bestPl = append([]int{}, cur...)
			}
		}
		// 进位
		i := n - 1
		for i >= 0 {
			cur[i]++
			if cur[i] < D {
				break
			}
			cur[i] = 0
			i--
		}
		if i < 0 {
			break
		}
	}
	bestFP.PlacementAware = true
	bestFP.Diagnostics = fmt.Sprintf("%s｜穷举放置 %v（共评估 %d 个）",
		bestFP.Diagnostics, bestPl, evaluated)
	return JointPlan{
		FailurePlan:     bestFP,
		Placement:       bestPl,
		PlacementSearch: fmt.Sprintf("穷举 %d 个候选放置", evaluated),
		Evaluated:       evaluated,
	}
}

// ─────────────────────────────────────────────────────────────────────
// 消融阶梯：把「放置」与「几何」的贡献分开
//
// ── 为什么必须要这一节 ───────────────────────────────────────────────
// `faftbench sens` 用「联合求解」对比「baseline（放置固定为 i%nDom）」，
// 在 90 个参数点上取得 43 个写消息数严格占优（典型形态是 |Q2| 从 3 降到 1）。
//
// 但审稿人会立刻问：**baseline 的放置是写死的，而你的方法自己选放置 ——
// 这算不算给方法多给了一个自由变量？**
//
// 这个问题**只能靠消融回答**，不能靠论述。本节的四臂阶梯把两个自由度
// 分别锁定，隔离出各自的贡献：
//
//	A1 naive        默认放置   + majority 几何      现成做法（基线）
//	A2 placement    优化放置   + majority 几何      只看「放置」的贡献
//	A3 geometry     默认放置   + FAFT 几何/成员     只看「几何」的贡献
//	A4 joint        优化放置   + FAFT 几何/成员     完整方法
//
// 读法：
//	A2 − A1 = 放置单独的贡献
//	A3 − A1 = 几何/成员单独的贡献
//	A4 − max(A2, A3) = **「联合」本身带来的额外贡献** ← 这才是 C1 的证据
//
// 如果 A4 ≈ max(A2,A3)，说明两个自由度是**可加的**，那"联合优化"这个
// 提法就站不住 —— 分开做两次就够了。这个可能性必须被诚实地检验。
// ─────────────────────────────────────────────────────────────────────

// SolvePlacementOnly 固定 quorum 几何，**只优化放置**。
//
// 与 SolveJoint 的区别：几何（|Q1|、|Q2|）被钉死，因此写消息数是常数，
// 目标退化为"最大化可用性裕度"。搜索策略相同（贪心 + 局部搜索）。
//
// 用途：消融的 A2 臂 —— 隔离出"放置"单独的贡献。
func SolvePlacementOnly(t *Topology, cfg Config, av AvailabilityModel,
	targets AvailabilityTargets, q1size, q2size int) JointPlan {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas
	if len(t.Domains) == 0 || n <= 0 {
		fp := SolveWithFailures(t, cfg, av, targets)
		return JointPlan{FailurePlan: fp}
	}
	if av.Model == nil {
		av.Model = IndependentFailure{P: 0.001}
	}
	q1size = clampInt(q1size, 1, n)
	q2size = clampInt(q2size, 1, n)

	evalAt := func(placement []int) FailurePlan {
		c := cfg
		c.Placement = placement
		q := Quorum{
			Q1Members: firstN(q1size),
			Q2Members: firstN(q2size),
		}
		plan := Evaluate(t, c, q)
		fp := FailurePlan{
			Plan:                plan,
			DataAvailability:    av.availOf(q.Q2Members, placement),
			ControlAvailability: av.availOf(q.Q1Members, placement),
			FailureModel:        modelName(av.Model),
		}
		fp.MeetsData = fp.DataAvailability >= DefaultTargets().Data
		fp.MeetsControl = fp.ControlAvailability >= DefaultTargets().Control
		fp.Meets = fp.MeetsData && fp.MeetsControl
		fp.PlacementAware = true
		fp.Diagnostics = fmt.Sprintf("固定几何放置优化：|Q1|=%d |Q2|=%d 写消息=%d",
			q1size, q2size, plan.WriteMsgsPerOp)
		return fp
	}

	used := make([]int, len(t.Domains))
	evaluated := 0
	placement := make([]int, 0, n)
	for len(placement) < n {
		bestD := -1
		bestKey := math.Inf(-1)
		for d := range t.Domains {
			if c := t.Domains[d].Capacity; c > 0 && used[d] >= c {
				continue
			}
			trial := append(append([]int{}, placement...), d)
			used[d]++
			full := completePlacement(t, trial, n, used)
			used[d]--
			fp := evalAt(full)
			evaluated++
			// 几何固定 ⇒ 消息数是常数 ⇒ 只比可行性/裕度。
			key := armKey(fp, targets)
			if bestD < 0 || key > bestKey {
				bestD, bestKey = d, key
			}
		}
		if bestD < 0 {
			break
		}
		placement = append(placement, bestD)
		used[bestD]++
	}
	cur := completePlacement(t, placement, n, used)
	curFP := evalAt(cur)
	curKey := armKey(curFP, targets)

	// 局部搜索：固定几何下只需最大化裕度。
	for round := 0; round < 8; round++ {
		improved := false
		for i := 0; i < n; i++ {
			from := cur[i]
			for d := range t.Domains {
				if d == from {
					continue
				}
				cnt := 0
				for _, x := range cur {
					if x == d {
						cnt++
					}
				}
				if c := t.Domains[d].Capacity; c > 0 && cnt >= c {
					continue
				}
				trial := append([]int{}, cur...)
				trial[i] = d
				fp := evalAt(trial)
				evaluated++
				if k := armKey(fp, targets); k > curKey {
					cur, curFP, curKey = trial, fp, k
					improved = true
				}
			}
		}
		if !improved {
			break
		}
	}
	curFP.Diagnostics = fmt.Sprintf("%s｜放置 %v（共评估 %d 个候选）",
		curFP.Diagnostics, cur, evaluated)
	return JointPlan{
		FailurePlan:     curFP,
		Placement:       cur,
		PlacementSearch: fmt.Sprintf("固定几何放置搜索：评估 %d 个候选", evaluated),
		Evaluated:       evaluated,
	}
}

// AblationArm 消融阶梯的一臂。
type AblationArm struct {
	Key         string  `json:"key"`
	Name        string  `json:"name"`
	Placement   []int   `json:"placement"`
	Q1Size      int     `json:"q1_size"`
	Q2Size      int     `json:"q2_size"`
	WriteMsgs   int     `json:"write_msgs_per_op"`
	DataAvail   float64 `json:"data_availability"`
	CtrlAvail   float64 `json:"control_availability"`
	Feasible    bool    `json:"feasible"`
	MinMargin   float64 `json:"min_rel_margin"`
	Description string  `json:"description"`
}

// AblationLadder 跑四臂消融。
func AblationLadder(t *Topology, cfg Config, av AvailabilityModel, targets AvailabilityTargets) []AblationArm {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas
	maj := n/2 + 1

	mk := func(key, name, desc string, fp FailurePlan, pl []int) AblationArm {
		return AblationArm{
			Key: key, Name: name, Description: desc,
			Placement: pl,
			Q1Size:    len(fp.Quorum.Q1Members),
			Q2Size:    len(fp.Quorum.Q2Members),
			WriteMsgs: fp.WriteMsgsPerOp,
			DataAvail: fp.DataAvailability,
			CtrlAvail: fp.ControlAvailability,
			Feasible:  fp.MeetsData && fp.MeetsControl,
			MinMargin: minRelMargin(fp.DataAvailability, fp.ControlAvailability, targets),
		}
	}

	// A1：默认放置 + majority 几何（现成做法）
	a1 := SolveWithFailures(t, Config{
		Replicas: n, DataFaults: cfg.DataFaults, ControlFaults: cfg.ControlFaults,
		LogBytes: cfg.LogBytes, WriteRatio: cfg.WriteRatio,
		Placement: defaultPlacement(n, len(t.Domains)),
	}, av, AvailabilityTargets{Data: 0, Control: 0})
	// 注意：A1 用 majority 的门槛看可用性，而不是 FAFT 的目标，
	// 否则它会为了达标而改几何，"只看放置"这一臂就污染了。
	a1 = planWithFixedGeometry(t, cfg, av, defaultPlacement(n, len(t.Domains)), maj, maj)

	// A2：为 majority 几何优化的放置 + majority 几何
	a2j := SolvePlacementOnly(t, cfg, av, targets, maj, maj)

	// A3：默认放置 + FAFT 几何/成员（现有的 FaftPlanner）
	a3 := SolveWithFailures(t, cfg, av, targets)

	// A4：联合优化
	a4j := SolveJoint(t, cfg, av, targets)

	return []AblationArm{
		mk("A1", "naive", "默认放置 i%nDom + majority 几何（现成做法）",
			a1, defaultPlacement(n, len(t.Domains))),
		mk("A2", "placement-only", "**为 majority 几何优化的放置** + majority 几何 —— 隔离「放置」的贡献",
			a2j.FailurePlan, a2j.Placement),
		mk("A3", "geometry-only", "默认放置 + FAFT 几何与成员选择 —— 隔离「几何+成员」的贡献",
			a3, cfg.Placement),
		mk("A4", "joint", "**联合优化放置与几何**（完整方法）",
			a4j.FailurePlan, a4j.Placement),
	}
}

// planWithFixedGeometry 在给定放置下，用固定大小的 quorum 评估。
func planWithFixedGeometry(t *Topology, cfg Config, av AvailabilityModel,
	placement []int, q1size, q2size int) FailurePlan {
	c := cfg
	c.Placement = placement
	q := Quorum{Q1Members: firstN(q1size), Q2Members: firstN(q2size)}
	plan := Evaluate(t, c, q)
	fp := FailurePlan{
		Plan:                plan,
		DataAvailability:    av.availOf(q.Q2Members, placement),
		ControlAvailability: av.availOf(q.Q1Members, placement),
		FailureModel:        modelName(av.Model),
	}
	fp.MeetsData = fp.DataAvailability >= DefaultTargets().Data
	fp.MeetsControl = fp.ControlAvailability >= DefaultTargets().Control
	fp.Meets = fp.MeetsData && fp.MeetsControl
	return fp
}

// armKey 消融各臂的统一比较键：可行优先，其次裕度。
//
// 消融的各臂**几何不同**，写消息数不可直接比 —— 只能先比可行性，
// 再比可用性裕度。否则"A1 消息数是 6、A4 是 2"会被误读成 A4 更好，
// 而实际上 A1 可能是达标的而 A4 不是。
func armKey(fp FailurePlan, targets AvailabilityTargets) float64 {
	feasible := fp.MeetsData && fp.MeetsControl
	m := minRelMargin(fp.DataAvailability, fp.ControlAvailability, targets)
	if feasible {
		return 1000 + m // 可行的一律排在不可行之前
	}
	return m
}

func firstN(n int) []int {
	out := make([]int, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, i)
	}
	return out
}

func defaultPlacement(n, domains int) []int {
	if domains <= 0 {
		domains = 1
	}
	out := make([]int, n)
	for i := range out {
		out[i] = i % domains
	}
	return out
}

// samePlacement 判断两个放置是否逐位相同。
func samePlacement(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
type JointPlanner struct {
	Targets AvailabilityTargets
}

// Name 实现 QuorumPlanner。
func (p JointPlanner) Name() string { return "faft-joint" }

// Source 实现 QuorumPlanner。
func (p JointPlanner) Source() string {
	return "本项目。联合优化副本放置与 quorum 几何（pkg/faft/placement.go）"
}

// Notes 实现 QuorumPlanner。
func (p JointPlanner) Notes() string {
	return "把「副本落在哪个域」也当作决策变量，而不只是选 quorum 成员子集"
}

// Plan 实现 QuorumPlanner。
func (p JointPlanner) Plan(topo *Topology, cfg Config, av AvailabilityModel) FailurePlan {
	targets := p.Targets
	if targets.Data <= 0 && targets.Control <= 0 {
		targets = DefaultTargets()
	}
	jp := SolveJoint(topo, cfg, av, targets)
	return jp.FailurePlan
}
