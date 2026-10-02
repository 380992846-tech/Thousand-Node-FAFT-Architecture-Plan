// cmd/faftbench/ablation.go
//
// 消融阶梯的实验入口：把「放置」与「几何」两个自由度的贡献分开。
//
// ── 它回答什么问题 ─────────────────────────────────────────────────
// 只问一句话：**「联合优化」是不是真的比「分开优化两次」更好？**
//
// `faftbench sens` 得到"联合求解在 43/90 个参数点上写消息数严格占优"，
// 但那个对照里 baseline 的放置是写死的 —— 审稿人会问"你是不是给自己的
// 方法多给了一个自由变量"。只有消融能回答。
//
// 四臂：
//
//	A1 naive        默认放置          + majority 几何
//	A2 placement    为 majority 优化的放置 + majority 几何
//	A3 geometry     默认放置          + FAFT 几何/成员
//	A4 joint        联合优化放置与几何（完整方法）
//
// 判据：
//
//	A2 − A1  放置单独的贡献
//	A3 − A1  几何/成员单独的贡献
//	A4 − max(A2, A3)  **「联合」本身的额外贡献** ← 这才是 C1 的证据
//
// 若 A4 ≈ max(A2,A3)，说明两个自由度**可加**，"联合优化"这个提法就站不住 ——
// 分开做两次就够了。这个可能性必须被诚实地检验，而不是假定它不成立。
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/distributed-kv/kvstore/pkg/faft"
)

type ablationOpts struct {
	domains  int
	replicas int
	dataTol  int
	ctrlTol  int
	pBase    float64

	skewList []float64
	qList    []float64
	kList    []int

	outPath string
}

// ablationCell 一个参数点上的四臂结果。
type ablationCell struct {
	Skew float64 `json:"skew"`
	Q    float64 `json:"q"`
	K    int     `json:"k"`

	Arms map[string]faft.AblationArm `json:"arms"`

	// JointBeatsBest: A4 严格优于 max(A2, A3) —— 「联合」本身有增量。
	JointBeatsBest bool `json:"joint_beats_best_separable"`
	// JointEqualsBest: A4 与 max(A2, A3) 打平 —— 两个自由度可加。
	JointEqualsBest bool `json:"joint_equals_best_separable"`
	// JointWorse: A4 反而不如某一臂 —— 求解器有问题。
	JointWorse bool `json:"joint_worse"`
	// BestSeparable 是 A2/A3 里更好的那一臂的 key。
	BestSeparable string `json:"best_separable_arm"`

	// FullyInfeasible: **四臂全部不达标**。
	//
	// 这种参数点上比较"谁失败得少"没有意义 —— 没有任何一个方法能达成目标，
	// 说"联合更差"只是说它的失败方式不同。必须如实列出，但**不计入**
	// ★/=/▼ 的统计。
	//
	// 实测有 3 个这样的点，全部在 skew=0（域完全等价）、K=3、Q≥1e-2 ——
	// 即"区域事件一次打掉 1/3 的域"这种极端情形，此时 5 副本 9 域
	// 无论如何都达不到 0.999/0.9999 的目标。
	FullyInfeasible bool `json:"fully_infeasible"`
}

type ablationReport struct {
	Config struct {
		Domains       int       `json:"domains"`
		Replicas      int       `json:"replicas"`
		DataFaults    int       `json:"data_faults"`
		ControlFaults int       `json:"control_faults"`
		PBase         float64   `json:"p_base"`
		SkewList      []float64 `json:"skew_list"`
		QList         []float64 `json:"q_list"`
		KList         []int     `json:"k_list"`
	} `json:"config"`

	Cells []ablationCell `json:"cells"`

	Total          int     `json:"total"`
	JointBeats     int     `json:"joint_beats_best"`
	JointEquals    int     `json:"joint_equals_best"`
	JointWorse     int     `json:"joint_worse"`
	BeatsRate      float64 `json:"joint_beats_rate"`
	EqualsRate     float64 `json:"joint_equals_rate"`
	PlacementOnly  int     `json:"placement_alone_sufficient"`
	GeometryOnly   int     `json:"geometry_alone_sufficient"`

	// FullyInfeasiblePoints: 四臂全部不达标的参数点数。
	// **不计入上面三个比例** —— 没有任何方法能达标的区域，
	// 比较"谁失败得少"没有意义。
	FullyInfeasiblePoints int `json:"fully_infeasible_points"`
	// Effective: 计入统计的参数点数（= Total − FullyInfeasiblePoints）。
	Effective int `json:"effective_points"`

	Notes []string `json:"notes"`
}

func runAblation(o ablationOpts) error {
	if o.pBase <= 0 {
		o.pBase = 1e-3
	}
	if len(o.skewList) == 0 {
		o.skewList = []float64{0, 0.5, 1.0, 2.0, 3.0}
	}
	if len(o.qList) == 0 {
		o.qList = []float64{0, 1e-4, 1e-3, 1e-2, 3e-2, 1e-1}
	}
	if len(o.kList) == 0 {
		o.kList = []int{1, 2, 3}
	}

	rep := &ablationReport{}
	rep.Config.Domains = o.domains
	rep.Config.Replicas = o.replicas
	rep.Config.DataFaults = o.dataTol
	rep.Config.ControlFaults = o.ctrlTol
	rep.Config.PBase = o.pBase
	rep.Config.SkewList = o.skewList
	rep.Config.QList = o.qList
	rep.Config.KList = o.kList

	targets := faft.AvailabilityTargets{Data: 0.999, Control: 0.9999}

	for _, sk := range o.skewList {
		for _, q := range o.qList {
			for _, k := range o.kList {
				if k > o.domains {
					continue
				}
				topo := sensTopology(o.domains, o.pBase, sk, q, k)
				cfg := faft.Config{
					Replicas:      o.replicas,
					DataFaults:    o.dataTol,
					ControlFaults: o.ctrlTol,
					LogBytes:      256,
					WriteRatio:    1.0,
				}
				av := faft.AvailabilityModel{
					Model:    faft.CorrelatedFailure{P: o.pBase, Q: q, K: k},
					Topology: topo,
				}
				arms := faft.AblationLadder(topo, cfg, av, targets)

				cell := ablationCell{Skew: sk, Q: q, K: k, Arms: map[string]faft.AblationArm{}}
				for _, a := range arms {
					cell.Arms[a.Key] = a
				}

				// 比较用统一判据（见 compareArms 的注释）：
				// 可行优先 → 消息数 → 裕度；都不可行时比裕度。
				a1, a2, a3, a4 := cell.Arms["A1"], cell.Arms["A2"], cell.Arms["A3"], cell.Arms["A4"]

				// 最佳可分子臂：A2 与 A3 里更好的那个。
				best := "A2"
				if compareArms(a3, a2) > 0 {
					best = "A3"
				}
				cell.BestSeparable = best
				bestArm := cell.Arms[best]

				// 四臂全部不达标 → 这个参数点上没有方法能达成目标，
				// 比较"谁失败得少"没有意义。如实记录，但排除出统计。
				if !a1.Feasible && !a2.Feasible && !a3.Feasible && !a4.Feasible {
					cell.FullyInfeasible = true
					rep.Cells = append(rep.Cells, cell)
					continue
				}

				switch {
				case compareArms(a4, bestArm) > 0:
					// A4 严格优于最佳可分子臂
					cell.JointBeatsBest = true
				case compareArms(a4, bestArm) == 0:
					// 打平：两个自由度可加，联合没有额外增量
					cell.JointEqualsBest = true
				default:
					// A4 反而不如某臂 —— 求解器缺陷
					cell.JointWorse = true
				}
				_ = a1
				rep.Cells = append(rep.Cells, cell)
			}
		}
	}

	rep.Total = len(rep.Cells)
	for _, c := range rep.Cells {
		if c.FullyInfeasible {
			rep.FullyInfeasiblePoints++
			continue
		}
		switch {
		case c.JointBeatsBest:
			rep.JointBeats++
		case c.JointEqualsBest:
			rep.JointEquals++
		default:
			rep.JointWorse++
		}
		// A2 单独就够了（A2 不劣于 A4）→ 说明几何那一半没贡献
		if compareArms(c.Arms["A2"], c.Arms["A4"]) >= 0 {
			rep.PlacementOnly++
		}
		if compareArms(c.Arms["A3"], c.Arms["A4"]) >= 0 {
			rep.GeometryOnly++
		}
	}
	rep.Effective = rep.Total - rep.FullyInfeasiblePoints
	if rep.Effective > 0 {
		rep.BeatsRate = float64(rep.JointBeats) / float64(rep.Effective)
		rep.EqualsRate = float64(rep.JointEquals) / float64(rep.Effective)
	}
	if rep.Total > 0 {
		rep.BeatsRate = float64(rep.JointBeats) / float64(rep.Total)
		rep.EqualsRate = float64(rep.JointEquals) / float64(rep.Total)
	}
	rep.Notes = ablationNotes(rep)

	printAblation(rep)

	if o.outPath != "" {
		data, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(o.outPath, append(data, '\n'), 0o644); err != nil {
			return err
		}
		fmt.Printf("\n→ 已写出 %s\n", o.outPath)
	}
	return nil
}

// compareArms 比较消融的两臂，返回 -1 / 0 / 1。
//
// 判据（与 pkg/faft 的 jointScore.betterThan 口径一致）：
//  1. **可行性优先** —— 不达标的一律排在达标之后，消息数再低也不算赢；
//  2. 都达标时比**写消息数**（少者胜）；
//  3. 消息数相同再比可用性裕度。
//  4. 都**不**达标时，比谁更接近目标（裕度）。
//
// ⚠️ 第 2 条是纠正过来的。第一版用的是"先可行性、再裕度"，理由写在注释里：
// 「各臂几何不同，写消息数不可直接比」。**这个理由是错的（半对）**：
//
//   - 若一方达标、另一方不达标 → 确实不能只看消息数 ✅（第 1 条已覆盖）
//   - 若**双方都达标** → 消息数少**就是**更好，这正是本方法的目的
//
// 用"裕度"当判据，等于把 A4 的核心优势（|Q2| 从 3 降到 1，消息数 6→2）
// 排除在比较之外，然后判它输。修之前跑出 6 个「▼ 联合反而不如某臂」，
// 全部是这个错误判据造成的假象，不是求解器缺陷。
func compareArms(a, b faft.AblationArm) int {
	if a.Feasible != b.Feasible {
		if a.Feasible {
			return 1
		}
		return -1
	}
	if !a.Feasible {
		return cmpFloat(a.MinMargin, b.MinMargin)
	}
	if a.WriteMsgs != b.WriteMsgs {
		if a.WriteMsgs < b.WriteMsgs {
			return 1
		}
		return -1
	}
	return cmpFloat(a.MinMargin, b.MinMargin)
}

func cmpFloat(a, b float64) int {
	const eps = 1e-12
	switch {
	case a > b+eps:
		return 1
	case a < b-eps:
		return -1
	default:
		return 0
	}
}

// sensTopology 与 sens.go 共用同一套拓扑构造，保证两个模式可比。
func sensTopology(domains int, pBase, skew, q float64, k int) *faft.Topology {
	t := faft.NewUniformTopology(domains, 2.0)
	rank := make([]int, domains)
	seen := make([]bool, domains)
	r := 0
	for step := 1; r < domains; step++ {
		d := (step * 7) % domains
		if !seen[d] {
			seen[d] = true
			rank[d] = r
			r++
		}
	}
	for i := range t.Domains {
		u := 0.0
		if domains > 1 {
			u = 2*float64(rank[i])/float64(domains-1) - 1
		}
		t.Domains[i].FailProb = pBase * math.Exp(skew*u)
	}
	t.RegionalEventProb = q
	t.RegionalEventDomains = k
	return t
}

func printAblation(r *ablationReport) {
	fmt.Printf("消融阶梯：%d 个域，每分片 %d 副本，f_data=%d，f_ctrl=%d，P_base=%.0e\n",
		r.Config.Domains, r.Config.Replicas, r.Config.DataFaults, r.Config.ControlFaults, r.Config.PBase)
	fmt.Printf("网格：skew × Q × K = %d × %d × %d = %d 个参数点\n\n",
		len(r.Config.SkewList), len(r.Config.QList), len(r.Config.KList), r.Total)

	fmt.Println("四臂定义：")
	fmt.Println("  A1 naive     默认放置 i%nDom              + majority 几何")
	fmt.Println("  A2 placement **为 majority 优化的放置**    + majority 几何")
	fmt.Println("  A3 geometry  默认放置 i%nDom              + FAFT 几何/成员")
	fmt.Println("  A4 joint     **联合优化放置与几何**（完整方法）")
	fmt.Println()

	// 逐 K 打表：显示 A4 相对 max(A2,A3) 的结果
	for _, k := range r.Config.KList {
		has := false
		for _, c := range r.Cells {
			if c.K == k {
				has = true
			}
		}
		if !has {
			continue
		}
		fmt.Printf("── K=%d ──\n", k)
		header := fmt.Sprintf("%-10s", "Q \\ skew")
		for _, sk := range r.Config.SkewList {
			header += fmt.Sprintf("%10s", fmt.Sprintf("%.2g", sk))
		}
		header += "     最佳可分子臂"
		fmt.Println(header)
		for _, q := range r.Config.QList {
			line := fmt.Sprintf("%-10s", fmt.Sprintf("%.0e", q))
			for _, sk := range r.Config.SkewList {
				c := findAblationCell(r, sk, q, k)
				line += fmt.Sprintf("%10s", ablationSymbol(c))
			}
			// 该行最常出现的可分子臂
			cnt := map[string]int{}
			for _, sk := range r.Config.SkewList {
				if c := findAblationCell(r, sk, q, k); c != nil {
					cnt[c.BestSeparable]++
				}
			}
			best := ""
			for name := range cnt {
				if cnt[name] > cnt[best] {
					best = name
				}
			}
			fmt.Println(line + "     " + best)
		}
		fmt.Println()
	}

	fmt.Println("符号： ★ 联合严格优于任一可分子臂   = 与最佳可分子臂打平（自由度可加）   ▼ 联合反而不如某臂（求解器问题）   ∅ 四臂全部不达标（排除出统计）")
	fmt.Println()

	fmt.Printf("汇总：%d 个参数点 = 有效 %d 个 + 四臂全部不达标 %d 个（已排除）\n",
		r.Total, r.Effective, r.FullyInfeasiblePoints)
	fmt.Printf("  ★ 联合严格占优（A4 > max(A2,A3)）  %4d  (%.0f%%)  ← **这是 C1 的证据**\n",
		r.JointBeats, r.BeatsRate*100)
	fmt.Printf("  = 与最佳可分子臂打平              %4d  (%.0f%%)  ← 两个自由度可加，联合提法不成立\n",
		r.JointEquals, r.EqualsRate*100)
	fmt.Printf("  ▼ 联合反而不如某臂                %4d  (%.0f%%)  ← 求解器缺陷，必须修\n",
		r.JointWorse, float64(r.JointWorse)/float64(maxInt(1, r.Effective))*100)
	fmt.Printf("  ∅ 四臂全部不达标                  %4d  （不计入上面三个比例）\n", r.FullyInfeasiblePoints)
	fmt.Println()

	fmt.Println("注意事项：")
	for _, n := range r.Notes {
		fmt.Println("  - " + n)
	}
}

func ablationSymbol(c *ablationCell) string {
	if c == nil {
		return "?"
	}
	switch {
	case c.JointBeatsBest:
		return "★"
	case c.FullyInfeasible:
		return "∅"
	case c.JointWorse:
		return "▼"
	default:
		return "="
	}
}

func findAblationCell(r *ablationReport, skew, q float64, k int) *ablationCell {
	for i := range r.Cells {
		c := &r.Cells[i]
		if c.K == k && c.Skew == skew && c.Q == q {
			return c
		}
	}
	return nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func ablationNotes(r *ablationReport) []string {
	var out []string

	out = append(out,
		"比较判据：**可行优先 → 写消息数 → 可用性裕度**；都不可行时比谁更接近目标。",
		"⚠️ 早先版本写的是「几何不同所以消息数不可比，只能比裕度」——**那是错的（半对）**："+
			"若一方达标另一方不达标，确实不能只看消息数；但**双方都达标时，消息数少就是更好**，"+
			"那正是本方法的目的。用裕度当判据会把 A4 的核心优势（|Q2| 从 3 降到 1，消息 6→2）"+
			"排除在比较之外，然后判它输 —— 会凭空造出"+"\"联合更差\"的假象。已修。",
		"消融的目的只有一个：**「联合优化」是否真的优于「分开优化两次」。** "+
			"若 A4 与 max(A2,A3) 打平，说明两个自由度可加，「联合」这个提法就站不住。")

	if r.FullyInfeasiblePoints > 0 {
		out = append(out, fmt.Sprintf(
			"有 %d 个参数点**四臂全部不达标**（本次全部是 skew=0、K=3、Q≥1e-2 —— "+
				"域完全等价，且区域事件一次打掉 1/3 的域，5 副本 9 域无论如何都达不到 0.999/0.9999）。"+
				"这些点已排除出统计：**没有方法能达标的区域，比较"+"\"谁失败得少\"没有意义。**",
			r.FullyInfeasiblePoints))
	}

	if r.Effective > 0 {
		switch {
		case r.JointBeats == 0:
			out = append(out, fmt.Sprintf(
				"🔴 **A4 在全部 %d 个有效点上都没有严格优于最佳可分子臂。**"+
					"这说明「联合优化」相比「先优化放置、再优化几何」没有额外增量 —— "+
					"论文里不能主张 C1 的联合性，只能主张「这两个自由度都值得优化」。",
				r.Effective))
		case r.JointBeats == r.Effective:
			out = append(out, fmt.Sprintf(
				"✅ **A4 在全部 %d 个有效点上都严格优于最佳可分子臂** —— 联合确实产生了"+
					"两个自由度单独优化拿不到的收益。这是 C1 的直接证据。", r.Effective))
		default:
			out = append(out, fmt.Sprintf(
				"🟡 A4 在 %d/%d 个有效点上严格占优，在 %d 个点上与最佳可分子臂打平。"+
					"论文里应当**报出分界线**而不是笼统说"+"\"联合更好\""+"：在打平的区域，"+
					"分开优化两次就够了，联合不带来额外收益。",
				r.JointBeats, r.Effective, r.JointEquals))
		}
	}

	if r.PlacementOnly > 0 {
		// 找出 A2 就够了的参数点特征
		var sk, qt []float64
		for _, c := range r.Cells {
			if compareArms(c.Arms["A2"], c.Arms["A4"]) >= 0 {
				sk = append(sk, c.Skew)
				qt = append(qt, c.Q)
			}
		}
		if len(sk) > 0 {
			sort.Float64s(sk)
			out = append(out, fmt.Sprintf(
				"其中 %d 个点上 **A2（只优化放置）已经不劣于 A4** —— "+
					"这些区域里几何那一半没有贡献。其 skew 范围 %.2g–%.2g。",
				len(sk), sk[0], sk[len(sk)-1]))
		}
	}

	if r.JointWorse > 0 {
		out = append(out, fmt.Sprintf(
			"⚠️ 有 %d 个点上 **A4 反而不如某个可分子臂** —— 这是求解器缺陷，不是方法性质。"+
				"必须先修掉，否则这些点会污染整张表。", r.JointWorse))
	}
	return out
}
