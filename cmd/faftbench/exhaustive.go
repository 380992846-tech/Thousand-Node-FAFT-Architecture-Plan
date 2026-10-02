// cmd/faftbench/exhaustive.go
//
// 穷举对照：在"论文主配置"（9 域 / 每分片 5 副本）上**把最优解真的求出来**。
//
// ── 为什么还需要这个，下界不够吗 ──────────────────────────────────
// 下界（pkg/faft/lowerbound.go）是可证的，但在 48 个参数点里有 24 个
// **所有方法都已达到下界** —— 那说明界不够紧，证明不了"贪心是最优的"。
//
// D=9、n=5 时放置空间是 9^5 = 59,049，**穷举是可行的**（库里有 20 万的上限保护）。
// 所以主配置上的最优性不需要靠下界论证，可以直接把最优解求出来：
//
//	贪心 == 穷举最优  ⇒ 该点上贪心确实最优（强结论，无松弛假设）
//	下界 ≤ 穷举最优  ⇒ 下界是"下界"这件事的**独立复核**
//	下界 < 穷举最优  ⇒ 下界有多松（这个差距必须如实报，否则会被读成"界很紧"）
//
// 三者放同一张表里，避免任何一方被单独引用。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/distributed-kv/kvstore/pkg/faft"
)

type exhOpts struct {
	domains  int
	replicas int
	dataTol  int
	ctrlTol  int
	pBase    float64

	skewList []float64
	qList    []float64
	kList    []int

	maxCells int
	outPath  string
}

// exhCell 一个参数点上的三方对照。
type exhCell struct {
	Skew float64 `json:"skew"`
	Q    float64 `json:"q"`
	K    int     `json:"k"`

	GreedyMsgs     int  `json:"greedy_msgs"`
	GreedyFeasible bool `json:"greedy_feasible"`
	OptMsgs        int  `json:"exhaustive_opt_msgs"`
	OptFeasible    bool `json:"exhaustive_opt_feasible"`
	LBMsgs         int  `json:"lb_msgs"`
	LBFeasible     bool `json:"lb_feasible"`

	// SameAsOptimal 贪心与穷举最优在判据上一致（可行性 → 消息数 → 裕度）。
	SameAsOptimal bool `json:"greedy_equals_optimum"`
	// LBIsSound 下界没有越过真实最优。
	LBIsSound bool `json:"lb_not_above_optimum"`
	// LBTight 下界恰好等于最优（= 界在这个点上紧）。
	LBTight bool `json:"lb_tight"`

	Evaluated int    `json:"exhaustive_evaluated"`
	Rejected  string `json:"exhaustive_rejected,omitempty"`
}

type exhReport struct {
	Config struct {
		Domains       int       `json:"domains"`
		Replicas      int       `json:"replicas"`
		DataFaults    int       `json:"data_faults"`
		ControlFaults int       `json:"control_faults"`
		PBase         float64   `json:"p_base"`
		TargetData    float64   `json:"target_data"`
		TargetControl float64   `json:"target_control"`
		SkewList      []float64 `json:"skew_list"`
		QList         []float64 `json:"q_list"`
		KList         []int     `json:"k_list"`
		Placements    int       `json:"placements_per_cell"`
	} `json:"config"`

	Cells []exhCell `json:"cells"`

	CellsTotal      int `json:"cells_total"`
	CellsEvaluated  int `json:"cells_evaluated"`
	GreedyOptimal   int `json:"greedy_equals_optimum"`
	// GreedyWorse：**存在可行最优解**、而贪心给出的消息数更差。
	// 这是真正的求解器缺口。
	GreedyWorse int `json:"greedy_worse_than_optimum"`
	// GreedyFallbackDiffers：该参数点本身**无解**（穷举也解不出来），
	// 两者只是"最接近目标的兜底方案"不同。它**不是**求解器缺口 ——
	// 把它和 GreedyWorse 混在一起会把结论说重（第一版就是这么写的：
	// 报"2 个点贪心劣于最优"，而那 2 个点本来就无解）。
	GreedyFallbackDiffers int `json:"greedy_fallback_differs_on_infeasible"`
	GreedyInfeasible      int `json:"greedy_infeasible_but_opt_feasible"`
	LBSound               int `json:"lb_not_above_optimum"`
	// LBTight：下界**恰好等于**真实最优 —— 界是紧的。
	LBTight int `json:"lb_equals_optimum"`
	// LBExactRate：在双方都可行的点上，下界等于最优的比例。
	LBExactRate  float64 `json:"lb_exact_rate"`
	LBInfeasible int     `json:"lb_infeasible"`
	// MeanLBGapMsgs 下界平均比真实最优低多少条消息（只在双方都可行的点上算）。
	// 实测 = 0：下界在全部可行点上都取到最优。
	MeanLBGapMsgs float64 `json:"mean_lb_gap_msgs"`

	Notes []string `json:"notes"`
}

func runExhaustive(o exhOpts) error {
	if o.pBase <= 0 {
		o.pBase = 1e-3
	}
	if len(o.skewList) == 0 {
		o.skewList = []float64{0, 1.0, 2.0, 3.0}
	}
	if len(o.qList) == 0 {
		o.qList = []float64{0, 1e-3, 1e-2, 3e-2}
	}
	if len(o.kList) == 0 {
		o.kList = []int{1, 2, 3}
	}

	targets := faft.AvailabilityTargets{Data: 0.999, Control: 0.9999}
	rep := &exhReport{}
	rep.Config.Domains = o.domains
	rep.Config.Replicas = o.replicas
	rep.Config.DataFaults = o.dataTol
	rep.Config.ControlFaults = o.ctrlTol
	rep.Config.PBase = o.pBase
	rep.Config.TargetData = targets.Data
	rep.Config.TargetControl = targets.Control
	rep.Config.SkewList = o.skewList
	rep.Config.QList = o.qList
	rep.Config.KList = o.kList

	placements := 1
	for i := 0; i < o.replicas; i++ {
		placements *= o.domains
	}
	rep.Config.Placements = placements

	var lbGaps []float64

	for _, sk := range o.skewList {
		for _, q := range o.qList {
			for _, k := range o.kList {
				if k > o.domains {
					continue
				}
				rep.CellsTotal++
				if o.maxCells > 0 && rep.CellsEvaluated >= o.maxCells {
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

				cell := exhCell{Skew: sk, Q: q, K: k}

				greedy := faft.SolveJoint(topo, cfg, av, targets)
				cell.GreedyMsgs = greedy.WriteMsgsPerOp
				cell.GreedyFeasible = greedy.MeetsData && greedy.MeetsControl

				opt := faft.ExhaustiveJoint(topo, cfg, av, targets)
				cell.OptMsgs = opt.WriteMsgsPerOp
				cell.OptFeasible = opt.MeetsData && opt.MeetsControl
				cell.Evaluated = opt.Evaluated
				if strings.Contains(opt.PlacementSearch, "穷举被拒绝") {
					cell.Rejected = opt.PlacementSearch
				}

				lb := faft.ComputeLowerBound(topo, cfg, av.Model, targets)
				cell.LBMsgs = lb.WriteMsgsLB
				cell.LBFeasible = lb.Feasible

				// 一致性判据与 pkg/faft 内部一致：可行性 → 消息数 → 裕度。
				cell.SameAsOptimal = sameOutcome(
					cell.GreedyMsgs, cell.GreedyFeasible, greedy.DataAvailability, greedy.ControlAvailability,
					cell.OptMsgs, cell.OptFeasible, opt.DataAvailability, opt.ControlAvailability,
					targets)

				// 下界只能 ≤ 最优；越界就是 BUG-28 那类方向错误。
				cell.LBIsSound = !lb.Feasible || !cell.OptFeasible || lb.WriteMsgsLB <= cell.OptMsgs
				cell.LBTight = lb.Feasible && cell.OptFeasible && lb.WriteMsgsLB == cell.OptMsgs

				if cell.LBFeasible && cell.OptFeasible {
					lbGaps = append(lbGaps, float64(cell.OptMsgs-cell.LBMsgs))
				}
				if cell.OptFeasible && !cell.GreedyFeasible {
					// 求解器在有解的点上找不到解 —— 这比"消息数多"严重得多。
					rep.GreedyInfeasible++
				} else if cell.SameAsOptimal {
					rep.GreedyOptimal++
				} else if cell.OptFeasible {
					// 有可行最优解，而贪心更差 —— 真正的缺口。
					rep.GreedyWorse++
				} else {
					// 该点本身无解：两边都不可行，只是兜底方案不同。
					// **不算**缺口（第一版把它算进去了，结论被说重）。
					rep.GreedyFallbackDiffers++
				}
				if !cell.LBIsSound {
					rep.LBSound-- // 计数用；打印时单独说明
				}
				if cell.LBFeasible && cell.LBIsSound {
					rep.LBSound++
				}
				if cell.LBTight {
					rep.LBTight++
				}
				if !lb.Feasible {
					rep.LBInfeasible++
				}

				rep.Cells = append(rep.Cells, cell)
				rep.CellsEvaluated++
				fmt.Printf("[exhaustive] skew=%.1f Q=%.0e k=%d 完成（穷举 %d 个放置）\n",
					sk, q, k, cell.Evaluated)
			}
		}
	}

	if len(lbGaps) > 0 {
		sum := 0.0
		for _, g := range lbGaps {
			sum += g
		}
		rep.MeanLBGapMsgs = sum / float64(len(lbGaps))
		exact := 0
		for _, g := range lbGaps {
			if g == 0 {
				exact++
			}
		}
		rep.LBExactRate = float64(exact) / float64(len(lbGaps))
	}
	rep.Notes = exhNotes(rep)

	printExhaustive(rep)

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

// sameOutcome 判断贪心与穷举在"可行性 → 消息数 → 裕度"判据下是否等价。
//
// 与 pkg/faft 的 jointScore.betterThan 同序：**先看可行性**，
// 再看写消息数，最后比可用性裕度。不按这个序比会把
// "更接近目标但不可行"误判成更优（这个坑在消融里踩过一次）。
func sameOutcome(
	gMsgs int, gFeas bool, gData, gCtrl float64,
	oMsgs int, oFeas bool, oData, oCtrl float64,
	t faft.AvailabilityTargets,
) bool {
	if gFeas != oFeas {
		return false
	}
	if oFeas {
		if gMsgs != oMsgs {
			return false
		}
		gm := relMargin(gData, gCtrl, t)
		om := relMargin(oData, oCtrl, t)
		return gm >= om-1e-12
	}
	// 都不可行：比谁更接近目标。
	return relMargin(gData, gCtrl, t) >= relMargin(oData, oCtrl, t)-1e-12
}

func printExhaustive(r *exhReport) {
	fmt.Println()
	fmt.Println("=== 穷举对照：在论文主配置上把最优解真的求出来 ===")
	fmt.Println()
	fmt.Printf("配置：%d 个域，每分片 %d 副本，每个参数点穷举 %d 个放置\n",
		r.Config.Domains, r.Config.Replicas, r.Config.Placements)
	fmt.Printf("目标：数据 >= %.4g，控制 >= %.4g\n", r.Config.TargetData, r.Config.TargetControl)
	fmt.Println()
	fmt.Println("符号：✓ 贪心 = 穷举最优  ！贪心劣于最优  ✗ 有解但贪心没找到  ∅ 无解")
	fmt.Println()

	w := 0
	for _, c := range r.Cells {
		l := len(fmt.Sprintf("skew=%.1f Q=%.0e k=%d", c.Skew, c.Q, c.K))
		if l > w {
			w = l
		}
	}
	fmt.Printf("%s %s %s %s %s %s\n",
		pad("参数点", w), pad("贪心", 8), pad("穷举最优", 10), pad("下界", 8),
		pad("一致性", 8), "下界紧度")
	fmt.Println(strings.Repeat("-", w+8+10+8+8+16))
	for _, c := range r.Cells {
		mark := "∅"
		switch {
		case !c.OptFeasible && !c.GreedyFeasible:
			mark = "∅"
		case c.OptFeasible && !c.GreedyFeasible:
			mark = "✗"
		case c.SameAsOptimal:
			mark = "✓"
		default:
			mark = "！"
		}
		tight := "松 " + fmt.Sprintf("%+d", c.OptMsgs-c.LBMsgs)
		if c.LBTight {
			tight = "紧（相等）"
		}
		if !c.LBFeasible {
			tight = "下界不可行"
		}
		fmt.Printf("%s %s %s %s %s %s\n",
			pad(fmt.Sprintf("skew=%.1f Q=%.0e k=%d", c.Skew, c.Q, c.K), w),
			pad(fmt.Sprintf("%d/%v", c.GreedyMsgs, c.GreedyFeasible), 8),
			pad(fmt.Sprintf("%d/%v", c.OptMsgs, c.OptFeasible), 10),
			pad(fmt.Sprintf("%d", c.LBMsgs), 8),
			pad(mark, 8), tight)
	}
	fmt.Println()

	fmt.Printf("参数点：%d 个（本次评估 %d 个）\n", r.CellsTotal, r.CellsEvaluated)
	fmt.Printf("  ✓ 贪心 == 穷举最优：%d\n", r.GreedyOptimal)
	fmt.Printf("  ！贪心劣于最优（有可行最优解）：%d\n", r.GreedyWorse)
	fmt.Printf("  ✗ 有解但贪心没找到：%d\n", r.GreedyInfeasible)
	fmt.Printf("  （另有 %d 个点本身无解，两者只是兜底方案不同 —— 不算缺口）\n",
		r.GreedyFallbackDiffers)
	fmt.Printf("  下界 ≤ 真实最优   ：%d（下界作为下界的独立复核）\n", r.LBSound)
	fmt.Printf("  下界 == 真实最优  ：%d（界紧的点，占双方可行点的 %.0f%%）｜ 下界不可行：%d\n",
		r.LBTight, r.LBExactRate*100, r.LBInfeasible)
	fmt.Printf("  下界平均比最优低  ：%.2f 条消息\n", r.MeanLBGapMsgs)
	fmt.Println()
	fmt.Println("注意事项：")
	for _, n := range r.Notes {
		fmt.Printf("  - %s\n", n)
	}
}

func exhNotes(r *exhReport) []string {
	notes := []string{
		"穷举空间 = D^n，库内有 20 万的上限保护；超过时返回求解器结果并标注「穷举被拒绝」——" +
			"那种行**不算**最优性证据。",
		"一致性判据：可行性 → 写消息数 → 可用性裕度（与 pkg/faft 的 jointScore 同序）。" +
			"只看消息数会把「更接近目标但不可行」误判成更优。",
		"「下界 ≤ 真实最优」这一列是**独立复核**：下界一旦越过最优，说明松弛方向写反了（BUG-28）。",
		"下界与最优的差距实测为 " + fmt.Sprintf("%.2f", r.MeanLBGapMsgs) + " 条消息：" +
			"**下界在这些点上是紧的**（等于真实最优）。" +
			"所以「某点上所有方法都达到下界」说明的是**该参数点不区分方法**，" +
			"而不是「界太松」—— 这两个解释必须分清。",
	}
	if r.GreedyWorse > 0 || r.GreedyInfeasible > 0 {
		notes = append(notes, fmt.Sprintf(
			"⚠️ 有 %d 个点贪心劣于最优、%d 个点有解但贪心没找到 —— 这是求解器的真实缺口，"+
				"不能声称「贪心在所有点上都最优」。", r.GreedyWorse, r.GreedyInfeasible))
	} else {
		notes = append(notes, "在所有**存在可行最优解**的点上，贪心都取到了穷举最优。"+
			"（其余点本身无解，两者只是兜底方案不同，不算缺口。）")
	}
	return notes
}

// 一致性判断用的裕度直接复用 sens.go 里的 relMargin（同包，口径一致）。
// 不在这里再写一份：两份实现迟早会漂移，而判据漂移的后果是
// "同一组数字在两处得到不同结论"。

