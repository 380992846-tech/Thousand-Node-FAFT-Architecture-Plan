// cmd/faftbench/lowerbound.go
//
// 下界对照表的实验入口：回答「离理论最优还差多远」。
//
// 消融阶梯（ablation.go）只能回答「A4 比分开优化更好」。
// 审稿人接着会问：**那它是不是最优的？** 没有下界，这个问题只能回答
// "我们试过的都更好"。本模式把每个 planner 的写消息数与**可证下界**
// （pkg/faft/lowerbound.go，四条松弛，附守门测试）并排摆出：
//
//	差距 = 0  ⇒ 达到下界 ⇒ 在该目标下**消息数最优**（强结论）
//	差距 > 0  ⇒ 只说明"没证明最优"，不说明它不好 —— 界本身可能偏松
//	下界不可行 ⇒ 该目标连最理想的放置都达不到，比消息数没有意义
//
// 第三行必须显式统计：把"参数点本身不可达"混进"方法失败"里
// 会得出完全相反的结论。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/distributed-kv/kvstore/pkg/faft"
)

type lbOpts struct {
	domains  int
	replicas int
	dataTol  int
	ctrlTol  int
	pBase    float64

	skewList []float64
	qList    []float64
	kList    []int

	nSweep bool
	outPath string
}

// lbArm 一个 planner 在一个参数点上的结果。
type lbArm struct {
	Planner      string  `json:"planner"`
	Feasible     bool    `json:"feasible"`
	Q2           int     `json:"q2"`
	Msgs         int     `json:"write_msgs"`
	Gap          int     `json:"gap_vs_lower_bound"`
	Ratio        float64 `json:"ratio_to_lower_bound"`
	AtLowerBound bool    `json:"at_lower_bound"`
}

// lbCell 一个参数点：下界 + 全部 planner。
type lbCell struct {
	Skew float64 `json:"skew"`
	Q    float64 `json:"q"`
	K    int     `json:"k"`

	LBFeasible bool `json:"lb_feasible"`
	LBQ2       int  `json:"lb_q2"`
	LBQ1       int  `json:"lb_q1"`
	LBMsgs     int  `json:"lb_msgs"`

	Arms []lbArm `json:"arms"`
}

type lbSummary struct {
	Planner string `json:"planner"`
	// CellsEvaluated：下界可行的参数点（在这些点上比消息数才有意义）。
	CellsEvaluated int `json:"cells_lb_feasible"`
	AtLB           int `json:"at_lower_bound"`
	AboveLB        int `json:"above_lower_bound"`
	// MissedTarget：下界说可行、而该 planner 不可行 —— 目标可达但它没达到。
	MissedTarget int `json:"missed_reachable_target"`
	// MedianRatio：在可行的参数点上，消息数 ÷ 下界的中位数。
	MedianRatio float64 `json:"median_ratio"`
}

type lbReport struct {
	Config struct {
		Domains      int       `json:"domains"`
		Replicas     int       `json:"replicas"`
		DataFaults   int       `json:"data_faults"`
		ControlFaults int      `json:"control_faults"`
		PBase        float64   `json:"p_base"`
		TargetData   float64   `json:"target_data"`
		TargetCtrl   float64   `json:"target_control"`
		SkewList     []float64 `json:"skew_list"`
		QList        []float64 `json:"q_list"`
		KList        []int     `json:"k_list"`
	} `json:"config"`

	Cells   []lbCell    `json:"cells"`
	Summary []lbSummary `json:"summary"`

	LBCellsFeasible   int `json:"lb_feasible_cells"`
	LBCellsInfeasible int `json:"lb_infeasible_cells"`

	// DifferentiatingCells：存在"可达但未达"方法的参数点数量。
	//
	// 它是这张表**唯一有区分力**的部分：若一个点上所有可行方法都已达到下界，
	// 那这个点只能说明"下界不紧"，不能说明任何方法更好。
	// 只报 "36/36 达到下界" 会被误读成"随便哪个方法都最优"。
	DifferentiatingCells int `json:"differentiating_cells"`

	NSweep []lbNSweepRow `json:"n_sweep,omitempty"`

	Notes []string `json:"notes"`
}

type lbNSweepRow struct {
	N          int     `json:"n"`
	LBQ2       int     `json:"lb_q2"`
	LBMsgs     int     `json:"lb_msgs"`
	JointMsgs  int     `json:"joint_msgs"`
	JointFeas  bool    `json:"joint_feasible"`
	JointRatio float64 `json:"joint_ratio"`
	MajorityMsgs int   `json:"majority_msgs"`
}

func runLowerBound(o lbOpts) error {
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

	rep := &lbReport{}
	rep.Config.Domains = o.domains
	rep.Config.Replicas = o.replicas
	rep.Config.DataFaults = o.dataTol
	rep.Config.ControlFaults = o.ctrlTol
	rep.Config.PBase = o.pBase
	rep.Config.TargetData = targets.Data
	rep.Config.TargetCtrl = targets.Control
	rep.Config.SkewList = o.skewList
	rep.Config.QList = o.qList
	rep.Config.KList = o.kList

	planners := append(
		[]faft.QuorumPlanner{faft.JointPlanner{Targets: targets}},
		faft.DefaultBaselines(targets)...,
	)

	ratios := map[string][]float64{}

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

				lb := faft.ComputeLowerBound(topo, cfg, av.Model, targets)
				cell := lbCell{
					Skew: sk, Q: q, K: k,
					LBFeasible: lb.Feasible,
					LBQ2:       lb.Q2LB,
					LBQ1:       lb.Q1LB,
					LBMsgs:     lb.WriteMsgsLB,
				}
				if lb.Feasible {
					rep.LBCellsFeasible++
				} else {
					rep.LBCellsInfeasible++
				}

				for _, p := range planners {
					plan := p.Plan(topo, cfg, av)
					// 下界与 planner 无关：算一次，比 N 次。
					g := faft.CompareToLowerBoundWith(lb, plan)
					arm := lbArm{
						Planner:      p.Name(),
						Feasible:     !g.SolverInfeasible,
						Q2:           g.SolverQ2,
						Msgs:         g.SolverMsgs,
						Gap:          g.GapMsgs,
						Ratio:        g.Ratio,
						AtLowerBound: g.Optimal && g.SolverMsgs > 0,
					}
					if !lb.Feasible {
						// 下界不可行时消息数不可比 —— 标 0，统计里单独算。
						arm.Gap, arm.Ratio, arm.AtLowerBound = 0, 0, false
					} else if !arm.Feasible {
						arm.Gap, arm.Ratio, arm.AtLowerBound = 0, 0, false
					} else {
						ratios[p.Name()] = append(ratios[p.Name()], arm.Ratio)
					}
					cell.Arms = append(cell.Arms, arm)
				}
				rep.Cells = append(rep.Cells, cell)
			}
		}
	}

	// 汇总：只在"下界可行"的参数点上统计，否则会把不可达目标算成方法失败。
	names := make([]string, 0, len(planners))
	for _, p := range planners {
		names = append(names, p.Name())
	}
	rep.Summary, rep.DifferentiatingCells = summarizeLB(rep.Cells, names, ratios)

	if o.nSweep {
		rep.NSweep = lbNSweep(o, targets, planners)
	}

	rep.Notes = lbNotes(rep)

	printLowerBound(rep)

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

// lbNSweep 固定故障参数、扫描副本数，看下界与求解器如何随规模变化。
func lbNSweep(o lbOpts, targets faft.AvailabilityTargets, planners []faft.QuorumPlanner) []lbNSweepRow {
	var rows []lbNSweepRow
	for _, n := range []int{3, 5, 7, 9, 11, 21} {
		topo := sensTopology(o.domains, o.pBase, 2.0, 1e-3, 2)
		cfg := faft.Config{Replicas: n, DataFaults: o.dataTol, ControlFaults: o.ctrlTol, WriteRatio: 1.0}
		av := faft.AvailabilityModel{
			Model:    faft.CorrelatedFailure{P: o.pBase, Q: 1e-3, K: 2},
			Topology: topo,
		}
		lb := faft.ComputeLowerBound(topo, cfg, av.Model, targets)
		row := lbNSweepRow{N: n, LBQ2: lb.Q2LB, LBMsgs: lb.WriteMsgsLB}

		for _, p := range planners {
			if p.Name() != "faft-joint" && p.Name() != "majority" {
				continue
			}
			plan := p.Plan(topo, cfg, av)
			g := faft.CompareToLowerBoundWith(lb, plan)
			if p.Name() == "faft-joint" {
				row.JointFeas = !g.SolverInfeasible
				row.JointMsgs = g.SolverMsgs
				row.JointRatio = g.Ratio
			} else {
				row.MajorityMsgs = g.SolverMsgs
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// summarizeLB 是纯函数，单独拆出来是为了能用手算的输入直接测它。
//
// 拆出来的理由就是 **BUG-29**：汇总曾经按 planner 名字聚合，而两个
// FlexiRaft baseline 同名，于是 36 个点被数成 72 个 —— "未达可达目标 72"
// 超过了"可行点 36"，一个指标跑出了自己的定义域。这类错误不会崩溃、
// 不会报错，只会把 baseline 说得更差，所以必须有一条能用手算复核的测试。
//
// 统计口径：只在下界可行的参数点上统计。下界不可行 = 目标本身不可达，
// 那些点上"某方法不可行"不是方法缺陷，计入会得出相反结论。
func summarizeLB(cells []lbCell, names []string, ratios map[string][]float64) ([]lbSummary, int) {
	var out []lbSummary
	for _, name := range names {
		s := lbSummary{Planner: name}
		for _, c := range cells {
			if !c.LBFeasible {
				continue
			}
			s.CellsEvaluated++
			for _, a := range c.Arms {
				if a.Planner != name {
					continue
				}
				switch {
				case !a.Feasible:
					s.MissedTarget++
				case a.AtLowerBound:
					s.AtLB++
				default:
					s.AboveLB++
				}
			}
		}
		s.MedianRatio = median(ratios[name])
		out = append(out, s)
	}

	diff := 0
	for _, c := range cells {
		if !c.LBFeasible {
			continue
		}
		for _, a := range c.Arms {
			if a.Feasible && !a.AtLowerBound {
				diff++
				break
			}
		}
	}
	return out, diff
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	ys := append([]float64(nil), xs...)
	sort.Float64s(ys)
	return ys[len(ys)/2]
}

func printLowerBound(r *lbReport) {
	fmt.Println("=== 可证下界对照：写消息数离理论最优还有多远 ===")
	fmt.Println()
	fmt.Printf("配置：%d 个域，每分片 %d 副本，f_data=%d，f_ctrl=%d，P_base=%.0e\n",
		r.Config.Domains, r.Config.Replicas, r.Config.DataFaults,
		r.Config.ControlFaults, r.Config.PBase)
	fmt.Printf("目标：数据 >= %.4g，控制 >= %.4g\n", r.Config.TargetData, r.Config.TargetCtrl)
	fmt.Println("下界：relaxed lower bound（4 条松弛，见 pkg/faft/lowerbound.go）")
	fmt.Println("符号：★ = 达到下界（消息数最优）  +n = 比下界多 n 条消息  ✗ = 不可行  ∅ = 下界不可行")
	fmt.Println()

	armNames := []string{}
	for _, a := range r.Cells[0].Arms {
		armNames = append(armNames, a.Planner)
	}
	// 表头宽度按名字最长者定，避免列错位。
	w := 0
	for _, n := range armNames {
		if len(n) > w {
			w = len(n)
		}
	}

	fmt.Printf("%s %s %s", pad("参数点", 24), pad("下界(|Q2|,|Q1|)", 15), pad("消息", 6))
	for _, n := range armNames {
		fmt.Printf(" %s", pad(n, w))
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 24+1+15+1+6+(w+1)*len(armNames)))

	for _, c := range r.Cells {
		lbStr, q2Str := "∅", "-"
		if c.LBFeasible {
			lbStr = fmt.Sprintf("(%d,%d)", c.LBQ2, c.LBQ1)
			q2Str = fmt.Sprintf("%d", c.LBMsgs)
		}
		fmt.Printf("%s %s %s",
			pad(fmt.Sprintf("skew=%.1f Q=%.0e k=%d", c.Skew, c.Q, c.K), 24),
			pad(lbStr, 15), pad(q2Str, 6))
		for _, a := range c.Arms {
			fmt.Printf(" %s", pad(lbArmSymbol(a), w))
		}
		fmt.Println()
	}
	fmt.Println()

	fmt.Printf("下界可行 / 不可行的参数点：%d / %d\n", r.LBCellsFeasible, r.LBCellsInfeasible)
	fmt.Printf("其中有区分力的点（存在「可达但未达下界」的方法）：%d / %d\n\n",
		r.DifferentiatingCells, r.LBCellsFeasible)

	fmt.Println("汇总（**只在下界可行的参数点上统计** —— 目标本身不可达的点，" +
		"比较消息数没有意义）：")
	fmt.Printf("%s %s %s %s %s %s\n",
		pad("planner", 20), pad("可行点", 10), pad("达到下界", 10),
		pad("高于下界", 10), pad("未达可达目标", 14), "相对下界中位比")
	fmt.Println(strings.Repeat("-", 88))
	for _, s := range r.Summary {
		fmt.Printf("%s %s %s %s %s %.2f×\n",
			pad(s.Planner, 20), pad(fmt.Sprintf("%d", s.CellsEvaluated), 10),
			pad(fmt.Sprintf("%d", s.AtLB), 10), pad(fmt.Sprintf("%d", s.AboveLB), 10),
			pad(fmt.Sprintf("%d", s.MissedTarget), 14), s.MedianRatio)
	}
	fmt.Println()

	if len(r.NSweep) > 0 {
		fmt.Println("--- 副本数扫描（skew=2.0，Q=1e-3，K=2，目标同上一节）---")
		fmt.Printf("%s %s %s %s %s %s %s\n",
			pad("n", 6), pad("下界|Q2|", 10), pad("下界消息", 10), pad("faft-joint", 12),
			pad("比值", 8), pad("majority", 10), "比值")
		fmt.Println(strings.Repeat("-", 84))
		for _, row := range r.NSweep {
			joint := "✗ 不可行"
			if row.JointFeas {
				joint = fmt.Sprintf("%d", row.JointMsgs)
			}
			majRatio := "-"
			if row.MajorityMsgs > 0 && row.LBMsgs > 0 {
				majRatio = lbRatioStr(float64(row.MajorityMsgs) / float64(row.LBMsgs))
			}
			fmt.Printf("%s %s %s %s %s %s %s\n",
				pad(fmt.Sprintf("%d", row.N), 6),
				pad(fmt.Sprintf("%d", row.LBQ2), 10),
				pad(fmt.Sprintf("%d", row.LBMsgs), 10),
				pad(joint, 12), pad(lbRatioStr(row.JointRatio), 8),
				pad(fmt.Sprintf("%d", row.MajorityMsgs), 10), majRatio)
		}
		fmt.Println()
	}

	fmt.Println("注意事项：")
	for _, n := range r.Notes {
		fmt.Printf("  - %s\n", n)
	}
}

func lbArmSymbol(a lbArm) string {
	switch {
	case !a.Feasible:
		return "✗"
	case a.Q2 == 0:
		return "∅"
	case a.AtLowerBound:
		return fmt.Sprintf("%d★", a.Msgs)
	default:
		return fmt.Sprintf("%d (+%d)", a.Msgs, a.Gap)
	}
}

func lbRatioStr(r float64) string {
	if r <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f×", r)
}

// dispWidth 返回字符串在等宽终端下的显示宽度（全角字符占 2 列）。
//
// fmt 的 %-Ns 按 **rune 数**补齐，所以中文表头会比数据行短一截（每个全角字少 1 列），
// 整张表列错位。这张表要直接进论文，所以按显示宽度对齐。
func dispWidth(s string) int {
	w := 0
	for _, r := range s {
		switch {
		case r >= 0x1100 && r <= 0x115F,
			r == 0x2329, r == 0x232A,
			r >= 0x2E80 && r <= 0xA4CF,
			r >= 0xAC00 && r <= 0xD7A3,
			r >= 0xF900 && r <= 0xFAFF,
			r >= 0xFE30 && r <= 0xFE6F,
			r >= 0xFF00 && r <= 0xFF60,
			r >= 0xFFE0 && r <= 0xFFE6,
			r >= 0x20000 && r <= 0x3FFFD:
			w += 2
		default:
			w++
		}
	}
	return w
}

// pad 按**显示宽度**左对齐补齐到 width 列。
func pad(s string, width int) string {
	if d := dispWidth(s); d < width {
		return s + strings.Repeat(" ", width-d)
	}
	return s
}

func lbNotes(r *lbReport) []string {
	notes := []string{
		"下界是**写消息数**的下界，不是可用性的下界：差距为 0 表示消息数已达理论最优；" +
			"差距大于 0 只说明「未证明最优」，不说明方案差 —— 界本身可能偏松。",
		"四条松弛（见 pkg/faft/lowerbound.go）：① 两条路径分别取上界（忽略耦合）；" +
			"② 忽略域容量与同域堆积；③ 无事件项按最可靠域算；④ 事件项对成员数分拆取最大值。",
		"松弛 ① 是最松的一条：真实系统要求 Q1 与 Q2 **同时**存活，" +
			"松弛后各自单独达标即可，因此下界在控制目标很紧时会明显偏低。",
		"D ≤ 12 时事件项按「命中全部 C(D,k) 组合」**精确求最大**；" +
			"D > 12 时拓扑用轮转采样，下界改用与命中集合无关的解析界，此时会偏松。",
	}
	if r.LBCellsInfeasible > 0 {
		notes = append(notes, fmt.Sprintf(
			"有 %d 个参数点下界不可行：连「最可靠域 + 最理想分拆」都达不到目标，"+
				"这些点上任何方法的不可行都不是方法缺陷。", r.LBCellsInfeasible))
	}
	if r.DifferentiatingCells*2 < r.LBCellsFeasible {
		notes = append(notes, fmt.Sprintf(
			"⚠️ 只有 %d/%d 个可行点有区分力：其余点上所有可行方法都已达到下界，"+
				"说明**下界在这些点上不够紧**（松弛 ① 忽略 Q1/Q2 耦合是最主要的原因）。"+
				"引用「达到下界」时必须同时给出区分力点数，否则会被读成「谁做都一样」。",
			r.DifferentiatingCells, r.LBCellsFeasible))
	}
	return notes
}
