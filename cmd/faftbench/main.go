// cmd/faftbench 计算 FAFT 的收益上界与代价，用于判断方向是否值得投入。
//
// 用法：
//
//	faftbench scale         # 副本数扫描：省多少消息、控制路径容错是否退化
//	faftbench cost          # 副本粒度可用性：弹性 quorum 的真实代价
//	faftbench domain        # 按故障域组织 quorum 能否恢复控制路径可用性
//	faftbench solve         # 故障感知成员选择 vs 按域计数贪心（FAFT 核心增量）
//	faftbench solve-uniform # 对照组：域之间等价时的表现
//	faftbench solve-plan    # 给定 SLA 目标求最小消息数方案
//	faftbench planners      # baseline 对照（majority / FlexiRaft / FAFT / 消融）
//	faftbench flexiraft-sweep # FlexiRaft data-commit quorum 参数扫描
//	faftbench regional      # 区域事件是故障域感知的必要前提（对照实验）
//	faftbench evidence      # 本项目结论的证据等级清单（写作自查用）
//	faftbench n1000         # n=1000 单点详算
//	faftbench curve         # n=1000 的完整权衡曲线
//	faftbench avail         # 独立失效 vs 相关失效的可用性高估量
//	faftbench sens          # **敏感性扫描**：联合求解在故障参数空间的哪些区域占优
//	faftbench ablation      # **消融阶梯**：联合优化 vs 分开优化两次（C1 的直接证据）
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/distributed-kv/kvstore/pkg/faft"
)

func main() {
	mode := "scale"
	sensOut := ""
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if len(os.Args) > 2 {
		sensOut = os.Args[2]
	}

	switch mode {
	case "scale":
		sc := faft.ScaleConfig{Domains: 5, CrossDomainRTTMs: 2.0, DataFaults: 1, ControlFaults: 1}
		fmt.Println("=== FAFT 收益与代价：副本数扫描（5 个故障域，跨域 RTT 2ms）===")
		fmt.Println()
		_, rows := faft.MaxBenefit(sc, []int{3, 5, 7, 9, 11, 21, 51, 101})
		fmt.Print(faft.FormatScaleTable(rows))

	case "cost":
		fmt.Println("=== 弹性 quorum 的真实代价（副本粒度可用性，单副本失效率 p=1e-4）===")
		fmt.Println()
		const p = 1e-4
		fmt.Printf("%-7s %-6s %-6s %-11s %-18s %-18s %s\n",
			"n", "|Q2|", "|Q1|", "写消息/op", "数据路径可用性", "控制路径可用性", "选举需在线副本")
		fmt.Println(strings.Repeat("-", 108))
		for _, n := range []int{5, 9, 21, 101, 1000} {
			m := n/2 + 1
			base := faft.ComputeControlCost(n, m, p)
			fmt.Printf("%-7d %-6d %-6d %-11d %-18.12f %-18.12f %d  (基线多数 quorum)\n",
				n, base.DataQuorum, base.ControlQuorum, 2*base.DataQuorum,
				base.DataAvailability, base.ControlAvailability, base.ReplicasNeededToElect)

			ext := faft.ComputeControlCost(n, 2, p)
			fmt.Printf("%-7s %-6d %-6d %-11d %-18.12f %-18.12f %d  (|Q2|=2 极限)\n",
				"", ext.DataQuorum, ext.ControlQuorum, 2*ext.DataQuorum,
				ext.DataAvailability, ext.ControlAvailability, ext.ReplicasNeededToElect)

			q2 := n / 4
			if q2 < 2 {
				q2 = 2
			}
			mid := faft.ComputeControlCost(n, q2, p)
			fmt.Printf("%-7s %-6d %-6d %-11d %-18.12f %-18.12f %d  (|Q2|=n/4 折中)\n",
				"", mid.DataQuorum, mid.ControlQuorum, 2*mid.DataQuorum,
				mid.DataAvailability, mid.ControlAvailability, mid.ReplicasNeededToElect)
			fmt.Println()
		}

	case "domain":
		fmt.Println("=== 按故障域组织 quorum：控制路径可用性能否恢复 ===")
		fmt.Println()
		fmt.Println("模型：1000 副本摊到 D 个域；域内失效完全相关，域间独立；单域失效率 p_d=1e-3")
		fmt.Println()
		const n = 1000
		const pd = 1e-3
		fmt.Printf("%-8s %-12s %-12s %-12s %-22s %s\n",
			"域数 D", "每域副本", "|Q2|(域)", "|Q1|(域)", "控制路径可用性", "选举需在线域数")
		fmt.Println(strings.Repeat("-", 96))
		for _, D := range []int{3, 5, 7, 9, 15} {
			for _, q2d := range []int{2, 3} {
				q1d := D - q2d + 1
				if q1d > D {
					q1d = D
				}
				avail := faft.AvailabilityBound(D, D-q1d, pd)
				fmt.Printf("%-8d %-12d %-12d %-12d %-22.15f %d\n",
					D, n/D, q2d, q1d, avail, q1d)
			}
			fmt.Println()
		}
		fmt.Println("对照：不按域组织（副本粒度，p=1e-4）时，|Q2|=2 需要 999/1000 副本在线，")
		fmt.Println("控制路径可用性降到 0.995325232148 —— 即约 13 倍的可达性劣化。")

	case "solve":
		fmt.Println("=== 故障感知成员选择 vs 按域计数贪心 ===")
		fmt.Println()
		fmt.Println("关键前提：域之间必须**不等价**。若域等价，「多覆盖一个域」永远最优，")
		fmt.Println("按计数贪心恰好就是最优解，故障感知无从发挥。")
		fmt.Println()
		runSolveComparison(true)

	case "solve-uniform":
		fmt.Println("=== 对照组：域之间等价（失效率相同）===")
		fmt.Println()
		fmt.Println("预期：此时两种策略结果应一致。若一致，说明收益确实来自「域不等价」。")
		fmt.Println()
		runSolveComparison(false)

	case "solve-plan":
		fmt.Println("=== 故障感知求解：给定可用性目标求最小消息数方案 ===")
		fmt.Println()
		runSolvePlan()

	case "regional":
		runRegionalControl()

	case "planners":
		// P0-4：baseline 对照。所有 planner 在同一拓扑、同一故障模型、
		// 同一代价模型下比较，避免拿不同口径的数字对比。
		fmt.Println("=== Baseline 对照：同一故障模型下的 quorum 规划器 ===")
		fmt.Println()
		runPlannerComparison()

	case "ablation":
		// 消融阶梯：把「放置」与「几何」两个自由度的贡献分开。
		//
		// 它只回答一句话：**「联合优化」是否真的优于「分开优化两次」。**
		// 敏感性扫描得到「联合占优 43/90」，但那个对照里 baseline 的放置是
		// 写死的 —— 审稿人会问"是不是给方法多给了一个自由变量"。只有消融能回答。
		fmt.Println("=== 消融阶梯：联合优化 vs 分开优化两次 ===")
		fmt.Println()
		if err := runAblation(ablationOpts{domains: 9, replicas: 5, dataTol: 1, ctrlTol: 1, outPath: sensOut}); err != nil {
			fmt.Fprintln(os.Stderr, "消融失败:", err)
			os.Exit(1)
		}

	case "sens":
		// 敏感性扫描：结论在故障参数空间的哪些区域里成立。
		//
		// 这是「拿不到真实故障数据时」的正确应对 —— 报分界线而不是报单点数字。
		// 分界线本身与具体参数值无关，而且它明确说出了方法的适用边界。
		fmt.Println("=== 敏感性扫描：联合求解在故障参数空间的哪些区域占优 ===")
		fmt.Println()
		if err := runSens(sensOpts{domains: 9, replicas: 5, dataTol: 1, ctrlTol: 1, outPath: sensOut}); err != nil {
			fmt.Fprintln(os.Stderr, "敏感性扫描失败:", err)
			os.Exit(1)
		}

	case "flexiraft-sweep":
		// FlexiRaft 的收益取决于其 data-commit quorum 取值，
		// 因此必须扫描该参数并报告整条曲线，不能只报一个点。
		fmt.Println("=== FlexiRaft data-commit quorum 参数扫描 ===")
		fmt.Println()
		runFlexiRaftSweep()

	case "evidence":
		fmt.Print(faft.FormatEvidenceManifest())

	case "n1000":
		sc := faft.ScaleConfig{
			Replicas: 1000, Domains: 5, CrossDomainRTTMs: 2.0,
			DataFaults: 1, ControlFaults: 1,
		}
		row := faft.AnalyzeScale(sc)
		b, _ := json.MarshalIndent(row, "", "  ")
		fmt.Println("=== n=1000, 5 个故障域 ===")
		fmt.Println(string(b))

	case "curve":
		sc := faft.ScaleConfig{Replicas: 1000, Domains: 5, CrossDomainRTTMs: 2.0}
		fmt.Println("=== n=1000 的完整权衡曲线（按写消息数升序）===")
		fmt.Println()
		fmt.Print(faft.FormatTradeoff(faft.TradeoffCurve(sc)))

	case "avail":
		fmt.Println("=== 独立失效 vs 相关失效：可用性被高估了多少 ===")
		fmt.Println()
		fmt.Printf("%-8s %-10s %-18s %-18s %s\n", "域数", "容错度", "独立模型P[可用]", "相关模型P[可用]", "高估")
		for _, d := range []int{3, 5, 7} {
			for _, tol := range []int{1, 2} {
				if tol >= d {
					continue
				}
				ind := faft.AvailabilityBound(d, tol, 0.001)
				cor := faft.CorrelatedAvailability(d, tol, 0.001, 0.0001, 2)
				fmt.Printf("%-8d %-10d %-18.12f %-18.12f %.8f\n", d, tol, ind, cor, ind-cor)
			}
		}

	default:
		fmt.Fprintf(os.Stderr,
			"未知模式 %q\n可用: scale cost domain solve solve-uniform solve-plan regional n1000 curve avail\n", mode)
		os.Exit(2)
	}
}

// skewedTopology 构造逐域失效率不等、且含区域事件的拓扑。
//
// 两个要素缺一不可：
//   - 逐域失效率不等     → 让"选哪个域"有意义
//   - 区域事件           → 让"跨域展开"优于"堆叠"
//
// 若只有前者而无后者，把全部副本堆在最稳的域在数学上就是最优，
// 故障感知无从体现（见 buildRegionalFreeTopology 的对照）。
func skewedTopology() *faft.Topology {
	t := faft.NewUniformTopology(5, 2.0)
	probs := []float64{1e-5, 1e-4, 1e-3, 5e-3, 1e-2}
	for i, p := range probs {
		t.Domains[i].FailProb = p
	}
	// 区域事件：1e-3 的概率一次打掉 3 个域（下标最小的三个）。
	t.RegionalEventProb = 1e-3
	t.RegionalEventDomains = 3
	return t
}

// regionalFreeTopology 与 skewedTopology 相同的逐域失效率，但**无区域事件**。
//
// 用途：对照实验。预期此时"按可用性贪心"会退化为"把副本堆在最稳的域"，
// 反而**劣于**跨域展开的按计数贪心 —— 说明区域事件是故障域感知的必要前提。
func regionalFreeTopology() *faft.Topology {
	t := skewedTopology()
	t.RegionalEventProb = 0
	t.RegionalEventDomains = 0
	return t
}

// uniformTopology 构造域之间完全等价的拓扑（对照组）。
func uniformTopology() *faft.Topology {
	t := faft.NewUniformTopology(5, 2.0)
	for i := range t.Domains {
		t.Domains[i].FailProb = 1e-3
	}
	t.RegionalEventProb = 1e-3
	t.RegionalEventDomains = 3
	return t
}

// runSolveComparison 打印两种成员选择策略的对比。
func runSolveComparison(skewed bool) {
	const n = 21
	topo := skewedTopology()
	label := "域失效率不等（1e-5~1e-2）+ 区域事件（1e-3 打掉 3 域）"
	if !skewed {
		topo = uniformTopology()
		label = "域等价（均 1e-3）+ 区域事件（1e-3 打掉 3 域）"
	}

	placement := make([]int, n)
	for i := range placement {
		placement[i] = i % len(topo.Domains)
	}

	fmt.Printf("拓扑：%s；n=%d 副本\n\n", label, n)
	fmt.Printf("%-7s %-18s %-20s %-14s %-9s\n",
		"|Q2|", "按域计数贪心", "按可用性贪心", "绝对提升", "倍数")
	fmt.Println(strings.Repeat("-", 74))

	av := faft.AvailabilityModel{Model: faft.IndependentFailure{P: 0}, Topology: topo}
	cmps := faft.ComparePlacementStrategies(topo, faft.Config{
		Replicas: n, Placement: placement,
	}, av, n)

	maxGain := 0.0
	for _, c := range cmps {
		gainX := "-"
		if c.GainX == 1 {
			gainX = "1.000x"
		} else if c.GainX > 1 {
			gainX = fmt.Sprintf("%.3fx", c.GainX)
		}
		mark := ""
		if c.Gain > 1e-15 {
			mark = "  <-- 提升"
			if c.Gain > maxGain {
				maxGain = c.Gain
			}
		}
		fmt.Printf("%-7d %-18.12f %-20.12f %-14.3e %-9s%s\n",
			c.Q2Size, c.CountBasedAvailability, c.AvailabilityBased, c.Gain, gainX, mark)
	}
	fmt.Println()
	fmt.Printf("最大绝对提升 = %.3e\n", maxGain)

	if !skewed {
		if maxGain < 1e-15 {
			fmt.Println("结论：域等价时两种策略结果一致 —— 收益确实来自「域不等价」。")
		} else {
			fmt.Printf("注意：域等价时仍有 %.3e 差异。\n", maxGain)
		}
	} else if maxGain < 1e-15 {
		fmt.Println("警告：域不等价却没有提升，需检查 pickMaxAvailability 的排序准则。")
	} else {
		fmt.Println("结论：域不等价时，按可用性贪心优于按域计数贪心。")
	}
}

// runRegionalControl 对照实验：区域事件是否存在，决定策略优劣方向。
func runRegionalControl() {
	const n = 21
	fmt.Println("=== 对照：区域事件是故障域感知的必要前提 ===")
	fmt.Println()
	fmt.Println("两组拓扑的逐域失效率完全相同（1e-5 ~ 1e-2，相差 1000 倍），")
	fmt.Println("唯一区别是有无『区域事件』（一次打掉多个域的相关故障）。")
	fmt.Println()

	for _, scen := range []struct {
		name string
		topo *faft.Topology
	}{
		{"有区域事件（1e-3 打掉 3 域）", skewedTopology()},
		{"无区域事件（纯逐域独立失效）", regionalFreeTopology()},
	} {
		placement := make([]int, n)
		for i := range placement {
			placement[i] = i % len(scen.topo.Domains)
		}
		av := faft.AvailabilityModel{Model: faft.IndependentFailure{P: 0}, Topology: scen.topo}
		cmps := faft.ComparePlacementStrategies(scen.topo, faft.Config{
			Replicas: n, Placement: placement,
		}, av, 5)

		fmt.Printf("--- %s ---\n", scen.name)
		fmt.Printf("%-6s %-18s %-20s %-14s\n", "|Q2|", "按域计数", "按可用性", "差异")
		for _, c := range cmps {
			fmt.Printf("%-6d %-18.12f %-20.12f %+-14.3e\n",
				c.Q2Size, c.CountBasedAvailability, c.AvailabilityBased, c.Gain)
		}
		worst := 0.0
		for _, c := range cmps {
			if c.Gain < worst {
				worst = c.Gain
			}
		}
		if worst < -1e-12 {
			fmt.Printf("注意：按可用性贪心在此场景下**劣于**按计数贪心（最差 %.3e）。\n", worst)
			fmt.Println("      这说明缺少区域事件时，堆叠副本在模型内是最优的 ——")
			fmt.Println("      模型不惩罚堆叠，故障域感知自然无从体现。")
		} else {
			fmt.Println("按可用性贪心不低于按计数贪心。")
		}
		fmt.Println()
	}
}

// runSolvePlan 演示给定 SLA 目标时的完整求解。
func runSolvePlan() {
	topo := skewedTopology()
	targets := faft.AvailabilityTargets{Data: 0.999, Control: 0.9999}

	fmt.Printf("可用性目标：数据路径 >= %.4g，控制路径 >= %.4g\n", targets.Data, targets.Control)
	fmt.Printf("拓扑：5 个域，逐域失效率 1e-5 ~ 1e-2\n\n")
	fmt.Printf("%-6s %-12s %-8s %-8s %-18s %-18s %-8s\n",
		"n", "写消息/op", "|Q2|", "|Q1|", "数据可用性", "控制可用性", "达标")
	fmt.Println(strings.Repeat("-", 92))

	for _, n := range []int{5, 7, 9, 11, 15, 21} {
		placement := make([]int, n)
		for i := range placement {
			placement[i] = i % len(topo.Domains)
		}
		av := faft.AvailabilityModel{Model: faft.IndependentFailure{P: 0}, Topology: topo}
		fp := faft.SolveWithFailures(topo, faft.Config{
			Replicas: n, Placement: placement,
		}, av, targets)

		q1, q2 := fp.Quorum.Size()
		ok := "否"
		if fp.Meets {
			ok = "是"
		}
		fmt.Printf("%-6d %-12d %-8d %-8d %-18.6g %-18.6g %-8s\n",
			n, fp.WriteMsgsPerOp, q2, q1,
			fp.DataAvailability, fp.ControlAvailability, ok)
	}
	fmt.Println()
	fmt.Println("说明：|Q1| 满足 |Q1|+|Q2| > n（Flexible Paxos, OPODIS 2016 §4.2）；")
	fmt.Println("求解目标是在两条路径可用性达标的前提下最小化写消息数 2|Q2|。")
}

// buildScenario 构造对照用的场景：拓扑 + 放置 + 故障模型。
func buildScenario(n int) (*faft.Topology, faft.Placement, faft.AvailabilityModel) {
	topo := skewedTopology() // 5 域，失效率 1e-5~1e-2，含区域事件
	placement := make(faft.Placement, n)
	for i := range placement {
		placement[i] = i % len(topo.Domains)
	}
	av := faft.AvailabilityModel{
		Model:    faft.IndependentFailure{P: 0},
		Topology: topo,
	}
	return topo, placement, av
}

// runPlannerComparison 在同一口径下对照五个 planner。
//
// ⚠️ 目标必须**可达**，否则所有 planner 都显示"不可行"，对照失去意义。
// 本拓扑含区域事件 q=1e-3，它把最坏情形下的可用性钉在 1-q = 0.999，
// 因此目标必须低于它（这里取 0.998 / 0.9989）。
// 初版取了 0.999 / 0.9999，结果每个 planner 都不可行 —— 那是场景设计错误，
// 不是 planner 的问题。
func runPlannerComparison() {
	targets := faft.AvailabilityTargets{Data: 0.998, Control: 0.9989}

	fmt.Println("拓扑：5 个域，逐域失效率 1e-5 ~ 1e-2（相差 1000 倍），区域事件 q=1e-3 打掉 3 域")
	fmt.Printf("可用性目标：数据 >= %.4g，控制 >= %.4g（低于 1-q=0.999，保证可达）\n",
		targets.Data, targets.Control)
	fmt.Println("评估判据：先看可行性（两条路径都达标），再比写消息数")
	fmt.Println()

	planners := faft.DefaultBaselines(targets)

	for _, n := range []int{5, 9, 11, 21, 51} {
		topo, placement, av := buildScenario(n)
		fmt.Printf("--- n=%d 副本 ---\n", n)

		cmps := faft.ComparePlanners(topo, faft.Config{Replicas: n, Placement: placement}, av, planners...)
		fmt.Print(faft.FormatPlannerComparison(cmps))

		if best, ok := faft.BestFeasible(cmps); ok {
			fmt.Printf("最优可行方案：%s（写消息 %d，相对多数 quorum 降 %.2fx）\n",
				best.Planner, best.WriteMsgsPerOp, best.MsgReductionVsMajority)
		} else {
			fmt.Println("**没有任何可行方案** —— 该规模下目标不可达")
		}
		fmt.Println()
	}

	fmt.Println("planner 说明：")
	fmt.Println("  majority          Raft / Multi-Paxos：Q1 = Q2 = ⌊n/2⌋+1")
	fmt.Println("  tikv-pd           TiKV PD：quorum 几何无自由度（同多数）；")
	fmt.Println("                    其优化在跨分片 placement 维度，本框架未度量")
	fmt.Println("  flexiraft         FlexiRaft (CIDR'23) static，data quorum = max(2, n/4)")
	fmt.Println("  flexiraft         （DataQuorum=2 时）data quorum 取最小")
	fmt.Println("  orca              Orca (PVLDB'26)：commit quorum 固定 k+1，Q1 按 FPaxos 推导")
	fmt.Println("  faft              本项目：可用性约束下最小化写消息，成员按边际可用性贪心")
	fmt.Println("  faft-countselect  消融：FAFT 的 quorum 大小 + 按域计数选成员")
	fmt.Println()
	fmt.Println("⚠️ 范围披露（论文中必须逐条写明）：")
	fmt.Println("  - FlexiRaft 仅 static 模式；dynamic 模式未实现")
	fmt.Println("  - Orca 仅结构性部分；动态 quorum 的完整运行时机制未实现，")
	fmt.Println("    且**未获得论文全文**，参数语义可能与原文有差异")
	fmt.Println("  - TiKV PD 无学术发表，依据官方文档与源码；其收益在跨分片维度，本框架测不到")
}

// runFlexiRaftSweep 扫描 FlexiRaft 的 data-commit quorum 参数。
func runFlexiRaftSweep() {
	// FlexiRaft 的收益完全取决于 data-commit quorum 取值，
	// 只报一个点是误导性的 —— 必须给出整条曲线。
	const n = 21
	topo, placement, av := buildScenario(n)
	cfg := faft.Config{Replicas: n, Placement: placement}

	fmt.Printf("n=%d，5 个域，目标：数据 >= %.4g，控制 >= %.4g\n\n",
		n, faft.DefaultTargets().Data, faft.DefaultTargets().Control)
	fmt.Printf("%-14s %-6s %-6s %-10s %-16s %-16s %-8s\n",
		"data quorum", "|Q2|", "|Q1|", "写消息", "数据可用性", "控制可用性", "可行")
	fmt.Println(strings.Repeat("-", 88))

	for _, q2 := range []int{1, 2, 3, 5, 8, 11} {
		p := faft.FlexiRaftPlanner{DataQuorum: q2}
		cmps := faft.ComparePlanners(topo, cfg, av, p)
		if len(cmps) == 0 {
			continue
		}
		c := cmps[0]
		feas := "否"
		if c.Feasible {
			feas = "是"
		}
		mark := ""
		if !c.MeetsControl {
			mark = "  ← 控制路径不达标"
		}
		fmt.Printf("%-14d %-6d %-6d %-10d %-16.6g %-16.6g %-8s%s\n",
			q2, c.Q2Size, c.Q1Size, c.WriteMsgsPerOp,
			c.DataAvailability, c.ControlAvailability, feas, mark)
	}

	fmt.Println()
	fmt.Println("对照：多数 quorum（Raft）")
	base := faft.ComparePlanners(topo, cfg, av, faft.MajorityPlanner{})
	if len(base) > 0 {
		c := base[0]
		feas := "否"
		if c.Feasible {
			feas = "是"
		}
		fmt.Printf("%-14s %-6d %-6d %-10d %-16.6g %-16.6g %-8s\n",
			"—", c.Q2Size, c.Q1Size, c.WriteMsgsPerOp,
			c.DataAvailability, c.ControlAvailability, feas)
	}
	fmt.Println()
	fmt.Println("观察要点：data quorum 越小，写消息越少，但控制路径可用性下降越快。")
	fmt.Println("          这正是 Flexible Paxos 的 |Q1|+|Q2| > N 约束在起作用。")
}
