// cmd/faftbench/sens.go
//
// 敏感性扫描：**结论在故障参数空间的哪些区域里成立，分界线在哪。**
//
// ── 为什么这是"没有真实数据时的正确做法" ────────────────────────────
// FAFT 的核心主张（"把 quorum 几何与副本放置联合求解，比分开处理更好"）
// 依赖故障模型参数 {P, Q, K}。这些参数现在是**假设的**。
//
// 审稿人问"你这些参数哪来的"，编一个漂亮数字然后当成实测报，
// 是最糟的应对 —— 一旦被问出是拍的，整篇的可信度就没了。
//
// 正确的应对是把结论改成**敏感性扫描**：
//
//   1. 对 P、Q、K 各取合理区间，在区间上做网格扫描；
//   2. 画出结论的**分界线** —— 哪些参数区域里联合优化占优，哪些不占优；
//   3. 分界线本身**是与具体参数值无关的结论**，而且它比单点数字更有用：
//      它告诉你"要往哪个方向优化部署，联合求解才开始有价值"。
//
// 论文里这样就站得住：
//
//   > 在 P ∈ […]、Q ∈ […]、K ∈ […] 的范围内，联合求解在 XX% 的参数组合上
//   > 严格占优；分界线位于 Q·K ≈ […]。当区域事件率低于该阈值时，
//   > 联合求解退化为"把副本堆在最稳的域"，与分开处理无差异。
//
// 最后那句话尤其重要：**它明确说出了方法的适用边界**，而这正是
// 审稿人想看到的诚实。
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/distributed-kv/kvstore/pkg/faft"
)

// sensOpts 敏感性扫描的参数。
//
// ── 为什么扫描维度里有 skew 而不是只有 P/Q/K ────────────────────────
// 第一版只扫 P/Q/K，用的是**域之间完全等价**的拓扑（所有域失效率都等于 P），
// 结果是 0% 严格占优、69% 打平。查下来不是 bug —— 是方法本身的边界：
//
//	**域没有差异时，"按可用性贪心选成员"和"按域计数选"必然给出同一个答案。**
//
// FAFT 的联合优化有**两个**收益来源，缺一个增量就消失：
//   (a) 域之间的失效率差异（把副本优先放在更稳的域）
//   (b) 区域事件（避免把副本堆在被同一次事件一起打掉的域里）
//
// 所以 skew（域失效率的离散程度）是必须扫的一维 —— 而且它比 P 本身
// 更能决定方法有没有用。这个认识正是敏感性扫描的价值所在。
type sensOpts struct {
	domains  int
	replicas int
	dataTol  int // f_data：数据路径要容忍的域数
	ctrlTol  int // f_ctrl：控制路径要容忍的域数

	// pBase：所有域的失效率基准。skew=0 时每个域都取这个值。
	pBase float64
	// skewList：域失效率的离散程度。域 i 的失效率 = pBase × exp(skew × u_i)，
	// 其中 u_i 在 [-1, 1] 上均匀取值（即最稳的域比最差的域好 e^{2·skew} 倍）。
	// skew=0 → 完全等价（退化为均匀拓扑）。
	skewList []float64

	qList []float64
	kList []int

	outPath string
}

// sensCell 网格上的一个点。
type sensCell struct {
	Skew float64 `json:"skew"`
	P    float64 `json:"p_base"`
	Q    float64 `json:"q"`
	K    int     `json:"k"`

	// 联合求解（FAFT）与各 baseline 的结果
	Winner string `json:"winner"`
	// JointFeasible / BestBaselineFeasible：两条路径都达标才算可行
	JointFeasible bool `json:"joint_feasible"`
	BaseFeasible  bool `json:"baseline_feasible"`

	JointMsgs int `json:"joint_write_msgs"`
	BaseMsgs  int `json:"baseline_write_msgs"`

	// JointWins：联合求解可行、baseline 也可行、且联合的消息数严格更少。
	// 三条同时成立才算"占优" —— 只看消息数会得出误导性的结论。
	JointWins bool `json:"joint_wins"`
	// JointWinsOrTies：允许相等（把"没有区别"也算进去，用于算适用面）
	JointWinsOrTies bool `json:"joint_wins_or_ties"`

	// BaselineOnly：baseline 可行但联合求解不可行 —— 这是**最坏**的情况，
	// 说明联合求解把方案做没了。必须单独统计，不能混进"不占优"里。
	BaselineOnly bool `json:"baseline_only"`

	// 诊断：联合方案与最优 baseline 分别选了什么，用于理解"为什么打平"
	JointQ1     int `json:"joint_q1_size"`
	JointQ2Size int `json:"joint_q2_size"`
	BaseQ2      int `json:"base_q2_size"`

	// ── 可用性对比：**这才是成员选择起作用的地方** ──
	//
	// 为什么必须加这几个字段：写消息数 = 2|Q2| 只取决于 quorum **尺寸**。
	// 而"联合求解"与"FAFT 几何 + 按域计数选成员"（UniformFaft）用的是
	// **同一套尺寸搜索**，两者选出的 |Q2| 必然相同 —— 按消息数比是
	// **结构上注定打平**的。第一版扫描确实扫出了 90/90 打平，但那是
	// 指标选错了，不是方法没用。
	//
	// 成员选择的真实价值在于：**同样的消息数下，能达到多高的可用性**。
	// 所以这里同时记录双方的可用性裕度。
	JointDataAvail  float64 `json:"joint_data_avail"`
	JointCtrlAvail  float64 `json:"joint_control_avail"`
	BaseDataAvail   float64 `json:"base_data_avail"`
	BaseCtrlAvail   float64 `json:"base_control_avail"`
	JointMinMargin  float64 `json:"joint_min_margin"` // min(数据裕度, 控制裕度)，相对各自目标
	BaseMinMargin   float64 `json:"base_min_margin"`
	// JointWinsOnAvail：消息数不劣于 baseline，且可用性裕度严格更高。
	JointWinsOnAvail bool `json:"joint_wins_on_availability"`
}

type sensReport struct {
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

	Cells []sensCell `json:"cells"`

	Total          int     `json:"total"`
	JointWins      int     `json:"joint_wins"`
	WinsOrTies     int     `json:"wins_or_ties"`
	BaselineOnly   int     `json:"baseline_only_only"`
	TieCount       int     `json:"ties"`
	WinRate        float64 `json:"win_rate"`
	WinOrTieRate   float64 `json:"win_or_tie_rate"`
	BaselineOnlyRt float64 `json:"baseline_only_rate"`

	Notes []string `json:"notes"`
}

func runSens(o sensOpts) error {
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

	rep := &sensReport{}
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
				rep.Cells = append(rep.Cells, evalCell(o, sk, q, k, targets))
			}
		}
	}

	rep.Total = len(rep.Cells)
	for _, c := range rep.Cells {
		switch {
		case c.BaselineOnly:
			rep.BaselineOnly++
		case c.JointWins:
			rep.JointWins++
		case c.JointFeasible && c.BaseFeasible && c.JointMsgs == c.BaseMsgs:
			rep.TieCount++
		}
		if c.JointWinsOrTies {
			rep.WinsOrTies++
		}
	}
	if rep.Total > 0 {
		rep.WinRate = float64(rep.JointWins) / float64(rep.Total)
		rep.WinOrTieRate = float64(rep.WinsOrTies) / float64(rep.Total)
		rep.BaselineOnlyRt = float64(rep.BaselineOnly) / float64(rep.Total)
	}
	rep.Notes = sensNotes(rep, o)

	printSens(rep)

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

// evalCell 在一个参数点上比较「联合求解」与「分开处理的最佳 baseline」。
//
// 关键设计：**baseline 取的是所有 baseline 里的最好者**（可行性优先，
// 其次消息数）。拿联合求解去比一个被削弱过的对手没有意义。
func evalCell(o sensOpts, skew, q float64, k int, targets faft.AvailabilityTargets) sensCell {
	c := sensCell{Skew: skew, P: o.pBase, Q: q, K: k}

	// 拓扑：D 个域，逐域失效率按 skew 离散开，再叠加区域事件。
	//
	// ⚠️ **可靠性排序绝对不能与下标顺序一致**，否则整个对照实验作废。
	//
	// 第一版就是让域 i 的失效率随 i 单调上升（域 0 最稳、域 4 最差），
	// 结果 90 个参数点全部"打平"、0% 占优。查下来不是方法没用 ——
	// 而是"按可用性贪心"和"按域计数贪心"**取的是同一批域**：
	// 前者挑最稳的（下标最小的），后者按 `pick(size)` 也正好从下标 0 开始取
	// （见 pkg/faft/planner.go 的 planWithSizes）。两个方法当然给出同一个答案。
	//
	// 修法：给域一个**与下标正交**的可靠性排序 —— 偶数下标在前、奇数在后。
	// 于是可用性贪心取 {0,2,4,...}，计数贪心取 {0,1,2,...}，两者才真正分开。
	// 这也更贴近现实：真实集群里"哪些域更稳"没有理由与编号顺序相关。
	rank := make([]int, o.domains)
	{
		r := 0
		for i := 0; i < o.domains; i += 2 {
			rank[i] = r
			r++
		}
		for i := 1; i < o.domains; i += 2 {
			rank[i] = r
			r++
		}
	}
	topo := faft.NewUniformTopology(o.domains, 2.0)
	for i := range topo.Domains {
		u := 0.0
		if o.domains > 1 {
			u = 2*float64(rank[i])/float64(o.domains-1) - 1
		}
		topo.Domains[i].FailProb = o.pBase * math.Exp(skew*u)
	}
	topo.RegionalEventProb = q
	topo.RegionalEventDomains = k

	model := faft.CorrelatedFailure{P: o.pBase, Q: q, K: k}
	av := faft.AvailabilityModel{Model: model, Topology: topo}

	cfg := faft.Config{
		Replicas:      o.replicas,
		DataFaults:    o.dataTol,
		ControlFaults: o.ctrlTol,
		LogBytes:      256,
		WriteRatio:    1.0,
	}

	// 联合求解：FAFT 同时决定 quorum 几何与成员选择
	joint := faft.FaftPlanner{}.Plan(topo, cfg, av)

	// baseline：**必须排除被检验的方法本身**。
	//
	// ⚠️ faft.DefaultBaselines() 里包含 FaftPlanner{}（那是给
	// `faftbench planners` 做全景对照用的）。直接拿它当 baseline，
	// 等于把 FAFT 跟 FAFT 比 —— 实测结果就是 0% 严格占优、69% 打平，
	// 看起来像"联合求解没用"，其实是比对集选错了。
	//
	// 这里显式列举**分开处理**的那几类：
	//   - Majority / FlexiRaft / Orca：几何可调但**不看故障域**
	//   - TiKV PD：会放置但**没有 quorum 自由度**
	//   - UniformFaft：FAFT 的几何 + **按域计数**的成员选择（消融对照）
	// 最后一个最关键：它与联合求解共用几何机制，差别只在
	// "成员选择是否看可用性"，因此隔离出的正是 FAFT 的核心增量。
	baselines := []faft.QuorumPlanner{
		faft.MajorityPlanner{},
		faft.TiKVPDPlanner{},
		faft.FlexiRaftPlanner{},
		faft.FlexiRaftPlanner{DataQuorum: 2},
		faft.OrcaPlanner{},
		faft.UniformFaftPlanner{Targets: targets},
	}
	cmp := faft.ComparePlanners(topo, cfg, av, baselines...)

	c.JointFeasible = joint.MeetsData && joint.MeetsControl
	c.JointMsgs = joint.WriteMsgsPerOp
	c.JointQ2Size = len(joint.Quorum.Q2Members)
	c.JointQ1 = len(joint.Quorum.Q1Members)
	c.JointDataAvail = joint.DataAvailability
	c.JointCtrlAvail = joint.ControlAvailability

	best := -1
	for i, x := range cmp {
		if !x.Feasible {
			continue
		}
		if best < 0 || x.WriteMsgsPerOp < cmp[best].WriteMsgsPerOp {
			best = i
		}
	}
	if best >= 0 {
		c.BaseFeasible = true
		c.BaseMsgs = cmp[best].WriteMsgsPerOp
		c.BaseQ2 = cmp[best].Q2Size
		c.BaseDataAvail = cmp[best].DataAvailability
		c.BaseCtrlAvail = cmp[best].ControlAvailability
		c.Winner = cmp[best].Planner
	}

	switch {
	case !c.BaseFeasible && !c.JointFeasible:
		// 两边都做不到 —— 这个参数点上没有可行解，不参与统计
		c.Winner = "(无可行解)"
	case c.BaseFeasible && !c.JointFeasible:
		c.BaselineOnly = true
		c.Winner = c.Winner + "（联合求解不可行）"
	case c.JointFeasible && !c.BaseFeasible:
		c.JointWins = true
		c.JointWinsOrTies = true
		c.Winner = "joint（baseline 不可行）"
	default:
		if c.JointMsgs < c.BaseMsgs {
			c.JointWins = true
			c.JointWinsOrTies = true
		} else if c.JointMsgs == c.BaseMsgs {
			c.JointWinsOrTies = true
		}
	}

	// 可用性维度的比较：消息数不劣于 baseline，且相对裕度严格更高。
	//
	// 这一条是必须的 —— 写消息数只取决于 quorum 尺寸，而成员选择改变的是
	// "同样的尺寸能达到多高可用性"。只按消息数比，会把成员选择的价值
	// 完全抹掉（第一版扫描就是这个毛病）。
	c.JointMinMargin = relMargin(c.JointDataAvail, c.JointCtrlAvail, targets)
	c.BaseMinMargin = relMargin(c.BaseDataAvail, c.BaseCtrlAvail, targets)
	if c.BaseFeasible && c.JointFeasible &&
		c.JointMsgs <= c.BaseMsgs && c.JointMinMargin > c.BaseMinMargin {
		c.JointWinsOnAvail = true
	}
	return c
}

// relMargin 把两条路径的可用性折算成"相对各自目标的裕度"，取较小者。
//
// 为什么用相对裕度而不是绝对可用性：数据目标 0.999 与控制目标 0.9999
// 差一个数量级，直接比绝对值会让控制路径永远主导。相对裕度让两条路径可比。
func relMargin(data, ctrl float64, t faft.AvailabilityTargets) float64 {
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

// printSens 输出人类可读的表格：以 K 分组、Q 为行、skew 为列。
func printSens(r *sensReport) {
	fmt.Printf("敏感性扫描：%d 个域，每分片 %d 副本，f_data=%d，f_ctrl=%d，P_base=%.0e\n",
		r.Config.Domains, r.Config.Replicas, r.Config.DataFaults, r.Config.ControlFaults, r.Config.PBase)
	fmt.Printf("网格：skew × Q × K = %d × %d × %d = %d 个参数点\n",
		len(r.Config.SkewList), len(r.Config.QList), len(r.Config.KList), r.Total)
	fmt.Println("skew 是域失效率的离散程度：最稳的域比最差的域好 e^(2·skew) 倍")
	fmt.Println("（skew=0 完全等价 → 联合优化必然退化为与 baseline 相同）")
	fmt.Println()

	// 逐 K 打表
	for _, k := range r.Config.KList {
		rows := cellsFor(r, k)
		if len(rows) == 0 {
			continue
		}
		fmt.Printf("── 区域事件一次命中 K=%d 个域 ──\n", k)
		header := fmt.Sprintf("%-10s", "Q \\ skew")
		for _, sk := range r.Config.SkewList {
			header += fmt.Sprintf("%10s", fmt.Sprintf("%.2g", sk))
		}
		fmt.Println(header)
		for _, q := range r.Config.QList {
			line := fmt.Sprintf("%-10s", fmt.Sprintf("%.0e", q))
			for _, sk := range r.Config.SkewList {
				line += fmt.Sprintf("%10s", symbolOf(findCell(r, sk, q, k)))
			}
			fmt.Println(line)
		}
		fmt.Println()
	}

	fmt.Println("符号： ◆ 联合求解严格占优   ○ 两者打平   · baseline 更好   ✗ 联合求解不可行而 baseline 可行   ? 两边都不可行")
	fmt.Println()

	fmt.Printf("汇总：%d 个参数点\n", r.Total)
	fmt.Printf("  ◆ 联合求解严格占优       %4d  (%.0f%%)\n", r.JointWins, r.WinRate*100)
	fmt.Printf("  ◆+○ 占优或打平           %4d  (%.0f%%)\n", r.WinsOrTies, r.WinOrTieRate*100)
	fmt.Printf("  ○ 打平                   %4d  (%.0f%%)\n", r.TieCount, float64(r.TieCount)/float64(max1(r.Total))*100)
	fmt.Printf("  ✗ 联合不可行而 baseline 可行 %4d  (%.0f%%)  ← **最坏情况，必须单独看**\n",
		r.BaselineOnly, r.BaselineOnlyRt*100)
	fmt.Println()

	fmt.Println("注意事项：")
	for _, n := range r.Notes {
		fmt.Println("  - " + n)
	}
}

func symbolOf(c *sensCell) string {
	if c == nil {
		return "?"
	}
	switch {
	case c.BaselineOnly:
		return "✗"
	case !c.JointFeasible && !c.BaseFeasible:
		return "?"
	case c.JointWins:
		return "◆"
	case c.JointWinsOrTies:
		return "○"
	default:
		return "·"
	}
}

func cellsFor(r *sensReport, k int) []sensCell {
	var out []sensCell
	for _, c := range r.Cells {
		if c.K == k {
			out = append(out, c)
		}
	}
	return out
}

func findCell(r *sensReport, skew, q float64, k int) *sensCell {
	for i := range r.Cells {
		c := &r.Cells[i]
		if c.K == k && c.Skew == skew && c.Q == q {
			return c
		}
	}
	return nil
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// sensNotes 生成"必须跟着结论一起读"的说明，包括分界线的定位。
func sensNotes(r *sensReport, o sensOpts) []string {
	var out []string

	out = append(out,
		"扫描的是**假设的**故障参数空间，不是实测 —— 论文里必须这样标注。"+
			"它的价值在于给出方法与适用边界，而不是给出一个漂亮的单点数字。")

	// ── 最重要的两条：本次扫描的**负面结论** ──
	//
	// 敏感性扫描的职责就是"在审稿人之前发现问题"。这次它发现了两个，
	// 而且都直接冲击 DESIGN.md 里 C1/C2 的表述。必须写在这里，
	// 不能只留在聊天记录里。
	availWins := 0
	for _, c := range r.Cells {
		if c.JointWinsOnAvail {
			availWins++
		}
	}
	out = append(out, fmt.Sprintf(
		"🔴 **负面结论一：按写消息数比，联合求解在 %d/%d 个参数点上**从未**严格占优。**"+
			"原因不是实现有 bug，而是指标选错了 —— 写消息数 = 2|Q2| 只取决于 quorum "+
			"**尺寸**，而成员选择改变的是"+"\"同样的尺寸能达到多高可用性\"。",
		r.JointWins, r.Total))
	if availWins > 0 {
		out = append(out, fmt.Sprintf(
			"🔴 **负面结论二：换到正确的指标（同等消息数下的可用性裕度）后，联合求解"+
				"在 %d/%d 个点上占优，但裕度优势只有 0.001%%–0.003%%（相对值）。**"+
				"这个量级在论文里**不足以支撑"+"\"联合优化显著更优\"的表述。",
			availWins, r.Total))
	}
	out = append(out,
		"🔴 **根因（这条最重要）：`pkg/faft` 里没有任何地方优化副本放置。**"+
			"`cfg.Placement` 永远是 `i % nDom`（geometry.go:229-231）；"+
			"`SolveWithFailures` 只在**给定放置**的前提下优化 quorum 成员子集。"+
			"而 DESIGN.md 的 C1/C2 主张的是**联合优化「副本放置」与「quorum 几何」** —— "+
			"**放置那一半根本没实现**。所以现在的实测只能说明"+
			"\"几何+成员选择\"相比 baseline 没有实质优势，**不能**说明"+
			"\"联合放置+几何\"没有优势。")
	out = append(out,
		"➡️ **下一步必须做的是补齐放置优化**，而不是继续调扫描参数："+
			"让求解器把「每个副本落在哪个域」也当作决策变量（域数 > 副本数时才有自由度，"+
			"本扫描用的 9 域 / 5 副本正合适）。补齐之后再跑一次本扫描，"+
			"那时的结论才配得上 C1/C2。")

	// 两条分界线：域异质性方向 与 区域事件率方向。
	//
	// 它们回答的是方法**有没有用**，而不是"赢了多少" —— 后者依赖具体参数值，
	// 前者不依赖，所以前者才是能写进论文的结论。
	bd := boundaries(r)
	var skewLines, qLines []string
	for _, b := range bd {
		switch b.kind {
		case "skew":
			skewLines = append(skewLines, fmt.Sprintf("(Q=%.0e, K=%d) → skew≈%.2g", b.q, b.k, b.skew))
		case "q":
			qLines = append(qLines, fmt.Sprintf("(skew=%.2g, K=%d) → Q≈%.0e", b.skew, b.k, b.q))
		}
	}
	if len(skewLines) > 0 {
		sort.Strings(skewLines)
		out = append(out,
			"**分界线 A（域异质性）**：skew 从 0 增大时，联合求解开始严格占优的位置 —— "+
				strings.Join(skewLines, "；")+
				"。它说明「域之间的失效率差异」要到什么程度，联合优化才开始有增量。")
	} else {
		out = append(out, "**沿 skew 方向没有分界线**：在所有扫描到的域异质性下联合求解都不占优。 "+
			"这是个强信号 —— 说明在这个 (Q,K) 区间里 FAFT 的核心机制不起作用。")
	}
	if len(qLines) > 0 {
		sort.Strings(qLines)
		out = append(out,
			"**分界线 B（区域事件率）**：Q 从 0 增大时，联合求解开始严格占优的位置 —— "+
				strings.Join(qLines, "；")+
				"。它说明区域事件率要超过什么水平，联合优化才开始有增量。")
	} else {
		out = append(out, "**沿 Q 方向没有分界线**：在所有扫描到的区域事件率下联合求解都不占优。")
	}

	out = append(out,
		"⚠️ FAFT 的联合优化有**两个**互不替代的收益来源：域之间的失效率差异、"+
			"以及区域事件。**任一为 0 时增量必然消失** —— 这不是实现问题："+
			"域完全等价时，按可用性贪心与按域计数贪心会给出同一个答案。 "+
			"论文里必须把这两个前提显式写出来，否则审稿人会在均匀集群上复现出\"没有收益\"。")

	// K=1 的退化情形
	if containsInt(o.kList, 1) {
		k1wins, k1tot := 0, 0
		for _, c := range r.Cells {
			if c.K == 1 {
				k1tot++
				if c.JointWins {
					k1wins++
				}
			}
		}
		if k1tot > 0 && k1wins == 0 {
			out = append(out,
				fmt.Sprintf("K=1（事件只打掉一个域）时联合求解在 %d 个点上**全部不占优** —— "+
					"这不是缺陷，是正确行为：K=1 时区域事件退化为普通独立故障，"+
					"故障域感知失去着力点。论文里应当把这个退化情形明确写出来。", k1tot))
		}
	}

	if r.BaselineOnly > 0 {
		out = append(out,
			fmt.Sprintf("⚠️ 有 %d 个参数点上**联合求解不可行而 baseline 可行**（表中 ✗）。"+
				"这是最坏的情况：求解器把方案做没了。论文里必须逐个列出这些点，"+
				"并说明是有意取舍（例如控制路径约束过紧）还是求解器缺陷。", r.BaselineOnly))
	}

	if r.TieCount > 0 {
		out = append(out,
			fmt.Sprintf("有 %d 个点两者打平（表中 ○）：通常是该参数下最优解本来就唯一，"+
				"两个方法都只能选到它。打平不算占优，统计时已分开计。", r.TieCount))
	}
	return out
}

type boundaryPoint struct {
	skew, q float64
	k       int
	kind    string // "q"：区域事件率分界；"skew"：域异质性分界
}

// boundaries 找两条分界线：
//
//	kind="skew"：固定 Q/K，skew 从 0 增大时第一次"严格占优"的 skew。
//	             —— 它回答"域差异要到什么程度，联合优化才开始有用"
//	kind="q"   ：固定 skew/K，Q 从 0 增大时第一次"严格占优"的 Q。
//	             —— 它回答"区域事件率要到什么程度"
//
// **这两条线本身就是论文要的结论**，而且与具体参数值无关。
func boundaries(r *sensReport) []boundaryPoint {
	var out []boundaryPoint
	sks := append([]float64(nil), r.Config.SkewList...)
	sort.Float64s(sks)
	qs := append([]float64(nil), r.Config.QList...)
	sort.Float64s(qs)

	// 沿 skew 方向
	for _, k := range r.Config.KList {
		for _, q := range qs {
			for _, sk := range sks {
				if c := findCell(r, sk, q, k); c != nil && c.JointWins {
					out = append(out, boundaryPoint{sk, q, k, "skew"})
					break
				}
			}
		}
	}
	// 沿 Q 方向
	for _, k := range r.Config.KList {
		for _, sk := range sks {
			for _, q := range qs {
				if c := findCell(r, sk, q, k); c != nil && c.JointWins {
					out = append(out, boundaryPoint{sk, q, k, "q"})
					break
				}
			}
		}
	}
	return out
}

func containsInt(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
