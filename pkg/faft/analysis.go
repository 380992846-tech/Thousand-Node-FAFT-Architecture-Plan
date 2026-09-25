// pkg/faft/analysis.go
//
// 把"FAFT 值不值得做"变成可以算出来的数字。
//
// 本文件回答一个具体的、可证伪的问题：
//
//	在 n=1000、f=5 的设定下，把 quorum 几何与副本落点联合起来选，
//	相比"多数 quorum"，到底能省多少消息、损失多少可用性？
//
// 如果收益上界不够大，这个项目就不该继续 —— 所以这个计算必须在
// 写任何论文之前先做，而且必须能在几秒内重跑。
package faft

import (
	"fmt"
	"math"
	"strings"
)

// ScaleRow 规模扫描的一行结果。
type ScaleRow struct {
	Shards int `json:"shards"`
	// Replicas 每分片副本数。
	Replicas int `json:"replicas"`

	// Baseline（多数 quorum）
	BaseWriteMsgs   int     `json:"base_write_msgs"`
	BaseControlTol  int     `json:"base_control_tolerant"`
	BaseDataTol     int     `json:"base_data_tolerant"`
	BaseWriteMs     float64 `json:"base_write_latency_ms"`

	// FAFT（在满足控制路径容错目标下，写消息数最小）
	FaftWriteMsgs  int     `json:"faft_write_msgs"`
	FaftControlTol int     `json:"faft_control_tolerant"`
	FaftDataTol    int     `json:"faft_data_tolerant"`
	FaftWriteMs    float64 `json:"faft_write_latency_ms"`

	// 相对收益
	MsgReductionX float64 `json:"msg_reduction_x"`
	// ControlTolDelta > 0 表示 FAFT 的控制路径容错反而更高。
	ControlTolDelta int `json:"control_tol_delta"`
	// Feasible 是否存在同时满足数据与控制容错目标的 FAFT 方案。
	Feasible bool `json:"feasible"`
	// Note 说明该行的重要观察。
	Note string `json:"note"`
}

// ScaleConfig 规模扫描的配置。
type ScaleConfig struct {
	// Shards 分片数（仅用于报告，不改变几何计算）。
	Shards int
	// Replicas 每分片副本数 n。
	Replicas int
	// Domains 故障域数。
	Domains int
	// CrossDomainRTTMs 跨域 RTT。
	CrossDomainRTTMs float64
	// DataFaults / ControlFaults 容错目标（按域计）。
	DataFaults    int
	ControlFaults int
	// LogBytes 单条日志字节数。
	LogBytes int
}

func (c ScaleConfig) withDefaults() ScaleConfig {
	if c.Replicas <= 0 {
		c.Replicas = 5
	}
	if c.Domains <= 0 {
		c.Domains = 3
	}
	if c.CrossDomainRTTMs <= 0 {
		// 同城多 AZ 的典型值。
		c.CrossDomainRTTMs = 2.0
	}
	if c.DataFaults <= 0 {
		c.DataFaults = 1
	}
	if c.ControlFaults <= 0 {
		c.ControlFaults = 1
	}
	if c.LogBytes <= 0 {
		c.LogBytes = 256
	}
	return c
}

// AnalyzeScale 对一个规模配置求解并给出对比。
func AnalyzeScale(sc ScaleConfig) ScaleRow {
	sc = sc.withDefaults()
	t := NewUniformTopology(sc.Domains, sc.CrossDomainRTTMs)

	// 放置：把 n 个副本尽量均摊到各域。
	placement := make([]int, sc.Replicas)
	for i := range placement {
		placement[i] = i % sc.Domains
	}

	cfg := Config{
		Replicas:      sc.Replicas,
		Placement:     placement,
		DataFaults:    sc.DataFaults,
		ControlFaults: sc.ControlFaults,
		LeaderDomain:  0,
		LogBytes:      sc.LogBytes,
	}

	row := ScaleRow{Shards: sc.Shards, Replicas: sc.Replicas}

	// ---- 基线：多数 quorum ----
	base := BaselineMajority(t, cfg)
	row.BaseWriteMsgs = base.WriteMsgsPerOp
	row.BaseControlTol = base.ControlTolerant
	row.BaseDataTol = base.DataTolerant
	row.BaseWriteMs = base.WriteLatencyMs

	// ---- FAFT：在满足控制容错目标的前提下最小化写消息数 ----
	plans := Solve(t, cfg)
	best, ok := PickBest(plans, MinWriteMsgs, func(p Plan) bool {
		return p.MeetsData && p.MeetsControl
	})
	if !ok {
		// 退一步：只要求数据路径达标，报告控制容错的实际值。
		best, ok = PickBest(plans, MinWriteMsgs, func(p Plan) bool { return p.MeetsData })
	}
	row.Feasible = ok
	if ok {
		row.FaftWriteMsgs = best.WriteMsgsPerOp
		row.FaftControlTol = best.ControlTolerant
		row.FaftDataTol = best.DataTolerant
		row.FaftWriteMs = best.WriteLatencyMs

		if best.WriteMsgsPerOp > 0 {
			row.MsgReductionX = float64(base.WriteMsgsPerOp) / float64(best.WriteMsgsPerOp)
		}
		row.ControlTolDelta = best.ControlTolerant - base.ControlTolerant
	}

	row.Note = describeRow(row, base, best, ok)
	return row
}

func describeRow(row ScaleRow, base, best Plan, ok bool) string {
	if !ok {
		return "无可行方案"
	}
	if row.MsgReductionX < 1.05 {
		return "收益可忽略"
	}
	if row.ControlTolDelta < 0 {
		return fmt.Sprintf("省消息 %.1fx，但控制路径容错从 %d 降到 %d —— 需要显式设计取舍",
			row.MsgReductionX, base.ControlTolerant, best.ControlTolerant)
	}
	return fmt.Sprintf("省消息 %.1fx 且控制路径容错不低于基线", row.MsgReductionX)
}

// MaxBenefit 扫描不同 n，返回收益最大的规模点与收益上界。
//
// 这是"要不要做这个项目"的判定函数。
func MaxBenefit(sc ScaleConfig, replicaCounts []int) (best ScaleRow, all []ScaleRow) {
	bestX := 0.0
	for _, n := range replicaCounts {
		c := sc
		c.Replicas = n
		r := AnalyzeScale(c)
		all = append(all, r)
		if r.Feasible && r.MsgReductionX > bestX {
			bestX = r.MsgReductionX
			best = r
		}
	}
	return best, all
}

// FormatScaleTable 渲染规模扫描表。
func FormatScaleTable(rows []ScaleRow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-6s %-6s %-12s %-12s %-10s %-12s %-12s %s\n",
		"n", "域数", "基线写消息", "FAFT写消息", "省倍数", "基线控制容错", "FAFT控制容错", "观察")
	fmt.Fprintf(&b, "%s\n", strings.Repeat("-", 110))
	for _, r := range rows {
		fmt.Fprintf(&b, "%-6d %-6s %-12d %-12d %-10.2f %-12d %-12d %s\n",
			r.Replicas, "-", r.BaseWriteMsgs, r.FaftWriteMsgs,
			r.MsgReductionX, r.BaseControlTol, r.FaftControlTol, r.Note)
	}
	return b.String()
}

// TradeoffPoint 可用性-成本权衡曲线上的一个点。
type TradeoffPoint struct {
	// Q2Size / Q1Size 两个阶段的 quorum 大小。
	Q2Size, Q1Size int
	// WriteMsgs 稳态写消息数。
	WriteMsgs int
	// DataTol / ControlTol 两条路径各自的域级容错。
	DataTol, ControlTol int
	// WriteLatencyMs 稳态写延迟。
	WriteLatencyMs float64
	// SelectionLatencyMs 选主延迟。
	SelectionLatencyMs float64
}

// TradeoffCurve 给出某个 n 下完整的"消息数 vs 控制容错"权衡曲线。
//
// 这条曲线本身就是论文里的一张图：它显示了 |Q1|+|Q2| > N 这条约束
// 如何把两个目标锁在一起。
func TradeoffCurve(sc ScaleConfig) []TradeoffPoint {
	sc = sc.withDefaults()
	t := NewUniformTopology(sc.Domains, sc.CrossDomainRTTMs)

	placement := make([]int, sc.Replicas)
	for i := range placement {
		placement[i] = i % sc.Domains
	}
	cfg := Config{
		Replicas: sc.Replicas, Placement: placement,
		DataFaults: sc.DataFaults, ControlFaults: sc.ControlFaults,
		LogBytes: sc.LogBytes,
	}

	seen := map[[2]int]bool{}
	var out []TradeoffPoint
	for _, p := range Solve(t, cfg) {
		q1, q2 := p.Quorum.Size()
		key := [2]int{q1, q2}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, TradeoffPoint{
			Q2Size: q2, Q1Size: q1,
			WriteMsgs:          p.WriteMsgsPerOp,
			DataTol:            p.DataTolerant,
			ControlTol:         p.ControlTolerant,
			WriteLatencyMs:     p.WriteLatencyMs,
			SelectionLatencyMs: p.SelectionLatencyMs,
		})
	}
	// 按写消息数升序，便于看"省消息的代价"。
	sortByWriteMsgs(out)
	return out
}

func sortByWriteMsgs(ps []TradeoffPoint) {
	for i := 1; i < len(ps); i++ {
		for j := i; j > 0 && ps[j].WriteMsgs < ps[j-1].WriteMsgs; j-- {
			ps[j], ps[j-1] = ps[j-1], ps[j]
		}
	}
}

// FormatTradeoff 渲染权衡曲线。
func FormatTradeoff(pts []TradeoffPoint) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-8s %-8s %-12s %-10s %-12s %-14s %-14s\n",
		"|Q2|", "|Q1|", "写消息/op", "数据容错", "控制容错", "写延迟(ms)", "选主延迟(ms)")
	fmt.Fprintf(&b, "%s\n", strings.Repeat("-", 90))
	for _, p := range pts {
		fmt.Fprintf(&b, "%-8d %-8d %-12d %-10d %-12d %-14.2f %-14.2f\n",
			p.Q2Size, p.Q1Size, p.WriteMsgs, p.DataTol, p.ControlTol,
			p.WriteLatencyMs, p.SelectionLatencyMs)
	}
	return b.String()
}

// AvailabilityBound 计算某个 quorum 几何下的理论可用性上界。
//
// 模型：每个故障域以概率 p 独立整体失效。quorum 覆盖 d 个域时，
// 只要有 d - tol 个域在线就能形成 quorum，因此
//
//	P[可用] = P[最多 tol 个域同时失效]
//
// 注意这是**独立失效**假设；相关失效见 CorrelatedAvailability。
// 在两个独立假设之间做对比本身就是论文的一个消融实验。
func AvailabilityBound(domainsInQuorum, tol int, p float64) float64 {
	if p <= 0 {
		return 1
	}
	if p >= 1 {
		return 0
	}
	// P[最多 tol 个失效] = Σ_{i=0}^{tol} C(d,i) p^i (1-p)^(d-i)
	sum := 0.0
	for i := 0; i <= tol && i <= domainsInQuorum; i++ {
		sum += binom(domainsInQuorum, i) * math.Pow(p, float64(i)) * math.Pow(1-p, float64(domainsInQuorum-i))
	}
	return sum
}

func binom(n, k int) float64 {
	if k < 0 || k > n {
		return 0
	}
	r := 1.0
	for i := 1; i <= k; i++ {
		r = r * float64(n-k+i) / float64(i)
	}
	return r
}

// AvailabilityAtReplicaLevel 在**副本**粒度上计算某个 quorum 的可用性。
//
// 为什么需要它：域粒度的容错计数会饱和 —— 5 个域最多只能容错 4，
// 因此无法区分"需要 6/1000 在线"与"需要 995/1000 在线"这两种天差地别的配置。
// 必须回到副本粒度，用二项式尾部概率才能看出 Flexible Paxos 的真实代价。
//
// 模型：n 个副本各自以概率 p 独立失效（把域级相关性留给
// CorrelatedAvailability 处理），quorum 大小为 q 时可用性为
//
//	P[可用] = P[至少 q 个副本在线] = Σ_{i=q}^{n} C(n,i)(1-p)^i p^(n-i)
//
// 数值稳定性：n 很大时 (1-p)^n 会下溢，因此用对数域计算。
func AvailabilityAtReplicaLevel(n, q int, p float64) float64 {
	if q <= 0 {
		return 1
	}
	if p <= 0 {
		return 1
	}
	if q > n {
		return 0
	}
	if p >= 1 {
		return 0
	}

	logP := math.Log(p)
	log1mP := math.Log(1 - p)
	logSum := math.Inf(-1)
	for i := q; i <= n; i++ {
		// log C(n,i) + i*log(1-p) + (n-i)*log(p)
		lt := logBinom(n, i) + float64(i)*log1mP + float64(n-i)*logP
		logSum = logAdd(logSum, lt)
	}
	if math.IsInf(logSum, -1) {
		return 0
	}
	v := math.Exp(logSum)
	if v > 1 {
		return 1
	}
	return v
}

func logBinom(n, k int) float64 {
	if k < 0 || k > n {
		return math.Inf(-1)
	}
	return logGamma(float64(n)+1) - logGamma(float64(k)+1) - logGamma(float64(n-k)+1)
}

// logGamma 用 Lanczos 近似实现，避免引入外部依赖。
func logGamma(x float64) float64 {
	cof := [6]float64{
		76.18009172947146, -86.50532032941677, 24.01409824083091,
		-1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5,
	}
	y := x
	tmp := x + 5.5
	tmp -= (x + 0.5) * math.Log(tmp)
	ser := 1.000000000190015
	for j := 0; j < 6; j++ {
		y++
		ser += cof[j] / y
	}
	return -tmp + math.Log(2.5066282746310005*ser/x)
}

func logAdd(a, b float64) float64 {
	if math.IsInf(a, -1) {
		return b
	}
	if math.IsInf(b, -1) {
		return a
	}
	if a < b {
		a, b = b, a
	}
	return a + math.Log1p(math.Exp(b-a))
}

// ControlCost 一个 quorum 方案在**可用性**层面的代价。
//
// 这正是 Flexible Paxos 的真实代价所在：把阶段二 quorum 从 ⌊n/2⌋+1 缩到 f+1，
// 消息数大幅下降，但阶段一 quorum 必须相应变大（|Q1| ≥ n-|Q2|+1），
// 于是**选举所需的在线副本数急剧上升**。
type ControlCost struct {
	// DataQuorum 阶段二 quorum 大小。
	DataQuorum int
	// ControlQuorum 阶段一 quorum 大小。
	ControlQuorum int
	// DataAvailability 在全集群副本失效率 p 下，数据路径可用的概率。
	DataAvailability float64
	// ControlAvailability 控制路径（能选出 leader）可用的概率。
	ControlAvailability float64
	// ReplicasNeededToElect 选举所需的最少在线副本数。
	ReplicasNeededToElect int
	// ReplicasNeededToCommit 提交所需的最少在线副本数。
	ReplicasNeededToCommit int
}

// ComputeControlCost 计算某个 quorum 几何在副本粒度的可用性代价。
func ComputeControlCost(n int, q2 int, replicaFailProb float64) ControlCost {
	q1 := n - q2 + 1 // |Q1| + |Q2| > n 的最小解
	if q1 > n {
		q1 = n
	}
	return ControlCost{
		DataQuorum:             q2,
		ControlQuorum:          q1,
		DataAvailability:       AvailabilityAtReplicaLevel(n, q2, replicaFailProb),
		ControlAvailability:    AvailabilityAtReplicaLevel(n, q1, replicaFailProb),
		ReplicasNeededToElect:  q1,
		ReplicasNeededToCommit: q2,
	}
}

// CorrelatedAvailability 相关失效模型下的可用性。
//
// 与独立模型的区别：独立模型假设"每个域各自以 p 失效"，而现实中
// 一次区域性事件（供电、交换机固件、BGP 抖动）会同时打掉多个域。
// 这里用"以概率 q 发生一次全局事件，命中 k 个域"的简化模型，
// 用来量化**独立假设高估了多少可用性**。
func CorrelatedAvailability(domainsInQuorum, tol int, p float64, globalEventProb float64, domainsPerEvent int) float64 {
	avail := (1 - globalEventProb) * AvailabilityBound(domainsInQuorum, tol, p)
	// 全局事件下，若一次打掉的域数超过容错度，则不可用。
	if domainsPerEvent <= tol {
		avail += globalEventProb * 1.0
	}
	return avail
}
