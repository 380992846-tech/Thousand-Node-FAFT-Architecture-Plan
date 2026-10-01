package faft

import (
	"fmt"
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// 几何求解
// ---------------------------------------------------------------------------

// TestBaselineMajorityIsMajority 基线必须是标准多数 quorum。
func TestBaselineMajorityIsMajority(t *testing.T) {
	for _, n := range []int{3, 5, 7, 9, 21, 101} {
		tp := NewUniformTopology(3, 2.0)
		p := BaselineMajority(tp, Config{Replicas: n})
		q1, q2 := p.Quorum.Size()
		want := n/2 + 1
		if q1 != want || q2 != want {
			t.Errorf("n=%d: 多数 quorum = (%d,%d)，期望 (%d,%d)", n, q1, q2, want, want)
		}
		if !p.Safe {
			t.Errorf("n=%d: 多数 quorum 应满足 |Q1|+|Q2| > N", n)
		}
		// 稳态写消息数 = 2*|Q2|
		if p.WriteMsgsPerOp != 2*want {
			t.Errorf("n=%d: 写消息 %d，期望 %d", n, p.WriteMsgsPerOp, 2*want)
		}
	}
}

// TestSolveRespectsFlexiblePaxosConstraint 所有返回方案必须满足 |Q1|+|Q2| > N。
//
// 出处：Howard, Malkhi, Spiegelman, OPODIS 2016, §4.2。
// 这是安全性的必要条件，求解器绝不能返回违反它的方案。
func TestSolveRespectsFlexiblePaxosConstraint(t *testing.T) {
	for _, n := range []int{3, 5, 7, 9, 11, 21, 51} {
		tp := NewUniformTopology(5, 2.0)
		plans := Solve(tp, Config{Replicas: n})
		if len(plans) == 0 {
			t.Fatalf("n=%d: 求解器未返回任何方案", n)
		}
		for _, p := range plans {
			q1, q2 := p.Quorum.Size()
			if !p.Safe {
				t.Errorf("n=%d: 返回了不安全方案 |Q1|=%d |Q2|=%d（和 %d <= N）",
					n, q1, q2, q1+q2)
			}
			if q1+q2 <= n {
				t.Errorf("n=%d: |Q1|+|Q2| = %d 未超过 N=%d", n, q1+q2, n)
			}
			// 成员下标必须在 [0,n) 且无重复。
			seen := map[int]bool{}
			for _, m := range append(append([]int{}, p.Quorum.Q1Members...), p.Quorum.Q2Members...) {
				if m < 0 || m >= n {
					t.Errorf("n=%d: 成员下标 %d 越界", n, m)
				}
				seen[m] = true
			}
			if len(p.Quorum.Q1Members) != q1 || len(p.Quorum.Q2Members) != q2 {
				t.Errorf("n=%d: Size() 与实际成员数不一致", n)
			}
		}
	}
}

// TestSolveQ1Q2Intersect 两个 quorum 必须相交（Flexible Paxos 的核心要求）。
//
// 注意：Solve 的 Q2 取"离 leader 最近"、Q1 取"覆盖域最多"，
// 二者可能不相交 —— 而 |Q1|+|Q2|>N 只是逐对相交的充分条件之一。
// 本测试检查 Evaluate 是否正确报告了相交性。
func TestEvaluateReportsIntersection(t *testing.T) {
	tp := NewUniformTopology(3, 2.0)

	// 构造一个必然相交的方案：Q2 ⊂ Q1
	p := Evaluate(tp, Config{Replicas: 5}, Quorum{
		Q2Members: []int{0, 1, 2},
		Q1Members: []int{0, 1, 2, 3, 4},
	})
	if !p.Intersects {
		t.Error("Q2 ⊂ Q1 时 Intersects 应为 true")
	}
	if !p.Safe {
		t.Error("|Q1|+|Q2| = 8 > 5，Safe 应为 true")
	}

	// 构造不相交方案
	p2 := Evaluate(tp, Config{Replicas: 6}, Quorum{
		Q2Members: []int{0, 1, 2},
		Q1Members: []int{3, 4, 5},
	})
	if p2.Intersects {
		t.Error("成员完全不重叠时 Intersects 应为 false")
	}
	// |Q1|+|Q2| = 6 不 > 6，因此不安全
	if p2.Safe {
		t.Error("|Q1|+|Q2| = 6 不满足 > N=6，Safe 应为 false")
	}
}

// TestMessagesMatchQuorumSize 写消息数必须等于 2*|Q2|。
func TestMessagesMatchQuorumSize(t *testing.T) {
	tp := NewUniformTopology(5, 2.0)
	for _, p := range Solve(tp, Config{Replicas: 21}) {
		_, q2 := p.Quorum.Size()
		if p.WriteMsgsPerOp != 2*q2 {
			t.Errorf("|Q2|=%d 但写消息 = %d，期望 %d", q2, p.WriteMsgsPerOp, 2*q2)
		}
	}
}

// TestLatencyFollowsTopology 延迟必须随跨域 RTT 单调变化。
func TestLatencyFollowsTopology(t *testing.T) {
	cfg := Config{Replicas: 9, LeaderDomain: 0}

	near := Evaluate(NewUniformTopology(3, 1.0), cfg, Quorum{
		Q2Members: []int{0, 1, 2}, Q1Members: []int{0, 1, 2, 3, 4, 5, 6, 7},
	})
	far := Evaluate(NewUniformTopology(3, 50.0), cfg, Quorum{
		Q2Members: []int{0, 1, 2}, Q1Members: []int{0, 1, 2, 3, 4, 5, 6, 7},
	})
	if !(far.WriteLatencyMs > near.WriteLatencyMs) {
		t.Errorf("跨域 RTT 增大后写延迟未上升：%.2f -> %.2f", near.WriteLatencyMs, far.WriteLatencyMs)
	}
	if !(far.SelectionLatencyMs > near.SelectionLatencyMs) {
		t.Errorf("跨域 RTT 增大后选主延迟未上升：%.2f -> %.2f",
			near.SelectionLatencyMs, far.SelectionLatencyMs)
	}
}

// TestPeerDomainQuorumHasZeroLatency 全部副本在同一域时跨域延迟应为 0。
func TestPeerDomainQuorumHasZeroLatency(t *testing.T) {
	tp := NewUniformTopology(1, 10.0) // 只有一个域
	p := Evaluate(tp, Config{Replicas: 5, LeaderDomain: 0}, Quorum{
		Q2Members: []int{0, 1, 2}, Q1Members: []int{0, 1, 2, 3},
	})
	if p.WriteLatencyMs != 0 {
		t.Errorf("单域内写延迟 = %.2f，期望 0", p.WriteLatencyMs)
	}
	if p.SelectionLatencyMs != 0 {
		t.Errorf("单域内选主延迟 = %.2f，期望 0", p.SelectionLatencyMs)
	}
	// 单域下只能容忍 0 个域失效
	if p.DataTolerant != 0 {
		t.Errorf("单域下 DataTolerant = %d，期望 0", p.DataTolerant)
	}
}

// TestPickBestHonorsConstraint 在满足约束的方案里选最优。
func TestPickBestHonorsConstraint(t *testing.T) {
	tp := NewUniformTopology(5, 2.0)
	plans := Solve(tp, Config{Replicas: 51})

	best, ok := PickBest(plans, MinWriteMsgs, func(p Plan) bool { return p.MeetsData })
	if !ok {
		t.Fatal("未找到满足数据容错的方案")
	}
	// 该方案必须真的满足约束，且写消息数不高于任何其他满足约束的方案。
	for _, p := range plans {
		if p.MeetsData && p.WriteMsgsPerOp < best.WriteMsgsPerOp {
			t.Errorf("存在更优方案：%d < 选中的 %d", p.WriteMsgsPerOp, best.WriteMsgsPerOp)
		}
	}
	if !best.MeetsData {
		t.Error("选中的方案不满足约束")
	}
}

// TestScaleAnalysisMonotonic 规模扫描必须单调：n 越大，节省倍数越高（或持平）。
func TestScaleAnalysisMonotonic(t *testing.T) {
	sc := ScaleConfig{Domains: 5, CrossDomainRTTMs: 2.0, DataFaults: 1, ControlFaults: 1}
	_, rows := MaxBenefit(sc, []int{3, 5, 7, 9, 11, 21, 51, 101})
	if len(rows) == 0 {
		t.Fatal("未返回任何规模行")
	}
	for _, r := range rows {
		if r.BaseWriteMsgs <= 0 {
			t.Errorf("n=%d: 基线写消息 = %d", r.Replicas, r.BaseWriteMsgs)
		}
		if r.Feasible && r.MsgReductionX < 1.0 {
			t.Errorf("n=%d: 省倍数 %.2f < 1，FAFT 不应比基线更差",
				r.Replicas, r.MsgReductionX)
		}
		// 基线：多数 quorum => 写消息 = 2*(n/2+1)
		want := 2 * (r.Replicas/2 + 1)
		if r.BaseWriteMsgs != want {
			t.Errorf("n=%d: 基线写消息 %d，期望 %d", r.Replicas, r.BaseWriteMsgs, want)
		}
	}
	t.Logf("规模扫描：%d 档", len(rows))
}

// TestTradeoffCurveOrdered 权衡曲线按写消息数升序且无重复 (q1,q2) 组合。
func TestTradeoffCurveOrdered(t *testing.T) {
	pts := TradeoffCurve(ScaleConfig{Replicas: 51, Domains: 5, CrossDomainRTTMs: 2.0})
	if len(pts) == 0 {
		t.Fatal("权衡曲线为空")
	}
	seen := map[[2]int]bool{}
	for i, p := range pts {
		key := [2]int{p.Q1Size, p.Q2Size}
		if seen[key] {
			t.Errorf("重复的 (|Q1|,|Q2|) 组合: %v", key)
		}
		seen[key] = true
		if i > 0 && p.WriteMsgs < pts[i-1].WriteMsgs {
			t.Errorf("曲线未按写消息数升序：%d 出现在 %d 之后", p.WriteMsgs, pts[i-1].WriteMsgs)
		}
		if p.Q1Size+p.Q2Size <= 51 {
			t.Errorf("(%d,%d) 不满足 |Q1|+|Q2| > 51", p.Q1Size, p.Q2Size)
		}
	}
}

// ---------------------------------------------------------------------------
// 副本粒度可用性
// ---------------------------------------------------------------------------

// TestAvailabilityAtReplicaLevelMatchesBinomial 与独立的解析参照实现对照。
//
// 参数语义：p 是**失效**概率，q 是**需要在线**的副本数，因此
//
//	P[可用] = P[存活副本数 >= q]
//
// 参照实现（tailPMF）用**解析上尾**，即 P[失效数 <= n-q] = 1 - P[失效数 > n-q]，
// 在 log 空间累加，对 q 在两端都能给出正确结果。
//
// 初版测试把参照式写成了 Σ_{i=q}^{n} PMF(n,i,p)，把 p 当成了存活概率，
// 于是"证明"了错误实现是对的 —— 教训：参照实现必须独立于被测实现，
// 且对参数语义要与被测实现区分开（这里用"失效数"视角来交叉验证）。
func TestAvailabilityAtReplicaLevelMatchesBinomial(t *testing.T) {
	cases := []struct {
		n, q int
		p    float64
	}{
		{20, 12, 0.05}, // q 略高于均值 1.0，可用性极高
		{10, 1, 0.1},
		{10, 10, 0.1},
		{5, 3, 0.01},
		{21, 11, 0.1},
		{50, 26, 0.05},
		{200, 199, 0.01}, // q 接近 n，可用性极小 —— 检验两端精度
	}
	for _, c := range cases {
		got := AvailabilityAtReplicaLevel(c.n, c.q, c.p)
		// 至少 q 个在线 => 至多 n-q 个失效 => 1 - P[失效数 > n-q]
		want := 1 - tailPMF(c.n, c.n-c.q, c.p)

		// 容差必须用**相对**值：当可用性接近 1 时，参照式要做 1-极小值，
		// 必然损失约 1e-16 的绝对精度，用绝对容差会误报。
		// 本实现走 log 空间直接算上尾，没有这个抵消，因此两者会在
		// 1e-15 绝对量级上有差异，但相对误差远小于 1e-9。
		scale := math.Max(math.Abs(got), math.Abs(want))
		if scale < 1e-300 {
			scale = 1
		}
		relErr := math.Abs(got-want) / scale
		if relErr > 1e-9 {
			t.Errorf("n=%d q=%d p=%g: 实现 %.15g vs 参照 %.15g（相对误差 %.3e）",
				c.n, c.q, c.p, got, want, relErr)
		}
		t.Logf("n=%3d q=%3d p=%-6g -> %.9g（%s）", c.n, c.q, c.p, got, pct(got))
	}
}

// TestAvailabilityEdgeCases 边界条件必须自洽。
func TestAvailabilityEdgeCases(t *testing.T) {
	// q=0（不需要任何副本在线）或 p=0（无失效）=> 必然可用
	if got := AvailabilityAtReplicaLevel(10, 0, 0.1); got != 1 {
		t.Errorf("q=0 时可用性 = %v，期望 1", got)
	}
	if got := AvailabilityAtReplicaLevel(10, 5, 0); got != 1 {
		t.Errorf("p=0 时可用性 = %v，期望 1", got)
	}
	// p=1（全部必然失效）=> 只有 q=0 才可用
	if got := AvailabilityAtReplicaLevel(10, 5, 1); got != 0 {
		t.Errorf("p=1 时可用性 = %v，期望 0", got)
	}
	// q > n => 不可用
	if got := AvailabilityAtReplicaLevel(10, 11, 0.1); got != 0 {
		t.Errorf("q>n 时可用性 = %v，期望 0", got)
	}
	// q=1：只要有一个副本在线即可 => 1 - p^n
	got1 := AvailabilityAtReplicaLevel(10, 1, 0.1)
	want1 := 1 - math.Pow(0.1, 10)
	if math.Abs(got1-want1) > 1e-12 {
		t.Errorf("q=1 时可用性 = %.15g，期望 %.15g（= 1-p^n）", got1, want1)
	}
	// q=n：要求全部副本在线 => (1-p)^n
	gotN := AvailabilityAtReplicaLevel(10, 10, 0.1)
	wantN := math.Pow(0.9, 10)
	if math.Abs(gotN-wantN) > 1e-12 {
		t.Errorf("q=n 时可用性 = %.15g，期望 %.15g（= (1-p)^n）", gotN, wantN)
	}
	// 单调性：q 越大越难满足
	prev := 2.0
	for q := 1; q <= 10; q++ {
		v := AvailabilityAtReplicaLevel(10, q, 0.2)
		if v > prev+1e-15 {
			t.Errorf("q=%d 时可用性 %.15g 高于 q=%d 的 %.15g（应随 q 单调下降）",
				q, v, q-1, prev)
		}
		prev = v
	}
}

// TestControlCostFlexiblePaxosTradeoff 弹性 quorum 的代价：|Q2| 变小 => |Q1| 变大。
//
// 这是本项目的核心命题，必须在 API 层面可检验。
//
// 参数选择的关键：要让三种配置的可用性落在**可区分**的区间，
// 否则两端都饱和到 1 或 0，比较就失去意义。
//
// 参数选择原则：门槛必须落在**期望在线副本数**附近，否则可用性会饱和到
// 0 或 1，对比失去意义。期望在线数 = n(1-p)，所以要取
//
//	p ≈ 1 - q/n   （q 为待比较的门槛）
//
// 取 n=100、p=0.5（期望在线 50 个）：
//
//	|Q2|=51（多数 quorum）: 需 51 在线 → 约 0.5  ← 门槛正好在均值
//	                        |Q1|=50      → 约 0.5  对称
//	|Q2|=2  （极限）      : 需 2 在线   → ~1
//	                        |Q1|=99      → 约 0.32（需 99 在线，期望 50）
//
// 这个区间两端都不饱和，才是有效的量级对比。
// 此前试过 (n=1000,p=0.01) 与 (n=200,p=0.01)：前者两个门槛都远低于
// 期望在线数 990，可用性全是 1；后者门槛远高于期望在线数 198，
// 可用性全是 0。都不可比。
func TestControlCostFlexiblePaxosTradeoff(t *testing.T) {
	const n = 100
	const p = 0.5

	majority := ComputeControlCost(n, n/2+1, p)
	extreme := ComputeControlCost(n, 2, p)

	// quorum 大小必须符合 |Q1| + |Q2| > n 的最小解。
	if majority.DataQuorum != 51 {
		t.Errorf("多数 quorum |Q2| = %d，期望 51", majority.DataQuorum)
	}
	if extreme.DataQuorum != 2 {
		t.Errorf("极限配置 |Q2| = %d，期望 2", extreme.DataQuorum)
	}
	if extreme.ControlQuorum != n-2+1 {
		t.Errorf("极限配置 |Q1| = %d，期望 %d", extreme.ControlQuorum, n-2+1)
	}
	if majority.ControlQuorum != n-(n/2+1)+1 {
		t.Errorf("多数 quorum 的 |Q1| = %d，期望 %d", majority.ControlQuorum, n-(n/2+1)+1)
	}

	// 选主所需在线副本数：从 50 涨到 99。
	if majority.ReplicasNeededToElect != 50 {
		t.Errorf("多数 quorum 选主需 %d 副本，期望 50", majority.ReplicasNeededToElect)
	}
	if extreme.ReplicasNeededToElect != 99 {
		t.Errorf("极限配置选主需 %d 副本，期望 99", extreme.ReplicasNeededToElect)
	}

	// 数据路径：|Q2| 从 51 缩到 2，可用性应当**大幅提升**。
	if !(extreme.DataAvailability > majority.DataAvailability) {
		t.Errorf("|Q2|=2 的数据可用性 %.9g 未高于多数 quorum 的 %.9g",
			extreme.DataAvailability, majority.DataAvailability)
	}
	if extreme.DataAvailability < 0.99 {
		t.Errorf("|Q2|=2 时只需 2 个副本在线（期望在线 50），数据可用性 %.9g 应接近 1",
			extreme.DataAvailability)
	}
	// 多数 quorum 的门槛正好在期望在线数上，可用性应在 0.5 附近。
	if majority.DataAvailability < 0.3 || majority.DataAvailability > 0.7 {
		t.Errorf("|Q2|=51 且期望在线 50 时，数据可用性 %.9g 应在 0.5 附近",
			majority.DataAvailability)
	}

	// 控制路径：代价在这里 —— |Q1| 从 50 涨到 99。
	if !(extreme.ControlAvailability < majority.ControlAvailability) {
		t.Errorf("|Q2|=2 的控制可用性 %.9g 未低于多数 quorum 的 %.9g",
			extreme.ControlAvailability, majority.ControlAvailability)
	}
	// 要求 99/100 在线而期望在线仅 50 => 必然很低。
	if extreme.ControlAvailability > 0.05 {
		t.Errorf("|Q2|=2 时要求 99/100 副本在线，控制可用性 %.9g 应很低",
			extreme.ControlAvailability)
	}
	// 门槛差值才是核心代价：49 个额外副本必须同时在线。
	if extreme.ReplicasNeededToElect-majority.ReplicasNeededToElect != 49 {
		t.Errorf("选主门槛上升 %d，期望 49",
			extreme.ReplicasNeededToElect-majority.ReplicasNeededToElect)
	}

	t.Logf("n=%d p=%g（期望在线 %.0f 个）", n, p, float64(n)*(1-p))
	t.Logf("  |Q2| %d -> %d ；|Q1| %d -> %d ；选主需在线 %d -> %d（+%d）",
		majority.DataQuorum, extreme.DataQuorum,
		majority.ControlQuorum, extreme.ControlQuorum,
		majority.ReplicasNeededToElect, extreme.ReplicasNeededToElect,
		extreme.ReplicasNeededToElect-majority.ReplicasNeededToElect)
	t.Logf("  数据可用性 %.6g -> %.6g（提升）",
		majority.DataAvailability, extreme.DataAvailability)
	t.Logf("  控制可用性 %.6g -> %.6g（代价）",
		majority.ControlAvailability, extreme.ControlAvailability)
}

// TestControlCostTradeoffUnderRealisticFailureRate 用更温和的失效率
// 验证"控制路径可用性随 |Q2| 缩小而单调下降、数据路径单调上升"。
func TestControlCostTradeoffUnderRealisticFailureRate(t *testing.T) {
	const n = 100
	const p = 0.05

	prevCtrl := 2.0
	prevData := -1.0
	prevQ2 := 0
	for _, q2 := range []int{51, 25, 10, 5, 2} {
		cc := ComputeControlCost(n, q2, p)
		// |Q2| 缩小 => |Q1| 变大 => 控制可用性下降
		if cc.ControlAvailability > prevCtrl {
			t.Errorf("|Q2|=%d 控制可用性 %.9g 高于 |Q2|=%d 的 %.9g（应随 |Q2| 缩小而下降）",
				q2, cc.ControlAvailability, prevQ2, prevCtrl)
		}
		// |Q2| 缩小 => 数据可用性上升
		if cc.DataAvailability < prevData {
			t.Errorf("|Q2|=%d 数据可用性 %.9g 低于 |Q2|=%d 的 %.9g（应随 |Q2| 缩小而上升）",
				q2, cc.DataAvailability, prevQ2, prevData)
		}
		if cc.ReplicasNeededToElect != n-q2+1 {
			t.Errorf("|Q2|=%d: 选主需 %d 副本，期望 %d",
				q2, cc.ReplicasNeededToElect, n-q2+1)
		}
		prevCtrl = cc.ControlAvailability
		prevData = cc.DataAvailability
		prevQ2 = q2
	}

	// 对照：n=100 时 |Q2| 从 51 缩到 2，选主门槛从 50 涨到 99。
	first := ComputeControlCost(n, 51, p)
	last := ComputeControlCost(n, 2, p)
	t.Logf("n=%d p=%g: 选主需在线副本 %d -> %d；控制可用性 %.6g -> %.6g；数据可用性 %.6g -> %.6g",
		n, p, first.ReplicasNeededToElect, last.ReplicasNeededToElect,
		first.ControlAvailability, last.ControlAvailability,
		first.DataAvailability, last.DataAvailability)
}

// ---------------------------------------------------------------------------
// 故障模型
// ---------------------------------------------------------------------------

// TestIndependentFailureMatchesBinomial 独立模型必须等于二项式尾部。
func TestIndependentFailureMatchesBinomial(t *testing.T) {
	m := IndependentFailure{P: 0.01}
	for _, d := range []int{3, 5, 7} {
		for need := 1; need <= d; need++ {
			got := m.QuorumAvailable(d, need)
			want := binomTail(d, need, 0.01)
			if math.Abs(got-want) > 1e-12 {
				t.Errorf("d=%d need=%d: %.15f != %.15f", d, need, got, want)
			}
		}
	}
}

// TestCorrelatedFailureDegradesAvailability 相关失效必须降低可用性。
//
// 这是本项目最重要的定量论断之一：独立假设会高估可用性。
//
// 关于高估量的量级：事件命中 K=2 个域，而容错度为 1 时事件必然致命，
// 因此可用性下降的量约等于 Q（区域事件概率）乘以"事件后仍可用的条件概率"。
// 取 Q=1e-4 时高估量约 2e-7（另有一个 1e-6 量级的独立双域失效项）。
func TestCorrelatedFailureDegradesAvailability(t *testing.T) {
	ind := IndependentFailure{P: 0.001}
	cor := CorrelatedFailure{P: 0.001, Q: 1e-4, K: 2}

	for _, d := range []int{3, 5, 7} {
		indA := ind.QuorumAvailable(d, 1) // 容忍 1 个域失效
		corA := cor.QuorumAvailable(d, 1)
		if !(corA < indA) {
			t.Errorf("d=%d: 相关模型可用性 %.12f 未低于独立模型 %.12f", d, corA, indA)
		}
		over := indA - corA
		// 量级应在 1e-7 ~ 1e-4 之间（Q=1e-4 与独立双域项 1e-6 的合成）。
		if over < 1e-8 || over > 1e-3 {
			t.Errorf("d=%d: 高估量 %.3e 超出合理量级 [1e-8, 1e-3]", d, over)
		}
		t.Logf("d=%d 容忍1域: 独立 %.12f 相关 %.12f 高估 %.3e", d, indA, corA, over)
	}

	// 事件概率越大，高估越明显 —— 单调性检查。
	prev := -1.0
	for _, q := range []float64{0, 1e-5, 1e-4, 1e-3} {
		c := CorrelatedFailure{P: 0.001, Q: q, K: 2}
		over := ind.QuorumAvailable(5, 1) - c.QuorumAvailable(5, 1)
		if prev >= 0 && over < prev {
			t.Errorf("Q=%.0e 时高估量 %.3e 低于前一档 %.3e", q, over, prev)
		}
		prev = over
	}
}

// TestCorrelatedFailureNonFatalEvent 事件命中数不超过容错度时不应致命。
func TestCorrelatedFailureNonFatalEvent(t *testing.T) {
	// 5 个域、需要 3 个在线（即容忍 2 个失效），事件只命中 2 个域 => 仍可用。
	cor := CorrelatedFailure{P: 0, Q: 0.5, K: 2}
	if got := cor.QuorumAvailable(5, 3); got != 1 {
		t.Errorf("非致命事件下可用性 = %v，期望 1", got)
	}

	// 事件命中 3 个域 => 只剩 2 个，而需要 3 个 => 必然不可用。
	//
	// 注意结果不是 0 而是 0.5：模型是 (1-Q)*P_noEvent + Q*P_withEvent，
	// 其中 P_noEvent（不发生事件时，P=0 保证独立失效不触发）为 1，
	// P_withEvent 为 0。因此 Q=0.5 时结果是 0.5。
	// 初版测试期望 0，是把"事件那支为 0"误当成了"整体为 0"。
	cor2 := CorrelatedFailure{P: 0, Q: 0.5, K: 3}
	if got := cor2.QuorumAvailable(5, 3); got != 0.5 {
		t.Errorf("致命事件下可用性（need=3, K=3, 剩 2 域） = %v，期望 0.5（= 1-Q）", got)
	}

	// 事件概率越大，可用性越低：1 -> 0.75 -> 0.5
	for q, want := range map[float64]float64{0.25: 0.75, 0.5: 0.5, 1.0: 0.0} {
		cc := CorrelatedFailure{P: 0, Q: q, K: 3}
		if got := cc.QuorumAvailable(5, 3); math.Abs(got-want) > 1e-12 {
			t.Errorf("Q=%g 致命事件下可用性 = %v，期望 %v", q, got, want)
		}
	}

	// 同上但只需要 2 个在线 => 事件非致命，可用性恒为 1。
	cor3 := CorrelatedFailure{P: 0, Q: 0.5, K: 3}
	if got := cor3.QuorumAvailable(5, 2); got != 1 {
		t.Errorf("need=2 且事件后剩 2 域时应可用，实际 %v", got)
	}
	cor5 := CorrelatedFailure{P: 0, Q: 0.25, K: 1}
	if got := cor5.QuorumAvailable(5, 3); got != 1 {
		t.Errorf("Q=0.25 非致命事件下可用性 = %v，期望 1", got)
	}
}

// TestCorrelatedFailureKEqualsZero 事件不命中任何域时退化为独立模型。
func TestCorrelatedFailureKEqualsZero(t *testing.T) {
	ind := IndependentFailure{P: 0.01}
	cor := CorrelatedFailure{P: 0.01, Q: 0.7, K: 0}
	for _, d := range []int{3, 5} {
		a := ind.QuorumAvailable(d, 2)
		b := cor.QuorumAvailable(d, 2)
		if math.Abs(a-b) > 1e-12 {
			t.Errorf("d=%d: K=0 时应退化为独立模型，%.15f != %.15f", d, a, b)
		}
	}
}

// TestCorrelatedFailureKCoversAllDomains 事件命中全部域时必然不可用。
func TestCorrelatedFailureKCoversAllDomains(t *testing.T) {
	cor := CorrelatedFailure{P: 0, Q: 1.0, K: 5}
	if got := cor.QuorumAvailable(5, 1); got != 0 {
		t.Errorf("全部域被命中时可用性 = %v，期望 0", got)
	}
}

// TestHierarchicalFailureOrdering 层级越多、单元越大，可用性越低。
func TestHierarchicalFailureOrdering(t *testing.T) {
	// 单层：每域独立
	flat := HierarchicalFailure{Tiers: []TierFailure{{Name: "zone", Prob: 0.001, UnitSize: 1}}}
	// 两层：dc 层把 5 个域分成 1 个单元（全挂）
	clustered := HierarchicalFailure{Tiers: []TierFailure{
		{Name: "dc", Prob: 0.001, UnitSize: 5},
	}}

	flatA := flat.QuorumAvailable(5, 1)
	clusA := clustered.QuorumAvailable(5, 1)

	if !(clusA <= flatA) {
		t.Errorf("整簇同挂的可用性 %.9f 未低于逐域独立 %.9f", clusA, flatA)
	}
	if clustered.QuorumAvailable(5, 1) == 0 {
		t.Error("整簇同挂时可用性不应为 0（事件概率只有 1e-3）")
	}
	t.Logf("5 域容忍1: 逐域独立 %.9f  整簇同挂 %.9f", flatA, clusA)
}

// TestHierarchicalEmpty 空层级必须返回 1（无失效来源）。
func TestHierarchicalEmpty(t *testing.T) {
	h := HierarchicalFailure{}
	if got := h.QuorumAvailable(5, 3); got != 1 {
		t.Errorf("空层级可用性 = %v，期望 1", got)
	}
}

// TestCompareFailureModels 比较函数应以首个模型为基准计算高估量。
func TestCompareFailureModels(t *testing.T) {
	ind := IndependentFailure{P: 0.001}
	cor := CorrelatedFailure{P: 0.001, Q: 1e-4, K: 2}

	out := CompareFailureModels(5, 4, ind, cor) // 需要 4/5 个域在线
	if len(out) != 2 {
		t.Fatalf("结果数 = %d，期望 2", len(out))
	}
	if out[0].Model != "independent" || out[1].Model != "correlated" {
		t.Fatalf("模型顺序错误: %v", []string{out[0].Model, out[1].Model})
	}
	if out[0].OverestimateVsBase != 0 {
		t.Errorf("基准模型的高估量应为 0，实际 %v", out[0].OverestimateVsBase)
	}
	if out[1].OverestimateVsBase <= 0 {
		t.Errorf("相关模型应有正的高估量，实际 %v", out[1].OverestimateVsBase)
	}
	// Unavailable = 1 - Available。
	//
	// 容差用相对值：当 Available 极接近 1 时，1-Available 的绝对值本身
	// 就接近 double 在该量级上的分辨率，用绝对容差会误报。
	for _, m := range out {
		diff := math.Abs(m.Unavailable - (1 - m.Available))
		tol := 1e-12
		if m.Available > 1-1e-6 {
			tol = 1e-15
		}
		if diff > tol {
			t.Errorf("%s: Unavailable(%.20f) 与 1-Available(%.20f) 相差 %.2e",
				m.Model, m.Unavailable, 1-m.Available, diff)
		}
	}
}

// TestQuorumAvailabilityByPlacement 同一组 quorum 在不同放置下可用性不同。
//
// 这正是 FAFT"联合优化 quorum 几何与落点"的立论基础：
// 如果放置不影响可用性，这个联合问题就不存在。
func TestQuorumAvailabilityByPlacement(t *testing.T) {
	model := CorrelatedFailure{P: 0.001, Q: 1e-3, K: 2}

	// 3 个副本：一种放置摊到 3 个域，另一种挤在 1 个域。
	spread := Placement{0, 1, 2}
	packed := Placement{0, 0, 0}

	q := &Quorum{Q2Members: []int{0, 1, 2}}

	spreadA := QuorumAvailability(q, spread, model)
	packedA := QuorumAvailability(q, packed, model)

	if !(spreadA > packedA) {
		t.Errorf("摊开到 3 域的可用性 %.9f 未高于挤在 1 域的 %.9f", spreadA, packedA)
	}
	if packedA != 0 {
		t.Errorf("3 个副本全在同一域时，该域失效即不可用，可用性应为 0，实际 %.9f", packedA)
	}
	t.Logf("同一 quorum 不同放置：摊开 %.9f  挤一起 %.9f", spreadA, packedA)
}

// TestQuorumAvailabilityNil 空输入不应 panic。
func TestQuorumAvailabilityNil(t *testing.T) {
	m := IndependentFailure{P: 0.01}
	if got := QuorumAvailability(nil, Placement{0}, m); got != 0 {
		t.Errorf("nil quorum = %v，期望 0", got)
	}
	if got := QuorumAvailability(&Quorum{Q2Members: []int{0}}, Placement{0}, nil); got != 0 {
		t.Errorf("nil model = %v，期望 0", got)
	}
	if got := QuorumAvailability(&Quorum{}, Placement{0}, m); got != 0 {
		t.Errorf("空 quorum = %v，期望 0", got)
	}
}

// ---------------------------------------------------------------------------
// 数值工具
// ---------------------------------------------------------------------------

// tailPMF 返回 P[X > m]，X ~ Binomial(n, failProb)，用**解析的上尾**
// 在 log 空间逐项累加。
//
// 为什么用"上尾"而不是"P[X <= m] 的直和"：
//   - m 接近均值时 P[X <= m] 约 0.5，直和各项量级温和；
//   - m 远低于均值时可用性接近 0，log 空间累加仍能给出正确的极小值；
//   - 关键是它不会像朴素求和那样在 i≈np 处把项数成 0。
//
// 这是测试里独立的第二实现，用于交叉验证 binomTail / survivalProbability。
func tailPMF(n int, m int, failProb float64) float64 {
	if m < 0 {
		return 1
	}
	if m >= n {
		return 0
	}
	logP := math.Log(failProb)
	log1mP := math.Log(1 - failProb)
	logSum := math.Inf(-1)
	for i := m + 1; i <= n; i++ {
		lt := logBinom(n, i) + float64(i)*logP + float64(n-i)*log1mP
		logSum = logAdd(logSum, lt)
	}
	if math.IsInf(logSum, -1) {
		return 0
	}
	return math.Exp(logSum)
}

// pct 把概率格式化为人类可读的百分比，极小值用科学计数法。
func pct(v float64) string {
	if v == 0 {
		return "0%"
	}
	if v > 1e-4 {
		return fmt.Sprintf("%.6f%%", v*100)
	}
	return fmt.Sprintf("%.4g%%", v*100)
}

// TestBinomTailSumEqualsOne 二项分布尾部之和必须为 1（概率归一化）。
func TestBinomTailSumEqualsOne(t *testing.T) {
	for _, n := range []int{5, 20, 100} {
		for _, p := range []float64{1e-4, 0.01, 0.5, 0.99} {
			sum := 0.0
			for k := 0; k <= n; k++ {
				sum += binomPMF(n, k, p)
			}
			if math.Abs(sum-1) > 1e-9 {
				t.Errorf("n=%d p=%g: PMF 之和 = %.12f，期望 1", n, p, sum)
			}
		}
	}
}

// TestBinomTailMonotonic 需要更多副本在线时可用性必须下降。
func TestBinomTailMonotonic(t *testing.T) {
	const n, p = 21, 0.01
	prev := 2.0
	for need := 1; need <= n; need++ {
		v := binomTail(n, need, p)
		if v > prev+1e-15 {
			t.Errorf("need=%d 时可用性 %.15f 高于 need=%d 的 %.15f", need, v, need-1, prev)
		}
		prev = v
	}
}

// TestAvailabilityBoundConsistency 域粒度与副本粒度的独立模型应能互相印证。
//
// 等价设定：每个域放一个副本，且域失效率等于副本失效率。
// 此时"允许最多 tol 个域失效"等价于"需要至少 d-tol 个副本在线"。
//
// ⚠️ 这里的参数对应关系极易写错：AvailabilityBound 的第二个参数是
// **容错度 tol**，而 AvailabilityAtReplicaLevel 的第二个参数是
// **需要的最少在线数 q**，两者关系是 q = d - tol。
// 初版测试误把 tol 写成了 d-q（即把 q 当 tol 用），导致"两个模型不一致"的
// 假警报 —— 实际是两个函数本来就在算不同的量。
func TestAvailabilityBoundConsistency(t *testing.T) {
	const domains, p = 5, 0.01

	for _, tol := range []int{0, 1, 2, 3, 4} {
		q := domains - tol // 需要的最少在线数
		// 域粒度：允许最多 tol 个域失效
		domainLevel := AvailabilityBound(domains, tol, p)
		// 副本粒度：需要至少 q 个副本在线
		replicaLevel := AvailabilityAtReplicaLevel(domains, q, p)

		if math.Abs(domainLevel-replicaLevel) > 1e-12 {
			t.Errorf("tol=%d (q=%d): 域粒度 %.15g 与副本粒度 %.15g 不一致",
				tol, q, domainLevel, replicaLevel)
		}
	}

	// 边界核对：tol=d 时应为 1（允许全部域失效）。
	if got := AvailabilityBound(domains, domains, p); got != 1 {
		t.Errorf("容忍全部域失效时可用性 = %v，期望 1", got)
	}
	// 要求全部在线 => (1-p)^d
	allAlive := AvailabilityAtReplicaLevel(domains, domains, p)
	if math.Abs(allAlive-math.Pow(1-p, domains)) > 1e-12 {
		t.Errorf("要求全部在线时可用性 = %.15g，期望 %.15g", allAlive, math.Pow(1-p, domains))
	}
}
