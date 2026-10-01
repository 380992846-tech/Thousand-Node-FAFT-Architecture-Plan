package faft

import (
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// 成员集合可用性：这些是本轮修复的两个建模错误的第一道防线
// ---------------------------------------------------------------------------

// TestMemberAvailabilityBinomialWithinDomain 域内多个副本必须按二项分布卷积。
//
// 这是本轮修复的关键错误：初版把「域存活」当作 0/1 事件
// （域活 => c 个副本全活，域挂 => 0 个活），于是同域 c 个副本的
// 可用性被算成 (1-p) 而不是 (1-p)^c。
//
// 后果不是"数字略有偏差"，而是**方向性错误**：贪心会认为
// "把副本堆在最稳的域"与"跨域展开"同样好，从而系统性选中堆叠方案，
// 让"故障感知"完全失效（实测 k=3 时堆叠得 0.9999999997 而跨域只有 0.99989）。
func TestMemberAvailabilityBinomialWithinDomain(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	const p = 1e-5
	for i := range topo.Domains {
		topo.Domains[i].FailProb = p
	}
	// 5 个域，每个域 1 个副本；构造"同域多个副本"需要人为放置。
	// 这里让 5 个副本全部落在域 0。
	placement := Placement{0, 0, 0, 0, 0}

	av := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: topo}

	// 取前 2 个副本：需要 2/2+1 = 2 个存活 => 两个都必须活 => (1-p)^2
	got2 := av.availOf([]int{0, 1}, placement)
	want2 := math.Pow(1-p, 2)
	if math.Abs(got2-want2) > 1e-15 {
		t.Errorf("同域 2 副本: 得到 %.15g，期望 (1-p)^2 = %.15g", got2, want2)
	}

	// 取前 3 个：需要 3/2+1 = 2 个存活 => P[X>=2], X~Bin(3, 1-p)
	got3 := av.availOf([]int{0, 1, 2}, placement)
	want3 := 3*math.Pow(1-p, 2)*p + math.Pow(1-p, 3)
	if math.Abs(got3-want3) > 1e-15 {
		t.Errorf("同域 3 副本: 得到 %.15g，期望 %.15g", got3, want3)
	}

	// 对照组：跨域展开在"单副本失效率"模型下应更低
	crossPlacement := Placement{0, 1, 2, 3, 4}
	uniform := NewUniformTopology(5, 2.0)
	for i := range uniform.Domains {
		uniform.Domains[i].FailProb = p
	}
	avCross := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: uniform}
	gotCross := avCross.availOf([]int{0, 1}, crossPlacement)
	wantCross := math.Pow(1-p, 2) // 两个独立副本各以 (1-p) 存活
	if math.Abs(gotCross-wantCross) > 1e-15 {
		t.Errorf("跨域 2 副本: 得到 %.15g，期望 %.15g", gotCross, wantCross)
	}

	t.Logf("同域 2 副本 = %.15g ; 同域 3 副本 = %.15g ; 跨域 2 副本 = %.15g",
		got2, got3, gotCross)
}

// TestMemberAvailabilityRegionalEventCapsAvailability 区域事件决定可用性的可行上界。
//
// 关键结论（论文里有价值的一条）：在相关故障模型下，
// 可用性 = 1 − P(事件命中集合足以打掉 quorum)，且这个概率随
// **quorum 需要存活的副本数**上升而单调上升。
// 也就是说：加大 quorum 会让系统对相关故障更脆弱，而不是更健壮。
//
// 设定：5 个域，事件等概率命中 C(5,3)=10 种三域组合之一；
// 单副本失效率设 0 以隔离区域事件这一个因素；副本摊在不同域上。
// 事件命中集合为 S 时，存活副本数 = |replica 域 \ S|。
//
// 逐情形计数（need = q/2+1）：
//
//	3 副本 / 3 域，need=2：|replica域 \ S| < 2 的情形 7 种
//	  （S 含 2 个 replica 域：C(3,2)C(2,1)=6；S 含 3 个：C(3,3)=1）
//	  → 可用性 = 1 − 0.7q
//
//	4 副本 / 4 域，need=3：|replica域 \ S| < 3 的情形 10 种（全部）
//	  （S 含 2 个 replica 域：C(4,2)C(1,1)=6，剩 2 个存活；
//	    S 含 3 个：C(4,3)C(1,0)=4，剩 1 个；两者都 < 3）
//	  → 可用性 = 1 − q
//
//	5 副本 / 5 域，need=3：S 恰打掉 3 个域，剩 2 个 < 3
//	  → 可用性 = 1 − q
//
// 这三个数不是手推出来的 —— 初版测试我手工算错过两次（把 4/4 算成 5/10），
// 最终由 TestDebugRegionalCount 逐情形枚举确认为 7/10 与 10/10。
func TestMemberAvailabilityRegionalEventCapsAvailability(t *testing.T) {
	const q = 1e-3 // 区域事件概率
	const k = 3   // 一次打掉 3 个域

	topo := NewUniformTopology(5, 2.0)
	for i := range topo.Domains {
		topo.Domains[i].FailProb = 0 // 隔离区域事件这一个因素
	}
	topo.RegionalEventProb = q
	topo.RegionalEventDomains = k

	placement := Placement{0, 1, 2, 3, 4}
	av := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: topo}

	if n := len(topo.RegionalEventScenarios()); n != 10 {
		t.Fatalf("事件情形数 = %d，期望 C(5,3)=10", n)
	}

	cases := []struct {
		replicas  int
		killFrac  float64
		note      string
	}{
		{3, 0.7, "need=2，7/10 情形全灭"},
		{4, 1.0, "need=3，10/10 情形全灭"},
		{5, 1.0, "need=3，10/10 情形全灭"},
	}
	avails := make([]float64, 0, len(cases))
	for _, c := range cases {
		members := make([]int, c.replicas)
		for i := range members {
			members[i] = i
		}
		got := av.availOf(members, placement)
		want := 1 - c.killFrac*q
		if math.Abs(got-want) > 1e-12 {
			t.Errorf("%d 副本/%d 域：可用性 = %.15g，期望 %.15g（%s）",
				c.replicas, c.replicas, got, want, c.note)
		}
		avails = append(avails, got)
		t.Logf("  %d 副本/%d 域（need=%d）= %.9g = 1-%.1fq  （%s）",
			c.replicas, c.replicas, c.replicas/2+1, got, c.killFrac, c.note)
	}

	// 单调性：可用性随 quorum 规模上升而下降（对相关故障更脆弱）。
	if !(avails[0] > avails[1] && avails[1] >= avails[2]) {
		t.Errorf("可用性应随 quorum 规模上升而下降，实际 %v", avails)
	}

	t.Logf("事件 q=%.0e 打掉 %d 个域（共 C(5,3)=10 种情形）", q, k)
	t.Logf("结论：相关故障下加大 quorum 会让系统**更脆弱** —— ")
	t.Logf("      因为 quorum 越大，需要存活的副本越多，事件越容易把它打穿。")
}

// TestRegionalEventNoSafeHarbor 区域事件不得给任何域"免死金牌"。
//
// 初版把事件命中的域固定为"下标最小的 k 个"，于是下标大的域从不被
// 事件命中，成为避风港 —— 把副本堆在那里在这个模型内就是最优。
// 实测导致"按可用性贪心"反而劣于"按域计数贪心"，与直觉相反。
//
// 正确做法是对所有命中集合取概率平均，使得每个域被命中的
// 边际概率相同。本测试检查这一性质。
func TestRegionalEventNoSafeHarbor(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	topo.RegionalEventProb = 1e-3
	topo.RegionalEventDomains = 2

	scenarios := topo.RegionalEventScenarios()
	if len(scenarios) == 0 {
		t.Fatal("未生成任何区域事件情形")
	}

	// 每个域被命中的总概率必须相同 = k/D
	hitProb := make([]float64, len(topo.Domains))
	total := 0.0
	for _, sc := range scenarios {
		total += sc.Prob
		for _, d := range sc.Domains {
			hitProb[d] += sc.Prob
		}
	}
	if math.Abs(total-1) > 1e-12 {
		t.Errorf("所有情形概率之和 = %.15g，期望 1", total)
	}
	want := float64(topo.RegionalEventDomains) / float64(len(topo.Domains))
	for d, p := range hitProb {
		if math.Abs(p-want) > 1e-12 {
			t.Errorf("域 %d 被命中概率 %.15g，期望 %.15g —— 存在免死金牌", d, p, want)
		}
	}
	t.Logf("%d 个域、事件打掉 %d 个 => %d 种情形，每域命中概率 %.4f",
		len(topo.Domains), topo.RegionalEventDomains, len(scenarios), want)
}

// TestRegionalEventFullCoverage 事件打掉全部域时与放置无关。
func TestRegionalEventFullCoverage(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	for i := range topo.Domains {
		topo.Domains[i].FailProb = 0
	}
	topo.RegionalEventProb = 0.1
	topo.RegionalEventDomains = 5 // 全部

	sc := topo.RegionalEventScenarios()
	if len(sc) != 1 {
		t.Fatalf("全量事件应只产生 1 种情形，实际 %d", len(sc))
	}
	if len(sc[0].Domains) != 5 {
		t.Fatalf("情形应覆盖 5 个域，实际 %d", len(sc[0].Domains))
	}

	av := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: topo}
	placement := Placement{0, 1, 2, 3, 4}
	got := av.availOf([]int{0, 1, 2}, placement)
	// 事件不发生（0.9）时全部副本存活 => 可用；事件发生则全灭。
	if math.Abs(got-0.9) > 1e-12 {
		t.Errorf("全量事件下可用性 = %.15g，期望 1-q = 0.9", got)
	}
}

// ---------------------------------------------------------------------------
// 求解器
// ---------------------------------------------------------------------------

// TestSolveWithFailuresMeetsTargets 求解结果必须满足给定的可用性目标。
func TestSolveWithFailuresMeetsTargets(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	for i := range topo.Domains {
		topo.Domains[i].FailProb = 1e-4
	}

	const n = 11
	placement := make(Placement, n)
	for i := range placement {
		placement[i] = i % len(topo.Domains)
	}

	av := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: topo}
	targets := AvailabilityTargets{Data: 0.999, Control: 0.999}

	fp := SolveWithFailures(topo, Config{Replicas: n, Placement: placement}, av, targets)

	q1, q2 := fp.Quorum.Size()
	if !fp.Safe {
		t.Errorf("求解结果不满足 |Q1|+|Q2| > N：%d + %d <= %d", q1, q2, n)
	}
	if q1+q2 <= n {
		t.Errorf("|Q1|+|Q2| = %d 未超过 N=%d", q1+q2, n)
	}
	if !fp.Meets {
		t.Errorf("未达标：数据 %.9g (目标 %.4g)，控制 %.9g (目标 %.4g)\n%s",
			fp.DataAvailability, targets.Data, fp.ControlAvailability, targets.Control, fp.Diagnostics)
	}
	if fp.WriteMsgsPerOp != 2*q2 {
		t.Errorf("写消息 %d != 2|Q2| = %d", fp.WriteMsgsPerOp, 2*q2)
	}
	t.Logf("n=%d: |Q2|=%d |Q1|=%d 写消息=%d 数据可用性=%.9g 控制可用性=%.9g",
		n, q2, q1, fp.WriteMsgsPerOp, fp.DataAvailability, fp.ControlAvailability)
}

// TestSolveWithFailuresMinimizesMessages 目标是写消息数最小，
// 因此若更小的 |Q2| 能达标，求解器不应返回更大的。
func TestSolveWithFailuresMinimizesMessages(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	for i := range topo.Domains {
		topo.Domains[i].FailProb = 1e-4
	}
	const n = 11
	placement := make(Placement, n)
	for i := range placement {
		placement[i] = i % len(topo.Domains)
	}
	av := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: topo}

	// 宽松目标：应该能用很小的 |Q2| 达标。
	loose := SolveWithFailures(topo, Config{Replicas: n, Placement: placement}, av,
		AvailabilityTargets{Data: 0.5, Control: 0.5})
	// 严格目标：需要更大的 |Q2| 或 |Q1|。
	strict := SolveWithFailures(topo, Config{Replicas: n, Placement: placement}, av,
		AvailabilityTargets{Data: 0.99999, Control: 0.99999})

	if !loose.Meets {
		t.Errorf("宽松目标未达标：%s", loose.Diagnostics)
	}
	if loose.WriteMsgsPerOp > strict.WriteMsgsPerOp {
		t.Errorf("宽松目标的写消息 %d 多于严格目标的 %d —— 求解器未最小化",
			loose.WriteMsgsPerOp, strict.WriteMsgsPerOp)
	}
	t.Logf("宽松目标 -> 写消息 %d（|Q2|=%d）；严格目标 -> 写消息 %d（|Q2|=%d）",
		loose.WriteMsgsPerOp, len(loose.Quorum.Q2Members),
		strict.WriteMsgsPerOp, len(strict.Quorum.Q2Members))
}

// TestSolveWithFailuresReportsWhenInfeasible 目标不可达时必须返回诊断而非空。
func TestSolveWithFailuresReportsWhenInfeasible(t *testing.T) {
	topo := NewUniformTopology(3, 2.0)
	for i := range topo.Domains {
		topo.Domains[i].FailProb = 0.5 // 极高的失效率
	}
	const n = 3
	placement := Placement{0, 1, 2}
	av := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: topo}

	fp := SolveWithFailures(topo, Config{Replicas: n, Placement: placement}, av,
		AvailabilityTargets{Data: 0.999999, Control: 0.999999})

	if fp.Meets {
		t.Error("失效率 0.5 下不应能满足 0.999999 的目标")
	}
	if fp.Diagnostics == "" {
		t.Error("未达标时 Diagnostics 不应为空 —— 调用方需要看到最接近的方案")
	}
	t.Logf("不可达场景的诊断：%s", fp.Diagnostics)
}

// TestComparePlacementStrategiesNeverWorse 按可用性贪心不应差于按域计数贪心。
//
// 这是"故障感知"的核心主张。若某个 |Q2| 上更差，说明贪心准则有问题。
func TestComparePlacementStrategiesNeverWorse(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	probs := []float64{1e-5, 1e-4, 1e-3, 5e-3, 1e-2}
	for i, p := range probs {
		topo.Domains[i].FailProb = p
	}
	// 含区域事件：这是"跨域展开"具备价值的必要条件。
	topo.RegionalEventProb = 1e-3
	topo.RegionalEventDomains = 3

	const n = 21
	placement := make(Placement, n)
	for i := range placement {
		placement[i] = i % len(topo.Domains)
	}

	av := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: topo}
	cmps := ComparePlacementStrategies(topo, Config{Replicas: n, Placement: placement}, av, n)
	if len(cmps) != n {
		t.Fatalf("对比结果数 = %d，期望 %d", len(cmps), n)
	}

	for _, c := range cmps {
		if c.AvailabilityBased < c.CountBasedAvailability-1e-12 {
			t.Errorf("|Q2|=%d: 按可用性贪心 %.12g 劣于按域计数贪心 %.12g（差 %.3e）",
				c.Q2Size, c.AvailabilityBased, c.CountBasedAvailability,
				c.AvailabilityBased-c.CountBasedAvailability)
		}
		if c.Gain < -1e-12 {
			t.Errorf("|Q2|=%d: Gain = %.3e 为负", c.Q2Size, c.Gain)
		}
	}

	best := SortedByGain(cmps)
	if len(best) > 0 {
		t.Logf("最大提升出现在 |Q2|=%d：%.3e（%.12g -> %.12g）",
			best[0].Q2Size, best[0].Gain,
			best[0].CountBasedAvailability, best[0].AvailabilityBased)
	}
}

// TestReplicaAliveProbPriority 逐域失效率优先于模型统一概率。
func TestReplicaAliveProbPriority(t *testing.T) {
	topo := NewUniformTopology(3, 2.0)
	topo.Domains[0].FailProb = 0.01
	topo.Domains[1].FailProb = 0 // 未设置 => 用模型值
	model := IndependentFailure{P: 0.2}

	if got := replicaAliveProb(topo, model, 0, 0); math.Abs(got-0.99) > 1e-12 {
		t.Errorf("域 0（显式 0.01）存活率 = %.15g，期望 0.99", got)
	}
	if got := replicaAliveProb(topo, model, 1, 0); math.Abs(got-0.8) > 1e-12 {
		t.Errorf("域 1（未设置）存活率 = %.15g，期望 0.8（来自模型）", got)
	}
	// 越界域索引不应 panic
	if got := replicaAliveProb(topo, model, 99, 0); got != 0.8 {
		t.Errorf("越界域存活率 = %.15g，期望回退到模型值 0.8", got)
	}
	if got := replicaAliveProb(nil, model, 0, 0); got != 0.8 {
		t.Errorf("nil 拓扑存活率 = %.15g，期望模型值 0.8", got)
	}
}
