// pkg/faft/lowerbound_test.go
//
// 这是整个下界工作的**守门测试**。
//
// 下界只有在"上界确实是上界"的前提下才成立。一旦某一步松弛写成了
// "某个具体放置下的可用性"，得到的数就会**低于**真实可达值，
// 于是报出去变成"我们的解超过了理论最优" —— 一个看起来漂亮、
// 实则自相矛盾的结论。
//
// 所以这里对**随机拓扑 × 随机放置 × 每个 quorum 大小**逐点验证：
//
//	真实可用性 ≤ AvailabilityUpperBound(q)
//
// 只要这条对任意放置都成立，下界就是可证的。
package faft

import (
	"math"
	"math/rand"
	"testing"
)

// actualAvailability 用真实的可用性计算入口（memberAvailabilityOn），
// 对给定放置与成员集合求值。刻意不复用任何下界代码 ——
// 两边共用实现就测不出"上界写低了"。
func actualAvailability(t *Topology, model FailureModel, placement Placement, q int) float64 {
	members := make([]int, q)
	for i := range members {
		members[i] = i
	}
	return memberAvailabilityOn(members, t, placement, model, 0)
}

// TestUpperBoundIsSound 对随机拓扑逐点验证上界性质。
func TestUpperBoundIsSound(t *testing.T) {
	rng := rand.New(rand.NewSource(20261002))

	// D 覆盖两个分支：D ≤ 12 走精确分拆（命中集合为全部 C(D,k) 组合），
	// D > 12 走解析界（轮转采样）。两条路都必须守住"上界"。
	for trial := 0; trial < 90; trial++ {
		D := 2 + rng.Intn(19) // 2..20 个域
		n := 1 + rng.Intn(9)  // 1..9 个副本
		pBase := math.Pow(10, -4+3*rng.Float64())
		skew := 4 * rng.Float64()
		q := 0.0
		k := 1
		if rng.Float64() < 0.7 {
			q = math.Pow(10, -4+3*rng.Float64())
			k = 1 + rng.Intn(D) // 含 k = D-1；k = D 单独在下面测
		}

		topo := unevenTopology(D, pBase, skew)
		topo.RegionalEventProb = q
		topo.RegionalEventDomains = k
		model := CorrelatedFailure{P: pBase, Q: q, K: k}

		// k = D（事件打掉全部域）也要覆盖：此时事件项必须为 0。
		ks := []int{k, D}
		for _, kk := range ks {
			topo.RegionalEventDomains = kk
			model = CorrelatedFailure{P: pBase, Q: q, K: kk}

			// 随机若干种放置
			for pt := 0; pt < 6; pt++ {
				placement := make(Placement, n)
				for i := range placement {
					placement[i] = rng.Intn(D)
				}
				for sz := 1; sz <= n; sz++ {
					actual := actualAvailability(topo, model, placement, sz)
					ub := AvailabilityUpperBound(topo, model, sz)
					if actual > ub+1e-12 {
						t.Fatalf("上界不成立：D=%d n=%d pBase=%.2e skew=%.2f q=%.2e k=%d\n"+
							"  放置=%v 大小=%d\n"+
							"  真实可用性 = %.12f\n"+
							"  上界       = %.12f\n"+
							"  真实值超出上界 %.3e —— 某一步松弛写反了方向",
							D, n, pBase, skew, q, kk, placement, sz, actual, ub, actual-ub)
					}
				}
			}
		}
	}
}

// TestUpperBoundMonotoneInEventSeverity 确认上界随区域事件**严重程度**单调不增。
//
// ⚠️ 原先这里写的是"随 quorum 大小单调不减"，那是**错的**：
// need = ⌊q/2⌋+1 涨得比 q 快，q=1 时 need=1 而 q=2 时 need=2，
// 真实可用性本身就会下降（单副本活着 vs 两个都活着），上界跟着降是对的。
// 见 TestUpperBoundTracksNonMonotoneInQ。
//
// 真正成立的性质是：事件同时打掉的域越多，可用性上界越低。
// 证明：φ 单调不增，对任意成员数分拆，K+1 命中集合的成员数总和
// 逐点不小于其任一 K 子集，取期望后 E_{K+1} ≤ E_K；对分拆取 max 保序。
func TestUpperBoundMonotoneInEventSeverity(t *testing.T) {
	for _, D := range []int{9, 20} { // 9 走精确分拆，20 走解析界
		topo := unevenTopology(D, 1e-3, 3.0)
		topo.RegionalEventProb = 1e-3
		model := CorrelatedFailure{P: 1e-3, Q: 1e-3}

		prev := 1.0
		for k := 1; k <= D; k++ {
			topo.RegionalEventDomains = k
			model.K = k
			ub := AvailabilityUpperBound(topo, model, 5)
			t.Logf("D=%d k=%d 上界=%.12f", D, k, ub)
			if ub > prev+1e-12 {
				t.Errorf("D=%d：事件越严重上界反而越大：k=%d 时 %.12f > k=%d 时 %.12f",
					D, k, ub, k-1, prev)
			}
			prev = ub
		}
	}
}

// TestUpperBoundTracksNonMonotoneInQ 记录"可用性随 quorum 大小非单调"这件事，
// 防止有人再把它当成 bug 去"修"。
func TestUpperBoundTracksNonMonotoneInQ(t *testing.T) {
	topo := unevenTopology(9, 1e-3, 3.0)
	topo.RegionalEventProb = 0
	topo.RegionalEventDomains = 0
	model := CorrelatedFailure{P: 1e-3}

	q1 := AvailabilityUpperBound(topo, model, 1)
	q2 := AvailabilityUpperBound(topo, model, 2)
	t.Logf("q=1 上界=%.12f，q=2 上界=%.12f", q1, q2)
	if q2 >= q1 {
		t.Errorf("q=2（need=2，必须两个都活）的可用性上界不应 ≥ q=1（need=1）："+
			"%.12f vs %.12f —— 若这条变了，说明 need 的定义被改动", q2, q1)
	}
}

// TestUpperBoundWithoutEvents 无区域事件时，上界应退化为
// "所有成员都在最可靠域、逐副本独立"的纯二项式。
func TestUpperBoundWithoutEvents(t *testing.T) {
	topo := unevenTopology(9, 1e-3, 3.0)
	topo.RegionalEventProb = 0
	topo.RegionalEventDomains = 0
	model := CorrelatedFailure{P: 1e-3, Q: 0, K: 0}

	pMin := math.Inf(1)
	for i := range topo.Domains {
		if topo.Domains[i].FailProb < pMin {
			pMin = topo.Domains[i].FailProb
		}
	}
	for q := 1; q <= 9; q++ {
		want := survivalProbability(q, q/2+1, pMin)
		got := AvailabilityUpperBound(topo, model, q)
		if math.Abs(got-want) > 1e-12 {
			t.Errorf("q=%d：无事件时上界应为 %.12f，实际 %.12f", q, want, got)
		}
	}
}

// TestLowerBoundIsAttainable 是本文件第二个重点：
// **下界必须是可达的，不能高于真实最优。**
//
// 做法：在小规模上穷举所有放置与 quorum 组合，求出真实最优消息数，
// 断言 `下界 ≤ 真实最优`。若下界高于真实最优，那它就不是下界。
func TestLowerBoundIsAttainable(t *testing.T) {
	cases := []struct {
		name    string
		D, n    int
		skew    float64
		q       float64
		k       int
		targets AvailabilityTargets
	}{
		{"D5-n3-无事件", 5, 3, 2.0, 0, 1, AvailabilityTargets{Data: 0.999, Control: 0.999}},
		{"D5-n3-弱事件", 5, 3, 2.0, 1e-3, 2, AvailabilityTargets{Data: 0.999, Control: 0.999}},
		{"D6-n4-强离散", 6, 4, 3.0, 1e-3, 2, AvailabilityTargets{Data: 0.999, Control: 0.999}},
		{"D4-n3-无事件", 4, 3, 1.0, 0, 1, AvailabilityTargets{Data: 0.99, Control: 0.99}},
		{"D7-n3-宽目标", 7, 3, 2.5, 1e-4, 1, AvailabilityTargets{Data: 0.99, Control: 0.99}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			topo := unevenTopology(c.D, 1e-3, c.skew)
			topo.RegionalEventProb = c.q
			topo.RegionalEventDomains = c.k
			model := CorrelatedFailure{P: 1e-3, Q: c.q, K: c.k}
			cfg := Config{Replicas: c.n, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
			av := AvailabilityModel{Model: model, Topology: topo}

			ub := ComputeLowerBound(topo, cfg, model, c.targets)
			opt := ExhaustiveJoint(topo, cfg, av, c.targets)
			optFeasible := opt.MeetsData && opt.MeetsControl

			t.Logf("下界：可行=%v |Q2|=%d 消息=%d", ub.Feasible, ub.Q2LB, ub.WriteMsgsLB)
			t.Logf("穷举最优：可行=%v |Q2|=%d 消息=%d 放置=%v",
				optFeasible, len(opt.Quorum.Q2Members), opt.WriteMsgsPerOp, opt.Placement)

			switch {
			case !optFeasible && ub.Feasible:
				// 下界说可行、穷举说不可行 —— 这只可能是"界太松"，
				// 不是矛盾（松弛允许了真实做不到的事）。允许，但要记录。
				t.Logf("注：松弛问题可行而穷举不可行 —— 界偏松，符合预期方向")
			case optFeasible && !ub.Feasible:
				t.Errorf("❌ 穷举找到了可行解（消息=%d），而下界说连松弛都不可行。"+
					"下界不可达，必须修。", opt.WriteMsgsPerOp)
			case optFeasible && ub.Feasible:
				if ub.WriteMsgsLB > opt.WriteMsgsPerOp {
					t.Errorf("❌ 下界消息数(%d) > 穷举真实最优(%d)：下界不可达",
						ub.WriteMsgsLB, opt.WriteMsgsPerOp)
				} else {
					t.Logf("✅ 下界 %d ≤ 真实最优 %d，间隔 %d 条消息",
						ub.WriteMsgsLB, opt.WriteMsgsPerOp, opt.WriteMsgsPerOp-ub.WriteMsgsLB)
				}
			}
		})
	}
}

// bruteForceOptimum 是全套方案的**黄金标准**：
// 枚举所有放置 × 所有成员子集 × 所有 (q1,q2) 组合，求出真实可达的最小写消息数。
//
// 与 ExhaustiveJoint 的区别：那个只枚举放置（成员集合固定取前 q 个），
// 这里连"哪些成员进 quorum"也一起枚举，因此它是更强的对照物 ——
// 下界只要越过它一次，就是下界错了。
func bruteForceOptimum(topo *Topology, cfg Config, model FailureModel, targets AvailabilityTargets) (int, bool) {
	D := len(topo.Domains)
	n := cfg.Replicas
	placement := make(Placement, n)
	best, feasible := 0, false

	var rec func(i int)
	rec = func(i int) {
		if i == n {
			// avail[q]：本放置下，规模为 q 的成员集合能达到的最优可用性
			avail := make([]float64, n+1)
			for mask := 1; mask < 1<<n; mask++ {
				var members []int
				for b := 0; b < n; b++ {
					if mask&(1<<b) != 0 {
						members = append(members, b)
					}
				}
				q := len(members)
				if a := memberAvailabilityOn(members, topo, placement, model, 0); a > avail[q] {
					avail[q] = a
				}
			}
			for q2 := 1; q2 <= n; q2++ {
				if avail[q2] < targets.Data {
					continue
				}
				for q1 := n - q2 + 1; q1 <= n; q1++ {
					if q1 < 1 || avail[q1] < targets.Control {
						continue
					}
					if !feasible || 2*q2 < best {
						best, feasible = 2*q2, true
					}
					break
				}
			}
			return
		}
		for d := 0; d < D; d++ {
			placement[i] = d
			rec(i + 1)
		}
	}
	rec(0)
	return best, feasible
}

// TestLowerBoundSoundAgainstBruteForce 是下界的**最强守门测试**：
// 下界 ≤ 穷举出的真实最优（对放置、成员集合、quorum 大小三者一起取最优）。
//
// 只要下界越过一次，就意味着我们把一个真实做不到的方案说成了"低于理论最优"。
func TestLowerBoundSoundAgainstBruteForce(t *testing.T) {
	cases := []struct {
		name    string
		D, n    int
		skew    float64
		q       float64
		k       int
		targets AvailabilityTargets
	}{
		{"D4-n3-无事件", 4, 3, 2.0, 0, 1, AvailabilityTargets{Data: 0.99, Control: 0.99}},
		{"D4-n3-区域事件", 4, 3, 2.5, 1e-2, 2, AvailabilityTargets{Data: 0.99, Control: 0.99}},
		{"D5-n3-强离散", 5, 3, 3.0, 1e-3, 2, AvailabilityTargets{Data: 0.999, Control: 0.999}},
		{"D3-n4-宽目标", 3, 4, 2.0, 5e-3, 1, AvailabilityTargets{Data: 0.95, Control: 0.95}},
		{"D4-n4-紧目标", 4, 4, 3.5, 2e-2, 2, AvailabilityTargets{Data: 0.9999, Control: 0.9999}},
	}
	tight := 0
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			topo := unevenTopology(c.D, 1e-3, c.skew)
			topo.RegionalEventProb = c.q
			topo.RegionalEventDomains = c.k
			model := CorrelatedFailure{P: 1e-3, Q: c.q, K: c.k}
			cfg := Config{Replicas: c.n, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}

			lb := ComputeLowerBound(topo, cfg, model, c.targets)
			opt, ok := bruteForceOptimum(topo, cfg, model, c.targets)
			t.Logf("下界：可行=%v |Q2|=%d 消息=%d ｜ 穷举最优：可行=%v 消息=%d",
				lb.Feasible, lb.Q2LB, lb.WriteMsgsLB, ok, opt)

			if ok && !lb.Feasible {
				t.Fatalf("❌ 穷举找到了可行方案（消息=%d），而下界说连松弛都不可行", opt)
			}
			if ok && lb.Feasible {
				if lb.WriteMsgsLB > opt {
					t.Fatalf("❌ 下界 %d > 真实最优 %d：下界不可达", lb.WriteMsgsLB, opt)
				}
				if lb.WriteMsgsLB == opt {
					tight++
				}
			}
		})
	}
	t.Logf("下界取到真实最优的用例：%d/%d", tight, len(cases))
}

// TestCompareToLowerBoundWithMatches 确认"预计算下界"的快路径与
// CompareToLowerBound 完全一致 —— 拆这一层是为性能（一张表要对同一个
// 参数点比 N 个 planner），但两条路必须给出同一个数。
func TestCompareToLowerBoundWithMatches(t *testing.T) {
	topo := unevenTopology(9, 1e-3, 3.0)
	topo.RegionalEventProb = 1e-3
	topo.RegionalEventDomains = 2
	model := CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 2}
	cfg := Config{Replicas: 5, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
	av := AvailabilityModel{Model: model, Topology: topo}
	targets := DefaultTargets()

	lb := ComputeLowerBound(topo, cfg, model, targets)
	for _, p := range DefaultBaselines(targets) {
		plan := p.Plan(topo, cfg, av)
		slow := CompareToLowerBound(topo, cfg, av, targets, plan)
		fast := CompareToLowerBoundWith(lb, plan)
		if slow != fast {
			t.Errorf("%s：两条路结果不一致\n slow=%+v\n fast=%+v", p.Name(), slow, fast)
		}
	}
}
func TestCompareToLowerBoundFlagsOptimal(t *testing.T) {
	topo := unevenTopology(9, 1e-3, 3.0)
	topo.RegionalEventProb = 1e-3
	topo.RegionalEventDomains = 2
	model := CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 2}
	cfg := Config{Replicas: 5, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
	av := AvailabilityModel{Model: model, Topology: topo}
	targets := DefaultTargets()

	joint := SolveJoint(topo, cfg, av, targets)
	g := CompareToLowerBound(topo, cfg, av, targets, joint.FailurePlan)
	t.Log(FormatOptimalityGap("joint", g))

	if g.LBInfeasible {
		t.Fatal("松弛问题应当可行")
	}
	if g.SolverMsgs < g.LBMsgs {
		t.Errorf("❌ 求解器消息数(%d) 低于下界(%d) —— 这说明上界不成立，"+
			"下界是错的（不是求解器更优）", g.SolverMsgs, g.LBMsgs)
	}
	if g.GapMsgs != g.SolverMsgs-g.LBMsgs {
		t.Errorf("差距计算不一致")
	}
}

// TestCompareToLowerBoundFlagsOptimal 确认"达到下界"能被正确识别。
// 见本文件末尾的对照测试。

// TestUpperBoundHandlesDegenerateInputs 边界输入不应 panic。
func TestUpperBoundHandlesDegenerateInputs(t *testing.T) {
	// 无域
	if got := AvailabilityUpperBound(nil, nil, 3); got < 0 || got > 1 {
		t.Errorf("nil 拓扑应返回 [0,1] 内的值，实际 %v", got)
	}
	// q=0
	topo := unevenTopology(3, 1e-3, 1)
	if got := AvailabilityUpperBound(topo, CorrelatedFailure{P: 1e-3}, 0); got != 1 {
		t.Errorf("q=0 应返回 1，实际 %v", got)
	}
	// 副本数超过域数（必然堆叠）
	topo2 := unevenTopology(2, 1e-3, 1)
	got := AvailabilityUpperBound(topo2, CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 1}, 5)
	if got < 0 || got > 1 {
		t.Errorf("q > 域数时应返回 [0,1] 内的值，实际 %v", got)
	}
	t.Logf("q=5, D=2 时上界=%.9f", got)
}
