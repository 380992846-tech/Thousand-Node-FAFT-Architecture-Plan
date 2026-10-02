// pkg/faft/ladder_test.go
//
// availabilityLadder 是一次**严格等价**的性能重排：把"对每个 k 各跑一次贪心"
// 换成"跑一次嵌套贪心，取前 k 项"。等价性不是显然的，所以必须有测试。
//
// 它替换掉的成本很高：SolveWithFailures 里 Q2 每个 k 求一次、Q1 每个大小求一次，
// 合计 O(n³) 次可用性计算。实测 n=21 时单次求解 0.37 s，
// 而联合求解要对上千个候选放置各调用一次。
package faft

import (
	"math"
	"math/rand"
	"testing"
)

func TestAvailabilityLadderMatchesPerKGreedy(t *testing.T) {
	rng := rand.New(rand.NewSource(20261003))

	for trial := 0; trial < 40; trial++ {
		D := 2 + rng.Intn(8)
		n := 1 + rng.Intn(8)
		topo := unevenTopology(D, math.Pow(10, -4+3*rng.Float64()), 3*rng.Float64())
		topo.RegionalEventProb = 1e-3 * float64(rng.Intn(20))
		if topo.RegionalEventProb > 0 {
			topo.RegionalEventDomains = 1 + rng.Intn(D)
		}
		model := CorrelatedFailure{P: 1e-3, Q: topo.RegionalEventProb, K: topo.RegionalEventDomains}
		av := AvailabilityModel{Model: model, Topology: topo}

		placement := make(Placement, n)
		for i := range placement {
			placement[i] = rng.Intn(D)
		}
		all := make([]int, n)
		for i := range all {
			all[i] = i
		}

		ladder := buildAvailabilityLadder(all, placement, av)
		if len(ladder.order) != n {
			t.Fatalf("阶梯长度 %d ≠ n=%d", len(ladder.order), n)
		}

		for k := 1; k <= n; k++ {
			wantMem := pickMaxAvailability(all, placement, av, k)
			gotMem := ladder.quorum(k)
			if len(gotMem) != len(wantMem) {
				t.Fatalf("k=%d：成员数 %d ≠ %d", k, len(gotMem), len(wantMem))
			}
			// 顺序也要求一致：旧实现 k ≥ n 时返回原序列而非贪心顺序，
			// 而成员顺序会出现在结果 JSON 与诊断字符串里。
			for i := range wantMem {
				if gotMem[i] != wantMem[i] {
					t.Fatalf("k=%d：成员集合不同\n 阶梯 %v\n 逐k %v", k, gotMem, wantMem)
				}
			}
			wantAvail := av.availOf(wantMem, placement)
			gotAvail := ladder.availAt(k)
			if math.Abs(wantAvail-gotAvail) > 1e-15 {
				t.Fatalf("k=%d：可用性 %.17g ≠ %.17g", k, gotAvail, wantAvail)
			}
		}
	}
}

// TestSolveWithFailuresUnaffectedByLadder 直接钉住被替换的函数：
// 两种成员选择路径必须给出同一个 FailurePlan。
func TestSolveWithFailuresUnaffectedByLadder(t *testing.T) {
	topo := unevenTopology(9, 1e-3, 3.0)
	topo.RegionalEventProb = 1e-3
	topo.RegionalEventDomains = 2
	av := AvailabilityModel{
		Model:    CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 2},
		Topology: topo,
	}
	targets := DefaultTargets()

	for _, n := range []int{1, 3, 5, 9, 11} {
		placement := defaultPlacement(n, len(topo.Domains))
		cfg := Config{Replicas: n, Placement: placement}

		got := SolveWithFailures(topo, cfg, av, targets)
		want := solveWithFailuresGreedyOnly(topo, cfg, av, targets)
		if got.WriteMsgsPerOp != want.WriteMsgsPerOp ||
			got.Meets != want.Meets ||
			got.DataAvailability != want.DataAvailability ||
			got.ControlAvailability != want.ControlAvailability {
			t.Errorf("n=%d：阶梯版与原版不一致\n 阶梯: 消息=%d 可行=%v 数据=%.17g 控制=%.17g\n 原版: 消息=%d 可行=%v 数据=%.17g 控制=%.17g",
				n, got.WriteMsgsPerOp, got.Meets, got.DataAvailability, got.ControlAvailability,
				want.WriteMsgsPerOp, want.Meets, want.DataAvailability, want.ControlAvailability)
		}
	}
}

// TestMemberAvailabilityIsBitReproducible 钉住"同一个输入必须给出逐位相同的结果"。
//
// 修之前 availabilityForKilledSet 用 `for d, c := range perDomain` 做卷积，
// Go 的 map 遍历顺序随机，而浮点加法不满足结合律 ⇒ 同一集合两次调用结果
// 最后几位不同（实测 …85479 vs …491）。这不影响结论，但**让"同种子 ⇒ 同结果"
// 在解析结果上都做不到**，而论文里的数字是要被人拿去复算的。
func TestMemberAvailabilityIsBitReproducible(t *testing.T) {
	topo := unevenTopology(9, 1e-3, 3.0)
	topo.RegionalEventProb = 2e-3
	topo.RegionalEventDomains = 3
	av := AvailabilityModel{
		Model:    CorrelatedFailure{P: 1e-3, Q: 2e-3, K: 3},
		Topology: topo,
	}
	placement := defaultPlacement(7, len(topo.Domains))
	members := []int{0, 1, 2, 3, 4, 5, 6}

	first := av.availOf(members, placement)
	for i := 0; i < 200; i++ {
		if got := av.availOf(members, placement); got != first {
			t.Fatalf("第 %d 次调用得到 %.17g，首次为 %.17g —— 可用性计算不可逐位复现"+
				"（检查是否又在遍历 map）", i+1, got, first)
		}
	}
}

// solveWithFailuresGreedyOnly 是 SolveWithFailures 的**参考实现**，
// 逐 k 调用 pickMaxAvailability（改动前的写法），只用于对照测试。
func solveWithFailuresGreedyOnly(
	t *Topology, cfg Config, av AvailabilityModel, targets AvailabilityTargets,
) FailurePlan {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas
	all := make([]int, n)
	for i := range all {
		all[i] = i
	}

	var bestPlan *Plan
	bestData, bestCtrl := -1.0, 0.0
	consider := func(p Plan, d, c float64) {
		if d > bestData || (d == bestData && c > bestCtrl) {
			cp := p
			bestPlan, bestData, bestCtrl = &cp, d, c
		}
	}

	for k := 1; k <= n; k++ {
		q2mem := pickMaxAvailability(all, cfg.Placement, av, k)
		dataAvail := av.availOf(q2mem, cfg.Placement)
		q1min := n - k + 1
		if q1min < 1 {
			q1min = 1
		}
		if q1min <= n {
			q1memMin := pickMaxAvailability(all, cfg.Placement, av, q1min)
			consider(Evaluate(t, cfg, Quorum{Q1Members: q1memMin, Q2Members: q2mem}),
				dataAvail, av.availOf(q1memMin, cfg.Placement))
		}
		if dataAvail < targets.Data {
			continue
		}
		for q1size := q1min; q1size <= n; q1size++ {
			q1mem := pickMaxAvailability(all, cfg.Placement, av, q1size)
			ctrlAvail := av.availOf(q1mem, cfg.Placement)
			p := Evaluate(t, cfg, Quorum{Q1Members: q1mem, Q2Members: q2mem})
			consider(p, dataAvail, ctrlAvail)
			fp := FailurePlan{
				Plan:                p,
				DataAvailability:    dataAvail,
				ControlAvailability: ctrlAvail,
				MeetsData:           dataAvail >= targets.Data,
				MeetsControl:        ctrlAvail >= targets.Control,
				FailureModel:        av.Model.Name(),
			}
			fp.Meets = fp.MeetsData && fp.MeetsControl
			if fp.Meets {
				return fp
			}
		}
	}
	if bestPlan == nil {
		return FailurePlan{}
	}
	return FailurePlan{
		Plan:                *bestPlan,
		DataAvailability:    bestData,
		ControlAvailability: bestCtrl,
		MeetsData:           bestData >= targets.Data,
		MeetsControl:        bestCtrl >= targets.Control,
		FailureModel:        av.Model.Name(),
	}
}
