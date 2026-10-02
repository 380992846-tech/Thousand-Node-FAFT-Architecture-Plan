// pkg/faft/ablation_test.go
//
// 消融阶梯的回归测试。
//
// 它守的是一条**方法学性质**：联合求解不应该输给自己的消融臂。
// A2（优化放置 + majority 几何）与 A3（默认放置 + FAFT 几何）的解都在
// 联合搜索的可行域内，所以 A4 至少要能找回它们 —— 出现"A4 更差"
// 一定是搜索有缺陷，而不是方法性质。
package faft

import (
	"testing"
)

// TestAblationLadderArms 确认四臂都在、且各自满足自己的定义：
//
//	A1：默认放置 + majority 几何
//	A2：majority 几何（放置可优化）
//	A3：默认放置（几何可优化）
//	A4：两个都自由
func TestAblationLadderArms(t *testing.T) {
	topo := unevenTopology(9, 1e-3, 3.0)
	cfg := Config{Replicas: 5, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
	av := AvailabilityModel{
		Model:    CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 2},
		Topology: topo,
	}
	arms := AblationLadder(topo, cfg, av, DefaultTargets())
	if len(arms) != 4 {
		t.Fatalf("应当返回 4 臂，实际 %d", len(arms))
	}
	byKey := map[string]AblationArm{}
	for _, a := range arms {
		byKey[a.Key] = a
		t.Logf("%s %-15s 放置=[%v] |Q1|=%d |Q2|=%d 消息=%d 可行=%v 裕度=%+.3e",
			a.Key, a.Name, a.Placement, a.Q1Size, a.Q2Size,
			a.WriteMsgs, a.Feasible, a.MinMargin)
	}

	maj := cfg.Replicas/2 + 1
	def := defaultPlacement(cfg.Replicas, len(topo.Domains))

	// A1 必须是默认放置 + majority 几何
	if a := byKey["A1"]; a.Q1Size != maj || a.Q2Size != maj {
		t.Errorf("A1 应当用 majority 几何 (%d,%d)，实际 (%d,%d)", maj, maj, a.Q1Size, a.Q2Size)
	}
	if a := byKey["A1"]; !samePlacement(a.Placement, def) {
		t.Errorf("A1 应当是默认放置 %v，实际 %v", def, a.Placement)
	}
	// A2 必须是 majority 几何
	if a := byKey["A2"]; a.Q1Size != maj || a.Q2Size != maj {
		t.Errorf("A2 应当用 majority 几何 (%d,%d)，实际 (%d,%d)", maj, maj, a.Q1Size, a.Q2Size)
	}
	// A3 必须是默认放置
	if a := byKey["A3"]; !samePlacement(a.Placement, def) {
		t.Errorf("A3 应当是默认放置 %v，实际 %v", def, a.Placement)
	}
	// A4 的放置长度必须等于副本数
	if a := byKey["A4"]; len(a.Placement) != cfg.Replicas {
		t.Errorf("A4 放置长度应为 %d，实际 %v", cfg.Replicas, a.Placement)
	}
}

// TestAblationJointNeverWorse 是本文件最重要的一条：
// **联合求解（A4）不应该输给任何一个可分子臂。**
//
// 若这条失败，说明 SolveJoint 的搜索有缺陷 —— 而不是"联合没用"。
// 实测在 skew=0、K=3、Q 较高时会退化（此时所有臂都不可行），
// 那种参数点已被 ablation 统计排除；这里只测**至少有一臂可行**的情形。
func TestAblationJointNeverWorse(t *testing.T) {
	cases := []struct {
		name  string
		skew  float64
		q     float64
		k     int
		dom   int
		reps  int
	}{
		{"均质域-无事件", 0, 0, 1, 9, 5},
		{"均质域-弱事件", 0, 1e-3, 2, 9, 5},
		{"离散域-无事件", 3.0, 0, 1, 9, 5},
		{"离散域-弱事件", 3.0, 1e-3, 2, 9, 5},
		{"离散域-中事件", 2.0, 1e-2, 2, 9, 5},
		{"小规模-离散", 3.0, 1e-3, 2, 6, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			topo := unevenTopology(c.dom, 1e-3, c.skew)
			topo.RegionalEventProb = c.q
			topo.RegionalEventDomains = c.k
			cfg := Config{Replicas: c.reps, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
			av := AvailabilityModel{
				Model:    CorrelatedFailure{P: 1e-3, Q: c.q, K: c.k},
				Topology: topo,
			}
			arms := AblationLadder(topo, cfg, av, DefaultTargets())
			byKey := map[string]AblationArm{}
			for _, a := range arms {
				byKey[a.Key] = a
			}
			a2, a3, a4 := byKey["A2"], byKey["A3"], byKey["A4"]

			// 至少一臂可行时，A4 不允许输给可分子臂
			anyFeasible := a2.Feasible || a3.Feasible || a4.Feasible
			if !anyFeasible {
				t.Skip("四臂全部不达标，比较无意义（ablation 统计里这类点会被排除）")
			}
			if compareArmsTest(a2, a4) > 0 {
				t.Errorf("A4 输给 A2：A2(消息=%d 可行=%v 裕度=%+.3e) vs A4(消息=%d 可行=%v 裕度=%+.3e)",
					a2.WriteMsgs, a2.Feasible, a2.MinMargin,
					a4.WriteMsgs, a4.Feasible, a4.MinMargin)
			}
			if compareArmsTest(a3, a4) > 0 {
				t.Errorf("A4 输给 A3：A3(消息=%d 可行=%v 裕度=%+.3e) vs A4(消息=%d 可行=%v 裕度=%+.3e)",
					a3.WriteMsgs, a3.Feasible, a3.MinMargin,
					a4.WriteMsgs, a4.Feasible, a4.MinMargin)
			}
		})
	}
}

// compareArmsTest 与 cmd/faftbench 里的 compareArms 同口径：
// 可行优先 → 消息数 → 裕度；都不可行时比裕度。
func compareArmsTest(a, b AblationArm) int {
	cmpF := func(x, y float64) int {
		switch {
		case x > y+1e-12:
			return 1
		case x < y-1e-12:
			return -1
		default:
			return 0
		}
	}
	if a.Feasible != b.Feasible {
		if a.Feasible {
			return 1
		}
		return -1
	}
	if !a.Feasible {
		return cmpF(a.MinMargin, b.MinMargin)
	}
	if a.WriteMsgs != b.WriteMsgs {
		if a.WriteMsgs < b.WriteMsgs {
			return 1
		}
		return -1
	}
	return cmpF(a.MinMargin, b.MinMargin)
}

// TestSolvePlacementOnlyRespectsFixedGeometry 确认消融 A2 臂真的把几何钉死了
// —— 否则它就不是"只看放置的贡献"，整个消融失去意义。
func TestSolvePlacementOnlyRespectsFixedGeometry(t *testing.T) {
	topo := unevenTopology(9, 1e-3, 3.0)
	cfg := Config{Replicas: 5, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
	av := AvailabilityModel{Model: CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 2}, Topology: topo}

	q1, q2 := 3, 3
	jp := SolvePlacementOnly(topo, cfg, av, DefaultTargets(), q1, q2)
	if len(jp.Quorum.Q1Members) != q1 || len(jp.Quorum.Q2Members) != q2 {
		t.Errorf("几何应被钉死在 (%d,%d)，实际 (%d,%d)",
			q1, q2, len(jp.Quorum.Q1Members), len(jp.Quorum.Q2Members))
	}
	if jp.WriteMsgsPerOp != 2*q2 {
		t.Errorf("写消息数应恒为 2|Q2|=%d，实际 %d", 2*q2, jp.WriteMsgsPerOp)
	}
	if len(jp.Placement) != cfg.Replicas {
		t.Errorf("放置长度应为 %d，实际 %v", cfg.Replicas, jp.Placement)
	}
}
