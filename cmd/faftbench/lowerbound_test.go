// cmd/faftbench/lowerbound_test.go
//
// 只测汇总口径 —— 因为这里出过的错（BUG-29）是**统计错误**，
// 不是计算错误：数字全都算得出来，只是被数了两遍。
// 用手算得出来的输入钉住它。
package main

import "testing"

func TestSummarizeLBCountsEveryCellOnce(t *testing.T) {
	// 3 个参数点：2 个下界可行、1 个下界不可行。
	cells := []lbCell{
		{
			LBFeasible: true,
			Arms: []lbArm{
				{Planner: "joint", Feasible: true, AtLowerBound: true, Msgs: 2},
				{Planner: "majority", Feasible: true, AtLowerBound: false, Msgs: 6},
			},
		},
		{
			LBFeasible: true,
			Arms: []lbArm{
				{Planner: "joint", Feasible: true, AtLowerBound: true, Msgs: 2},
				{Planner: "majority", Feasible: false},
			},
		},
		{
			// 下界不可行：目标本身不可达，任何方法都不可行。
			// 这一点**不得**计入 majority 的 MissedTarget。
			LBFeasible: false,
			Arms: []lbArm{
				{Planner: "joint", Feasible: false},
				{Planner: "majority", Feasible: false},
			},
		},
	}

	got, diff := summarizeLB(cells, []string{"joint", "majority"}, nil)

	if len(got) != 2 {
		t.Fatalf("汇总行数 = %d，期望 2（每个 planner 一行）", len(got))
	}
	joint, majority := got[0], got[1]

	if joint.CellsEvaluated != 2 || joint.AtLB != 2 {
		t.Errorf("joint：可行点=%d 达到下界=%d，期望 2 / 2", joint.CellsEvaluated, joint.AtLB)
	}
	if joint.MissedTarget != 0 || joint.AboveLB != 0 {
		t.Errorf("joint：未达目标=%d 高于下界=%d，期望 0 / 0", joint.MissedTarget, joint.AboveLB)
	}
	if majority.CellsEvaluated != 2 || majority.AtLB != 0 || majority.AboveLB != 1 || majority.MissedTarget != 1 {
		t.Errorf("majority：可行点=%d 达到=%d 高于=%d 未达=%d，期望 2 / 0 / 1 / 1",
			majority.CellsEvaluated, majority.AtLB, majority.AboveLB, majority.MissedTarget)
	}
	// 每个统计量都不得超过它的定义域 —— 超了就是重复计数。
	for _, s := range got {
		if sum := s.AtLB + s.AboveLB + s.MissedTarget; sum != s.CellsEvaluated {
			t.Errorf("%s：达到+高于+未达 = %d ≠ 可行点 %d（统计重复计数或漏计）",
				s.Planner, sum, s.CellsEvaluated)
		}
	}
	// 区分力：只有第 1 个点上有"可达但未达"的方法。
	if diff != 1 {
		t.Errorf("有区分力的点 = %d，期望 1", diff)
	}
}

func TestSummarizeLBExposesDuplicateNames(t *testing.T) {
	// BUG-29 的形态：两个 baseline 实例同名，被聚合成同一行，
	// 于是 1 个参数点被数成 2 次 —— 统计量超过了它的定义域。
	//
	// 这里把"这种现象能被不变式抓住"钉住：名字一旦再撞车，
	// TestSummarizeLBCountsEveryCellOnce 的
	// 「达到 + 高于 + 未达 == 可行点」会立刻失败。
	cells := []lbCell{{
		LBFeasible: true,
		Arms: []lbArm{
			{Planner: "flexiraft", Feasible: false},
			{Planner: "flexiraft", Feasible: false},
		},
	}}
	got, _ := summarizeLB(cells, []string{"flexiraft"}, nil)

	if got[0].MissedTarget <= got[0].CellsEvaluated {
		t.Fatalf("同名两臂未被合并计数（未达=%d 可行点=%d）—— "+
			"统计口径可能已改成按臂聚合，本测试与 BUG-29 的说明都需要更新",
			got[0].MissedTarget, got[0].CellsEvaluated)
	}
	t.Logf("同名两臂：未达目标=%d > 可行点=%d —— 不变式会报错，BUG-29 可被抓住",
		got[0].MissedTarget, got[0].CellsEvaluated)
}

func TestLBArmSymbolAndMedian(t *testing.T) {
	cases := []struct {
		arm  lbArm
		want string
	}{
		{lbArm{Feasible: false}, "✗"},
		{lbArm{Feasible: true, Q2: 0}, "∅"},
		{lbArm{Feasible: true, Q2: 1, Msgs: 2, AtLowerBound: true}, "2★"},
		{lbArm{Feasible: true, Q2: 3, Msgs: 6, Gap: 4}, "6 (+4)"},
	}
	for _, c := range cases {
		if got := lbArmSymbol(c.arm); got != c.want {
			t.Errorf("lbArmSymbol(%+v) = %q，期望 %q", c.arm, got, c.want)
		}
	}
	if got := median(nil); got != 0 {
		t.Errorf("空集合中位数应为 0，实际 %v", got)
	}
	if got := median([]float64{3, 1, 2}); got != 2 {
		t.Errorf("中位数应为 2，实际 %v", got)
	}
	if got := lbRatioStr(0); got != "-" {
		t.Errorf("比值为 0 应显示 -，实际 %q", got)
	}
}
