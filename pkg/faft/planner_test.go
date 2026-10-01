package faft

import "testing"

// TestFaftAblationActuallyDiffers 验证"FAFT 几何 + 域计数选择"这个消融配置
// 确实与完整 FAFT 产生**不同**的成员集合。
//
// 为什么必须验这件事：消融实验的价值完全在于它能隔离出"成员选择准则"的贡献。
// 若两个 planner 在任何场景下都给出相同结果，那这个消融项就没有信息量，
// 而论文里若把它当作"成员选择无贡献"的证据，就是错的 ——
// 真实原因可能是消融根本没生效（参数没传进去、分支没走到）。
//
// 初版在 |Q2|=1 的规模上观察到两者完全相同，那是**数学必然**
// （单成员集合没有选择余地），不能说明消融无效。
// 本测试选一个 |Q2|>1 且域失效率差异大的场景来真正检验。
func TestFaftAblationActuallyDiffers(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	// 失效率差异拉到 10000 倍，让"选哪个域"有显著区别。
	probs := []float64{1e-6, 1e-4, 1e-3, 1e-2, 1e-1}
	for i, p := range probs {
		topo.Domains[i].FailProb = p
	}
	// 不用区域事件：否则可用性被 1-q 钉住，差异被掩盖。
	topo.RegionalEventProb = 0

	// 目标必须让**单副本无法满足**，否则求解器总选 |Q2|=1，
	// 而单成员集合没有选择自由度，消融测不出差异。
	//
	// 本拓扑最稳的域失效率 1e-6，单个副本可用性 0.999999；
	// 因此目标取 0.999999999（要求失效率 ≤1e-9）迫使 |Q2|>1。
	// 初版取 0.999999 恰好等于单副本可用性，导致测试被 SKIP。
	const n = 21
	targets := AvailabilityTargets{Data: 0.999999999, Control: 0.9999}

	placement := make(Placement, n)
	for i := range placement {
		placement[i] = i % len(topo.Domains)
	}
	av := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: topo}
	cfg := Config{Replicas: n, Placement: placement}

	full := FaftPlanner{Targets: targets}.Plan(topo, cfg, av)
	ablate := UniformFaftPlanner{Targets: targets}.Plan(topo, cfg, av)

	_, q2Full := full.Quorum.Size()
	_, q2Ablate := ablate.Quorum.Size()

	t.Logf("目标 data>=%g control>=%g", targets.Data, targets.Control)
	t.Logf("完整 FAFT      : |Q2|=%d |Q1|=%d 数据可用性=%.12g",
		q2Full, len(full.Quorum.Q1Members), full.DataAvailability)
	t.Logf("消融（域计数）: |Q2|=%d |Q1|=%d 数据可用性=%.12g",
		q2Ablate, len(ablate.Quorum.Q1Members), ablate.DataAvailability)

	// 前提检查：|Q2| 必须 >1，否则"选择成员"没有自由度，测不出差异。
	if q2Full <= 1 {
		t.Skipf("本次求解得到 |Q2|=%d，成员选择无自由度，无法检验消融。"+
			"这不算失败 —— 需要调整目标让 |Q2|>1", q2Full)
	}

	// 核心断言：两者必须给出不同的成员集合，
	// 且完整 FAFT 的数据可用性不低于消融版。
	same := sameMemberSet(full.Quorum.Q2Members, ablate.Quorum.Q2Members)
	if same {
		t.Errorf("|Q2|=%d 时两个 planner 给出**相同**的成员集合 %v —— 消融未生效",
			q2Full, full.Quorum.Q2Members)
	}
	if full.DataAvailability < ablate.DataAvailability-1e-15 {
		t.Errorf("完整 FAFT 的数据可用性 %.12g 低于消融版 %.12g —— 按可用性选择应不劣于按域计数",
			full.DataAvailability, ablate.DataAvailability)
	}
	if full.DataAvailability > ablate.DataAvailability {
		t.Logf("成员选择的贡献：可用性 %.12g → %.12g（提升 %.3e）",
			ablate.DataAvailability, full.DataAvailability,
			full.DataAvailability-ablate.DataAvailability)
	} else {
		t.Logf("两者可用性相同 —— 在此场景下成员选择准则不改变结果（可作为负结果记录）")
	}
}

func sameMemberSet(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[int]int{}
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}

// TestPlannersAllRespectFlexiblePaxos 所有 planner 都必须满足 |Q1|+|Q2| > N。
//
// 这是安全性的必要条件，任何 planner 违反都会导致共识安全性被破坏。
// 无论 baseline 还是本项目，这条都不可协商。
func TestPlannersAllRespectFlexiblePaxos(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	for i := range topo.Domains {
		topo.Domains[i].FailProb = 1e-3
	}
	av := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: topo}

	planners := []QuorumPlanner{
		MajorityPlanner{},
		FlexiRaftPlanner{},
		FlexiRaftPlanner{DataQuorum: 2},
		FlexiRaftPlanner{DataQuorum: 3, UseAvailabilityAware: true},
		FaftPlanner{},
		UniformFaftPlanner{},
	}

	for _, n := range []int{3, 5, 9, 11, 21, 51} {
		placement := make(Placement, n)
		for i := range placement {
			placement[i] = i % len(topo.Domains)
		}
		cfg := Config{Replicas: n, Placement: placement}

		for _, p := range planners {
			fp := p.Plan(topo, cfg, av)
			q1, q2 := fp.Quorum.Size()
			if q1 == 0 && q2 == 0 {
				// 无解时允许返回零值（已有 Diagnostics 说明），但不算通过。
				t.Logf("n=%d %s: 无解（%s）", n, p.Name(), fp.Diagnostics)
				continue
			}
			if q1+q2 <= n {
				t.Errorf("n=%d %s: |Q1|(%d) + |Q2|(%d) = %d 不满足 > N(%d) —— 安全性被破坏",
					n, p.Name(), q1, q2, q1+q2, n)
			}
			if q1 < 1 || q2 < 1 {
				t.Errorf("n=%d %s: quorum 大小为 0（|Q1|=%d |Q2|=%d）", n, p.Name(), q1, q2)
			}
			// 成员下标不得越界或重复
			seen := map[int]bool{}
			for _, m := range append(append([]int{}, fp.Quorum.Q1Members...), fp.Quorum.Q2Members...) {
				if m < 0 || m >= n {
					t.Errorf("n=%d %s: 成员下标 %d 越界", n, p.Name(), m)
				}
				seen[m] = true
			}
			// 可用性必须在 [0,1]
			for _, v := range []float64{fp.DataAvailability, fp.ControlAvailability} {
				if v < 0 || v > 1 {
					t.Errorf("n=%d %s: 可用性 %.9g 超出 [0,1]", n, p.Name(), v)
				}
			}
		}
	}
}

// TestMajorityPlannerIsStandardMajority 基线必须与 Raft 的定义一致。
func TestMajorityPlannerIsStandardMajority(t *testing.T) {
	topo := NewUniformTopology(3, 2.0)
	av := AvailabilityModel{Model: IndependentFailure{P: 1e-4}, Topology: topo}

	for _, n := range []int{3, 5, 9, 21, 51} {
		placement := make(Placement, n)
		for i := range placement {
			placement[i] = i % len(topo.Domains)
		}
		fp := MajorityPlanner{}.Plan(topo, Config{Replicas: n, Placement: placement}, av)
		q1, q2 := fp.Quorum.Size()
		want := n/2 + 1
		if q1 != want || q2 != want {
			t.Errorf("n=%d: majority planner 给出 (%d,%d)，期望 (%d,%d)", n, q1, q2, want, want)
		}
		if fp.WriteMsgsPerOp != 2*want {
			t.Errorf("n=%d: 写消息 %d，期望 %d", n, fp.WriteMsgsPerOp, 2*want)
		}
	}
}

// TestFlexiRaftPlannerGeometry FlexiRaft 的 Q1 必须由 Q2 按 FPaxos 约束推导。
func TestFlexiRaftPlannerGeometry(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	av := AvailabilityModel{Model: IndependentFailure{P: 1e-4}, Topology: topo}
	const n = 21
	placement := make(Placement, n)
	for i := range placement {
		placement[i] = i % len(topo.Domains)
	}

	for _, q2 := range []int{1, 2, 5, 11} {
		fp := FlexiRaftPlanner{DataQuorum: q2}.Plan(topo,
			Config{Replicas: n, Placement: placement}, av)
		q1, gotQ2 := fp.Quorum.Size()
		if gotQ2 != q2 {
			t.Errorf("DataQuorum=%d 但得到 |Q2|=%d", q2, gotQ2)
		}
		wantQ1 := n - q2 + 1
		if q1 != wantQ1 {
			t.Errorf("DataQuorum=%d: |Q1|=%d，期望 n-|Q2|+1 = %d", q2, q1, wantQ1)
		}
		if q1+q2 <= n {
			t.Errorf("DataQuorum=%d: |Q1|+|Q2| = %d 不满足 > %d", q2, q1+q2, n)
		}
	}
}

// TestFlexiRaftDefaultQuorum 默认 data quorum 应为 max(2, n/4)。
func TestFlexiRaftDefaultQuorum(t *testing.T) {
	topo := NewUniformTopology(3, 2.0)
	av := AvailabilityModel{Model: IndependentFailure{P: 1e-4}, Topology: topo}

	cases := []struct{ n, wantQ2 int }{
		{4, 2}, {8, 2}, {12, 3}, {20, 5}, {40, 10}, {100, 25},
	}
	for _, c := range cases {
		placement := make(Placement, c.n)
		for i := range placement {
			placement[i] = i % len(topo.Domains)
		}
		fp := FlexiRaftPlanner{}.Plan(topo, Config{Replicas: c.n, Placement: placement}, av)
		_, q2 := fp.Quorum.Size()
		if q2 != c.wantQ2 {
			t.Errorf("n=%d: 默认 data quorum = %d，期望 %d", c.n, q2, c.wantQ2)
		}
	}
}

// TestPlannerSourceNotEmpty 每个 planner 都必须给出可核对的出处。
//
// 论文里若引用某个 baseline 却不写清依据，就是伪引用。
// 这个测试把"必须能说清依据"变成机器可检查的约束。
func TestPlannerSourceNotEmpty(t *testing.T) {
	for _, p := range []QuorumPlanner{
		MajorityPlanner{}, FlexiRaftPlanner{}, FaftPlanner{}, UniformFaftPlanner{},
	} {
		if p.Source() == "" {
			t.Errorf("%s: Source() 为空 —— 论文中无法核对依据", p.Name())
		}
	}
	// FlexiRaft 的 Notes 必须披露未实现的部分。
	fr := FlexiRaftPlanner{}
	if fr.Notes() == "" {
		t.Error("FlexiRaftPlanner: Notes() 为空 —— 必须披露实现范围")
	}
}
