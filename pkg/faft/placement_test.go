// pkg/faft/placement_test.go
//
// 这个文件回答一个论文里必须回答的问题：
// **你的放置搜索是贪心 + 局部搜索，它离最优有多远？**
//
// 做法：在小规模上（D^n ≤ 20 万）用穷举跑出真最优，与贪心结果逐点对照。
// 差距就是"贪心代价"，必须作为已知局限写进论文 —— 而不是等审稿人来问。
package faft

import (
	"math"
	"strings"
	"testing"
)

// unevenTopology 构造域失效率离散的拓扑。
//
// ⚠️ 可靠性排序**刻意与下标顺序正交**（用固定置换打乱）。
// 如果让域 0 最稳、域 1 次之……那么 `i % nDom` 这个默认放置恰好就是最优的，
// 于是"放置优化"看起来毫无价值 —— 那是拓扑构造的假象，不是方法的性质。
// 敏感性扫描踩过这个坑：90 个参数点全部打平。
func unevenTopology(domains int, pBase, spread float64) *Topology {
	t := NewUniformTopology(domains, 2.0)
	// 固定的质数步长置换，确保与下标无关且可复现。
	rank := make([]int, domains)
	seen := make([]bool, domains)
	idx := 0
	for step := 1; idx < domains; step++ {
		d := (step * 7) % domains
		if (step*7)%domains == 0 && step > 1 {
			d = 0
		}
		if !seen[d] {
			seen[d] = true
			rank[d] = idx
			idx++
		}
		if step > domains*3 {
			// 兜底：把剩下的按序补上（当 domains 与 7 不互质时会出现）
			for i := 0; i < domains; i++ {
				if !seen[i] {
					seen[i] = true
					rank[i] = idx
					idx++
				}
			}
		}
	}
	for i := range t.Domains {
		u := 0.0
		if domains > 1 {
			u = 2*float64(rank[i])/float64(domains-1) - 1
		}
		t.Domains[i].FailProb = pBase * math.Exp(spread*u)
	}
	t.RegionalEventProb = 1e-3
	t.RegionalEventDomains = 2
	return t
}

// TestDefaultPlacementIsNotOptimal 确认测试拓扑确实让默认放置（i % nDom）
// 不是最优的 —— 否则后面的对照测试全是空转。
func TestDefaultPlacementIsNotOptimal(t *testing.T) {
	topo := unevenTopology(9, 1e-3, 3.0)
	targets := DefaultTargets()
	cfg := Config{Replicas: 5, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
	av := AvailabilityModel{Model: CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 2}, Topology: topo}

	def := SolveWithFailures(topo, cfg, av, targets)
	joint := SolveJoint(topo, cfg, av, targets)

	t.Logf("默认放置 %v → |Q1|=%d |Q2|=%d 写消息=%d 数据可用性=%.9f 控制可用性=%.9f",
		cfg.withDefaults(topo).Placement,
		len(def.Quorum.Q1Members), len(def.Quorum.Q2Members),
		def.WriteMsgsPerOp, def.DataAvailability, def.ControlAvailability)
	t.Logf("联合求解 %v → |Q1|=%d |Q2|=%d 写消息=%d 数据可用性=%.9f 控制可用性=%.9f",
		joint.Placement,
		len(joint.Quorum.Q1Members), len(joint.Quorum.Q2Members),
		joint.WriteMsgsPerOp, joint.DataAvailability, joint.ControlAvailability)

	if sameInts(joint.Placement, cfg.withDefaults(topo).Placement) {
		t.Errorf("测试拓扑构造失败：联合求解选出的放置与默认的 i%%nDom 完全相同，"+
			"说明这个拓扑里默认放置已经是最优的，后面的对照测试没有意义。"+
			"默认=%v 联合=%v", cfg.withDefaults(topo).Placement, joint.Placement)
	}
}

// TestGreedyMatchesExhaustive 是本文件的重点：贪心离穷举最优有多远。
func TestGreedyMatchesExhaustive(t *testing.T) {
	cases := []struct {
		name          string
		domains, n    int
		pBase, spread float64
		data, ctrl    float64
	}{
		{"D6-n3-均质", 6, 3, 1e-3, 0.0, 0.999, 0.9999},
		{"D6-n3-离散", 6, 3, 1e-3, 3.0, 0.999, 0.9999},
		{"D6-n4-离散", 6, 4, 1e-3, 2.0, 0.999, 0.999},
		{"D5-n3-强离散", 5, 3, 1e-3, 4.0, 0.9999, 0.9999},
		{"D7-n3-宽目标", 7, 3, 1e-4, 3.0, 0.999, 0.999},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			topo := unevenTopology(c.domains, c.pBase, c.spread)
			targets := AvailabilityTargets{Data: c.data, Control: c.ctrl}
			cfg := Config{Replicas: c.n, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
			av := AvailabilityModel{
				Model:    CorrelatedFailure{P: c.pBase, Q: 1e-3, K: 2},
				Topology: topo,
			}

			g := SolveJoint(topo, cfg, av, targets)
			e := ExhaustiveJoint(topo, cfg, av, targets)

			gs := scoreOf(g.FailurePlan, targets)
			es := scoreOf(e.FailurePlan, targets)

			t.Logf("贪心：放置 %v 消息=%d 可用性裕度=%+.3e（评估 %d 个候选）",
				g.Placement, g.WriteMsgsPerOp, gs.margin, g.Evaluated)
			t.Logf("穷举：放置 %v 消息=%d 可用性裕度=%+.3e（评估 %d 个候选）",
				e.Placement, e.WriteMsgsPerOp, es.margin, e.Evaluated)

			// 贪心**不允许**比最优更差的消息数 —— 消息数是整数，差一级就是数量级差别。
			if gs.msgs > es.msgs {
				t.Errorf("贪心的写消息数(%d)劣于穷举最优(%d)：贪心代价过大",
					gs.msgs, es.msgs)
			}
			// 可行性不允许更差
			if es.feasible && !gs.feasible {
				t.Errorf("穷举可行而贪心不可行：贪心把方案做没了")
			}
			// 裕度允许略差（贪心是近似的），但不允许差一个数量级
			if es.feasible && gs.feasible && es.margin > 0 {
				if gs.margin < es.margin*0.5 {
					t.Errorf("贪心的可用性裕度(%.3e)不到穷举最优(%.3e)的一半",
						gs.margin, es.margin)
				}
			}
		})
	}
}

// TestPlacementImprovesOverDefault 是本文件的核心断言：
// **联合求解（含放置优化）应当不劣于"给定默认放置 + 只优化 quorum 成员"。**
//
// 这是 C1/C2 主张的最小可验证形式。若这条都不成立，
// 说明放置优化没有产生任何价值，论文的方向需要重新论证。
func TestPlacementImprovesOverDefault(t *testing.T) {
	topo := unevenTopology(9, 1e-3, 3.5)
	targets := DefaultTargets()
	cfg := Config{Replicas: 5, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
	av := AvailabilityModel{
		Model:    CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 2},
		Topology: topo,
	}

	only := SolveWithFailures(topo, cfg, av, targets) // 给定默认放置，只优化成员
	joint := SolveJoint(topo, cfg, av, targets)       // 放置 + 成员一起优化

	onlySc := scoreOf(only, targets)
	jointSc := scoreOf(joint.FailurePlan, targets)

	t.Logf("只优化成员：放置 %v 消息=%d 裕度=%+.3e",
		cfg.withDefaults(topo).Placement, only.WriteMsgsPerOp, onlySc.margin)
	t.Logf("联合优化　：放置 %v 消息=%d 裕度=%+.3e",
		joint.Placement, joint.WriteMsgsPerOp, jointSc.margin)

	if jointSc.msgs > onlySc.msgs {
		t.Errorf("联合求解的写消息数(%d)反而不如只优化成员(%d)", jointSc.msgs, onlySc.msgs)
	}
	if jointSc.msgs == onlySc.msgs && jointSc.margin <= onlySc.margin {
		t.Errorf("联合求解在消息数与可用性裕度上都没有改善 —— 放置优化没有产生价值。"+
			"消息数 %d vs %d，裕度 %.3e vs %.3e",
			jointSc.msgs, onlySc.msgs, jointSc.margin, onlySc.margin)
	}
}

// TestPlacementRespectsCapacity 确认容量约束被遵守 —— 否则求解器会给出
// 物理上放不下的放置。
func TestPlacementRespectsCapacity(t *testing.T) {
	topo := unevenTopology(6, 1e-3, 2.0)
	for i := range topo.Domains {
		topo.Domains[i].Capacity = 1 // 每个域最多 1 个副本
	}
	targets := DefaultTargets()
	cfg := Config{Replicas: 5, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
	av := AvailabilityModel{Model: CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 2}, Topology: topo}

	jp := SolveJoint(topo, cfg, av, targets)
	cnt := map[int]int{}
	for _, d := range jp.Placement {
		cnt[d]++
	}
	for d, c := range cnt {
		if c > 1 {
			t.Errorf("域 %d 被放了 %d 个副本，超过容量 1；放置=%v", d, c, jp.Placement)
		}
	}
	if len(jp.Placement) != 5 {
		t.Errorf("应放置 5 个副本，实际 %d 个：%v", len(jp.Placement), jp.Placement)
	}
}

// TestPlacementHandlesInsufficientCapacity 确认容量不足时**如实报告**。
//
// 行为约定：为了让下游按 Replicas 索引不越界，放置长度仍会补满 n；
// 但"容量只够放 k/n 个副本"这件事必须出现在 PlacementSearch 里 ——
// 调用方需要知道这个配置物理上不可行，而不是拿到一个看起来正常的方案。
func TestPlacementHandlesInsufficientCapacity(t *testing.T) {
	topo := unevenTopology(2, 1e-3, 1.0)
	for i := range topo.Domains {
		topo.Domains[i].Capacity = 1 // 总共只能放 2 个
	}
	targets := DefaultTargets()
	cfg := Config{Replicas: 5, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
	av := AvailabilityModel{Model: CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 1}, Topology: topo}

	jp := SolveJoint(topo, cfg, av, targets)
	if jp.PlacementSearch == "" {
		t.Fatalf("容量不足时必须给出说明，实际 PlacementSearch 为空")
	}
	if !strings.Contains(jp.PlacementSearch, "容量") {
		t.Errorf("说明里应当点明是容量问题，实际：%q", jp.PlacementSearch)
	}
	if len(jp.Placement) != cfg.Replicas {
		t.Errorf("放置长度应补满 Replicas=%d（否则下游索引越界），实际 %d：%v",
			cfg.Replicas, len(jp.Placement), jp.Placement)
	}
	// 至少要保证：真正有容量约束的那两个域里，各自不超过容量。
	cnt := map[int]int{}
	for _, d := range jp.Placement {
		cnt[d]++
	}
	if len(cnt) != 2 {
		t.Errorf("只有 2 个域，放置应当只涉及这 2 个，实际涉及 %d 个", len(cnt))
	}
	t.Logf("容量不足时的说明：%s", jp.PlacementSearch)
}

// TestJointPlannerIntegratesWithCompare 确认 JointPlanner 能进
// ComparePlanners 的对照框架（接口一致）。
func TestJointPlannerIntegratesWithCompare(t *testing.T) {
	topo := unevenTopology(6, 1e-3, 2.0)
	cfg := Config{Replicas: 5, DataFaults: 1, ControlFaults: 1, WriteRatio: 1}
	av := AvailabilityModel{Model: CorrelatedFailure{P: 1e-3, Q: 1e-3, K: 2}, Topology: topo}
	targets := DefaultTargets()

	cmp := ComparePlanners(topo, cfg, av,
		MajorityPlanner{},
		FaftPlanner{Targets: targets},
		JointPlanner{Targets: targets},
	)
	if len(cmp) != 3 {
		t.Fatalf("应当返回 3 个 planner 的对照，实际 %d 个", len(cmp))
	}
	names := map[string]bool{}
	for _, c := range cmp {
		names[c.Planner] = true
	}
	if !names["faft-joint"] {
		t.Errorf("对照结果里没有 faft-joint：%v", names)
	}
	for _, c := range cmp {
		t.Logf("%-14s |Q1|=%d |Q2|=%d 消息=%d 数据=%.9f 控制=%.9f 可行=%v",
			c.Planner, c.Q1Size, c.Q2Size, c.WriteMsgsPerOp,
			c.DataAvailability, c.ControlAvailability, c.Feasible)
	}
}

func sameInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
