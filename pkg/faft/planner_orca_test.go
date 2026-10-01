package faft

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Orca
// ---------------------------------------------------------------------------

// TestOrcaCommitQuorumIsKPlusOne commit quorum 必须固定为 k+1。
func TestOrcaCommitQuorumIsKPlusOne(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	av := AvailabilityModel{Model: IndependentFailure{P: 1e-4}, Topology: topo}
	const n = 21
	placement := make(Placement, n)
	for i := range placement {
		placement[i] = i % len(topo.Domains)
	}

	for _, k := range []int{1, 2, 3, 5} {
		fp := OrcaPlanner{FaultTolerance: k}.Plan(topo,
			Config{Replicas: n, Placement: placement}, av)
		_, q2 := fp.Quorum.Size()
		if q2 != k+1 {
			t.Errorf("k=%d: |Q2|=%d，期望 k+1=%d", k, q2, k+1)
		}
	}
}

// TestOrcaDefaultKIsSmall 默认 k 必须小到与多数 quorum 有区分度。
//
// 这是本次修正的核心：初版默认 k = n/2，导致 k+1 = n/2+1 恰好等于多数
// quorum，Orca 与 MajorityPlanner 完全重合、对照表里毫无信息量。
// Orca 的卖点正是"commit quorum 小于多数"，默认值必须体现这一点。
//
// ⚠️ n=5 是例外且属于数学必然：k=min(2, n/2)=2 时 k+1=3，
// 而 n=5 的多数恰好也是 3 —— 5 个节点根本不存在比多数更小的可用 quorum。
// 因此本测试从 n=9 起要求严格小于。
func TestOrcaDefaultKIsSmall(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	av := AvailabilityModel{Model: IndependentFailure{P: 1e-4}, Topology: topo}

	for _, n := range []int{5, 9, 11, 21, 51, 101} {
		placement := make(Placement, n)
		for i := range placement {
			placement[i] = i % len(topo.Domains)
		}
		cfg := Config{Replicas: n, Placement: placement}

		orca := OrcaPlanner{}.Plan(topo, cfg, av)
		majority := MajorityPlanner{}.Plan(topo, cfg, av)

		_, q2Orca := orca.Quorum.Size()
		_, q2Maj := majority.Quorum.Size()
		q1, _ := orca.Quorum.Size()

		// 无论 n 取何值，都必须满足 Flexible Paxos 约束。
		if q1+q2Orca <= n {
			t.Errorf("n=%d: Orca 的 |Q1|+|Q2| = %d 不满足 > %d", n, q1+q2Orca, n)
		}

		if n >= 9 {
			if q2Orca >= q2Maj {
				t.Errorf("n=%d: Orca 的 |Q2|=%d 不小于多数 quorum 的 %d —— "+
					"默认 k 过大，本 planner 失去区分度", n, q2Orca, q2Maj)
			}
		} else {
			// n=5：记录而非断言失败 —— 小集群下两者必然重合。
			if q2Orca == q2Maj {
				t.Logf("n=%d: Orca 与多数 quorum 的 |Q2| 同为 %d —— "+
					"该规模下不存在比多数更小的可用 quorum，属数学必然", n, q2Orca)
			}
		}

		t.Logf("n=%-4d Orca |Q2|=%d |Q1|=%d 写消息=%-4d ; 多数 |Q2|=%d 写消息=%-4d ; 降幅 %.2fx",
			n, q2Orca, q1, orca.WriteMsgsPerOp, q2Maj, majority.WriteMsgsPerOp,
			float64(majority.WriteMsgsPerOp)/float64(orca.WriteMsgsPerOp))
	}
}

// TestOrcaDynamicExcludesFailedReplicas 动态 quorum 必须真的排除故障副本。
func TestOrcaDynamicExcludesFailedReplicas(t *testing.T) {
	topo := NewUniformTopology(3, 2.0)
	av := AvailabilityModel{Model: IndependentFailure{P: 1e-4}, Topology: topo}
	const n = 9
	placement := make(Placement, n)
	for i := range placement {
		placement[i] = i % len(topo.Domains)
	}
	cfg := Config{Replicas: n, Placement: placement}

	failed := []int{0, 1, 2}
	fp := OrcaPlanner{FaultTolerance: 2, FailedReplicas: failed}.Plan(topo, cfg, av)

	// 被排除的副本不得出现在任何 quorum 里
	inQuorum := map[int]bool{}
	for _, m := range append(append([]int{}, fp.Quorum.Q1Members...), fp.Quorum.Q2Members...) {
		inQuorum[m] = true
	}
	for _, f := range failed {
		if inQuorum[f] {
			t.Errorf("副本 %d 被声明为故障，却仍出现在 quorum 中", f)
		}
	}
	// 必须成功构造出 quorum，而不是因为排除而失败
	if len(fp.Quorum.Q2Members) == 0 {
		t.Fatalf("排除故障副本后 Q2 为空，Diagnostics: %s", fp.Diagnostics)
	}
	if !strings.Contains(fp.Diagnostics, "已排除") {
		t.Errorf("Diagnostics 应说明排除了故障副本：%s", fp.Diagnostics)
	}
	t.Logf("排除 %v 后：|Q2|=%d |Q1|=%d 数据可用性=%.9g",
		failed, len(fp.Quorum.Q2Members), len(fp.Quorum.Q1Members), fp.DataAvailability)
}

// TestOrcaDynamicBeatsStaticFlexiRaftOrca 的动态性必须体现为可用性优势。
//
// 这是 Orca 相对 FlexiRaft 的**核心主张**：动态 quorum 在部分节点已失效时
// 仍能形成足够大的 quorum，而静态配置的 quorum 尺寸固定，会因故障而失效。
//
// 场景：n=21，先让 3 个副本处于"已失效"状态。
//
//	FlexiRaft static：Q2 尺寸固定为 n/4=5，从固定前缀里取 —— 其中包含
//	                  已失效的副本，实际有效成员只剩 2 个，quorum 无法形成。
//	Orca dynamic    ：排除已失效副本后再构造，有效成员仍是 k+1 个。
//
// 若本测试失败，说明 Orca 的"动态"没有产生实际差异，
// 那么把它列入 baseline 就没有意义。
func TestOrcaDynamicBeatsStaticFlexiRaftOrca(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	for i := range topo.Domains {
		topo.Domains[i].FailProb = 1e-4
	}
	topo.RegionalEventProb = 0
	av := AvailabilityModel{Model: IndependentFailure{P: 0}, Topology: topo}

	const n = 21
	placement := make(Placement, n)
	for i := range placement {
		placement[i] = i % len(topo.Domains)
	}
	cfg := Config{Replicas: n, Placement: placement}

	// 让前 3 个副本失效 —— FlexiRaft 的固定前缀取法会正好命中它们。
	failed := []int{0, 1, 2}

	// FlexiRaft static：从固定前缀取 q2 个成员，不感知故障。
	fr := FlexiRaftPlanner{DataQuorum: 6}
	frPlan := fr.Plan(topo, cfg, av)

	// Orca dynamic：排除故障副本后再取。
	orca := OrcaPlanner{FaultTolerance: 5, FailedReplicas: failed}
	orcaPlan := orca.Plan(topo, cfg, av)

	frQ2 := frPlan.Quorum.Q2Members
	orcaQ2 := orcaPlan.Quorum.Q2Members

	// 统计各自的 Q2 里有多少个"实际可用"（未被声明故障）的成员。
	countAlive := func(members []int) int {
		alive := 0
		for _, m := range members {
			isFailed := false
			for _, f := range failed {
				if m == f {
					isFailed = true
					break
				}
			}
			if !isFailed {
				alive++
			}
		}
		return alive
	}

	frAlive := countAlive(frQ2)
	orcaAlive := countAlive(orcaQ2)

	t.Logf("n=%d，已失效副本 %v", n, failed)
	t.Logf("  FlexiRaft static: |Q2|=%d，其中有效成员 %d 个（成员 %v）", len(frQ2), frAlive, frQ2)
	t.Logf("  Orca dynamic    : |Q2|=%d，其中有效成员 %d 个（成员 %v）", len(orcaQ2), orcaAlive, orcaQ2)

	if orcaAlive <= frAlive {
		t.Errorf("Orca 的有效成员数 %d 未超过 FlexiRaft static 的 %d —— "+
			"动态排除未产生实际差异，列入 baseline 无意义", orcaAlive, frAlive)
	}

	// Orca 的 Q2 必须完全不含故障副本。
	if orcaAlive != len(orcaQ2) {
		t.Errorf("Orca 的 Q2 含 %d 个故障副本（共 %d 个成员）—— 排除未生效",
			len(orcaQ2)-orcaAlive, len(orcaQ2))
	}
}

// TestOrcaNotesDiscloseLimitations Orca 的 Notes 必须披露未实现部分与文献不确信。
//
// Orca 是 PVLDB'26 的最新工作，我未获得论文全文。
// 若不披露这一点就把它当权威 baseline 引用，就是伪引用。
func TestOrcaNotesDiscloseLimitations(t *testing.T) {
	p := OrcaPlanner{}
	n := p.Notes()
	if n == "" {
		t.Fatal("Notes 为空 —— 必须披露实现范围")
	}
	// 必须提到"未实现"与"未获得论文全文"
	for _, kw := range []string{"未实现", "未获得"} {
		if !strings.Contains(n, kw) {
			t.Errorf("Notes 应含 %q，实际：%s", kw, n)
		}
	}
	if p.Source() == "" {
		t.Error("Source 为空 —— 论文中无法核对依据")
	}
	// Source 应含 DOI 或卷期信息，便于核对
	if !strings.Contains(p.Source(), "DOI") && !strings.Contains(p.Source(), "PVLDB") {
		t.Errorf("Source 应含可核对标识（DOI 或卷期）：%s", p.Source())
	}
}

// ---------------------------------------------------------------------------
// TiKV PD
// ---------------------------------------------------------------------------

// TestTiKVPDHasNoQuorumFreedom PD 在 quorum 几何维度必须与多数 quorum 一致。
//
// PD 是 placement 调度器，不是 quorum 规划器。本测试确认这一点，
// 从而使"FAFT 相对 PD 的增量"这个说法有据可依。
func TestTiKVPDHasNoQuorumFreedom(t *testing.T) {
	topo := NewUniformTopology(5, 2.0)
	av := AvailabilityModel{Model: IndependentFailure{P: 1e-4}, Topology: topo}

	for _, n := range []int{3, 5, 9, 21, 51} {
		placement := make(Placement, n)
		for i := range placement {
			placement[i] = i % len(topo.Domains)
		}
		cfg := Config{Replicas: n, Placement: placement}

		pd := TiKVPDPlanner{}.Plan(topo, cfg, av)
		maj := MajorityPlanner{}.Plan(topo, cfg, av)

		pdQ1, pdQ2 := pd.Quorum.Size()
		majQ1, majQ2 := maj.Quorum.Size()
		if pdQ1 != majQ1 || pdQ2 != majQ2 {
			t.Errorf("n=%d: PD 给出 (%d,%d)，多数 quorum 给出 (%d,%d) —— "+
				"两者在 quorum 几何上应完全一致", n, pdQ1, pdQ2, majQ1, majQ2)
		}
		if pd.WriteMsgsPerOp != maj.WriteMsgsPerOp {
			t.Errorf("n=%d: PD 写消息 %d != 多数 quorum 的 %d",
				n, pd.WriteMsgsPerOp, maj.WriteMsgsPerOp)
		}
	}
}

// TestTiKVPDNotesDiscloseScopeGap PD 的 Notes 必须说明维度差异。
//
// PD 的优化是跨分片的（把不同 region 的副本摊到不同 store），
// 而本对照框架只做单分片内的 quorum 决策。
// 若不披露这个维度差异，读者会误以为"FAFT 全面优于 PD"。
func TestTiKVPDNotesDiscloseScopeGap(t *testing.T) {
	p := TiKVPDPlanner{}
	n := p.Notes()
	if !strings.Contains(n, "跨分片") {
		t.Errorf("Notes 必须说明 PD 的优化在跨分片维度，实际：%s", n)
	}
	// 必须明确说出"只做单分片内"或等价的限定，否则读者会以为本框架覆盖了 PD 的全部收益。
	if !strings.Contains(n, "只做单分片") && !strings.Contains(n, "未度量") &&
		!strings.Contains(n, "未覆盖") {
		t.Errorf("Notes 必须说明跨分片维度不在本框架度量范围内，实际：%s", n)
	}
	if !strings.Contains(p.Source(), "非论文") {
		t.Errorf("Source 必须说明 PD 没有学术发表（这是本项目的文献发现之一）：%s", p.Source())
	}
}
// ---------------------------------------------------------------------------
// DefaultBaselines
// ---------------------------------------------------------------------------

// TestDefaultBaselinesComplete 默认 baseline 集合必须完整且命名唯一。
func TestDefaultBaselinesComplete(t *testing.T) {
	bs := DefaultBaselines(DefaultTargets())
	if len(bs) < 5 {
		t.Fatalf("baseline 数量 = %d，过少", len(bs))
	}

	names := map[string]int{}
	required := []string{"majority", "tikv-pd", "flexiraft", "faft", "faft-countselect", "orca"}
	for _, p := range bs {
		names[p.Name()]++
		if p.Source() == "" {
			t.Errorf("%s: Source 为空", p.Name())
		}
		if p.Notes() == "" {
			t.Errorf("%s: Notes 为空", p.Name())
		}
	}
	for _, r := range required {
		if names[r] == 0 {
			t.Errorf("缺少必需 baseline: %s", r)
		}
	}
	// 多数 quorum 必须唯一（它是相对降幅的基准）
	if names["majority"] != 1 {
		t.Errorf("majority 出现 %d 次，必须唯一（它是降幅基准）", names["majority"])
	}
	t.Logf("baseline 集合：%v", func() []string {
		var out []string
		for _, p := range bs {
			out = append(out, p.Name())
		}
		return out
	}())
}
