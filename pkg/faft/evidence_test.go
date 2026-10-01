package faft

import (
	"strings"
	"testing"
)

// TestEvidenceManifestWellFormed 证据清单必须完整且自洽。
//
// 这份清单是论文写作时的自查表：每条结论都必须标注证据等级、
// 产出位置、以及"能主张什么/不能主张什么"。
// 若某条缺了"不可主张"，作者就可能误把它当作强结论使用。
func TestEvidenceManifestWellFormed(t *testing.T) {
	m := EvidenceManifest()
	if len(m) == 0 {
		t.Fatal("证据清单为空")
	}

	validEvidence := map[Evidence]bool{
		EvidenceMeasured:  true,
		EvidenceAnalysis:  true,
		EvidenceSimulated: true,
		EvidenceDerived:   true,
	}

	for i, c := range m {
		if strings.TrimSpace(c.Statement) == "" {
			t.Errorf("第 %d 条：Statement 为空", i+1)
		}
		if !validEvidence[c.Evidence] {
			t.Errorf("第 %d 条：证据等级 %d 非法", i+1, c.Evidence)
		}
		if strings.TrimSpace(c.Where) == "" {
			t.Errorf("第 %d 条：Where 为空 —— 无法核对", i+1)
		}
		if strings.TrimSpace(c.CanClaim) == "" {
			t.Errorf("第 %d 条：CanClaim 为空", i+1)
		}
		// 这条最关键：每条结论都必须写清"不可主张"什么。
		if strings.TrimSpace(c.CannotClaim) == "" {
			t.Errorf("第 %d 条：CannotClaim 为空 —— 必须写明该等级下的使用边界", i+1)
		}
	}
}

// TestMeasuredClaimsPointToMeasuredArtifacts MEASURED 级结论必须指向实测产出。
//
// 这是防止"把解析结果说成实测"的机器化检查：
// 若某条结论标为 MEASURED，其 Where 必须提到实测的产出位置
// （cmd/kvbench 或 results/）。标错等级会在这里被拦住。
func TestMeasuredClaimsPointToMeasuredArtifacts(t *testing.T) {
	for i, c := range EvidenceManifest() {
		if c.Evidence != EvidenceMeasured {
			continue
		}
		w := c.Where
		if !strings.Contains(w, "kvbench") && !strings.Contains(w, "results/") {
			t.Errorf("第 %d 条标为 MEASURED 但 Where=%q 未指向实测产出"+
				"（应含 cmd/kvbench 或 results/）", i+1, w)
		}
	}
}

// TestAnalysisClaimsDoNotClaimPerformance ANALYSIS 级结论不得作性能主张。
//
// pkg/faft 的数字是组合概率与消息计数，不是测出来的吞吐或延迟。
// 若某条 ANALYSIS 级结论的 CanClaim 里出现性能字样，说明分级用错了。
func TestAnalysisClaimsDoNotClaimPerformance(t *testing.T) {
	// 允许出现的词：结构性质、成本界、可用性、消息数、门槛
	// 不允许："吞吐"、"QPS"、"延迟降低" 等性能主张
	banned := []string{"吞吐", "QPS", "ops/s", "延迟降"}

	for i, c := range EvidenceManifest() {
		if c.Evidence != EvidenceAnalysis {
			continue
		}
		for _, b := range banned {
			if strings.Contains(c.CanClaim, b) {
				t.Errorf("第 %d 条为 ANALYSIS 级，但 CanClaim 含性能主张 %q：%s",
					i+1, b, c.CanClaim)
			}
		}
	}
}

// TestDerivedClaimsMarkPublishedWork 已发表的结果不得标为 DERIVED。
//
// leader 出口带宽 ∝ 1/(n-1) 是 Mencius (OSDI'08 §7) 已给出的结论。
// 若把它标成本项目的推导，就是错误地宣称优先权。
// 本测试检查该条的 CannotClaim 明确说出了这一点。
func TestDerivedClaimsMarkPublishedWork(t *testing.T) {
	found := false
	for _, c := range EvidenceManifest() {
		if !strings.Contains(c.Statement, "1/(n-1)") {
			continue
		}
		found = true
		if c.Evidence != EvidenceDerived {
			t.Errorf("1/(n-1) 律的证据等级 = %s，期望 DERIVED", c.Evidence)
		}
		if !strings.Contains(c.CannotClaim, "已发表") &&
			!strings.Contains(c.CannotClaim, "文献") {
			t.Errorf("1/(n-1) 律的 CannotClaim 必须说明它来自已发表文献，"+
				"不能作为本项目贡献：%s", c.CannotClaim)
		}
	}
	if !found {
		t.Error("证据清单中未见 1/(n-1) 律 —— 该律的归属是最容易被误标的一条")
	}
}

// TestEvidenceStringRenders 证据等级的字符串化必须可读。
func TestEvidenceStringRenders(t *testing.T) {
	cases := map[Evidence]string{
		EvidenceMeasured:  "MEASURED",
		EvidenceAnalysis:  "ANALYSIS",
		EvidenceSimulated: "SIMULATED",
		EvidenceDerived:   "DERIVED",
	}
	for e, want := range cases {
		if got := e.String(); !strings.Contains(got, want) {
			t.Errorf("Evidence(%d).String() = %q，应含 %q", e, got, want)
		}
	}
	// 未知值不应 panic
	if got := Evidence(999).String(); got == "" {
		t.Error("未知证据等级应返回非空字符串")
	}
}

// TestEvidenceManifestRenders 渲染输出必须含全部等级说明。
func TestEvidenceManifestRenders(t *testing.T) {
	out := FormatEvidenceManifest()
	if out == "" {
		t.Fatal("渲染输出为空")
	}
	for _, kw := range []string{"MEASURED", "ANALYSIS", "SIMULATED", "DERIVED", "不可主张"} {
		if !strings.Contains(out, kw) {
			t.Errorf("渲染输出缺少 %q", kw)
		}
	}
}
