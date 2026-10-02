// cmd/faultagg/main_test.go
//
// 这个工具处在「监控系统导出」与「故障模型拟合」之间，是整条链路上
// **唯一带主观判据**的一步。判据错了，后面拟合出的 P/Q/K 全废，
// 而错误不会以任何形式暴露出来 —— 只会得到一组看起来合理的参数。
// 所以必须有回归测试。
package main

import (
	"testing"
	"time"
)

func mk(node, domain string, startMin, endMin int) nodeOutage {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return nodeOutage{
		node:   node,
		domain: domain,
		start:  t0.Add(time.Duration(startMin) * time.Minute),
		end:    t0.Add(time.Duration(endMin) * time.Minute),
	}
}

func TestAggregateRequiresThreshold(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tMax := t0.Add(10 * time.Hour)
	total := map[string]int{"az-a": 8}

	// 8 个节点里挂 3 个（37.5%）—— 低于 50% 阈值，不算域级故障
	var three []nodeOutage
	for i := 0; i < 3; i++ {
		three = append(three, mk("n", "az-a", 60, 120))
		three[i].node = "az-a-node-" + string(rune('0'+i))
	}
	got, err := aggregate(three, total, 0.5, time.Minute, 2*time.Minute, t0, tMax)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("37.5%% 节点不可用不应算作域级故障（阈值 50%%），实际得到 %d 段", len(got))
	}

	// 挂 4 个（50%）—— 达到阈值，算
	var four []nodeOutage
	for i := 0; i < 4; i++ {
		n := mk("n", "az-a", 60, 120)
		n.node = "az-a-node-" + string(rune('0'+i))
		four = append(four, n)
	}
	got, err = aggregate(four, total, 0.5, time.Minute, 2*time.Minute, t0, tMax)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("50%% 节点不可用应算作域级故障，实际得到 %d 段", len(got))
	}
	if got[0].domain != "az-a" {
		t.Errorf("域标识不对: %s", got[0].domain)
	}
}

// TestAggregateMinDurationFiltersFlapping 确认瞬时抖动被滤掉：
// 8 个节点同时挂 20 秒，但 -minduration 是 1 分钟，不应算作域级故障。
func TestAggregateMinDurationFiltersFlapping(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tMax := t0.Add(time.Hour)
	total := map[string]int{"az-a": 8}
	var ivs []nodeOutage
	for i := 0; i < 8; i++ {
		n := mk("n", "az-a", 30, 30) // 零长度，靠 jitter 不行，手工设
		n.node = "az-a-node-" + string(rune('0'+i))
		n.start = t0.Add(30 * time.Minute)
		n.end = n.start.Add(20 * time.Second)
		ivs = append(ivs, n)
	}
	got, _ := aggregate(ivs, total, 0.5, time.Minute, 2*time.Minute, t0, tMax)
	if len(got) != 0 {
		t.Errorf("20 秒的抖动应被 -minduration=1m 滤掉，实际得到 %d 段", len(got))
	}
}

// TestAggregateMergesGaps 确认短暂恢复不会被切成两段：
// 一次 30 分钟故障中间有 1 分钟恢复，-mergegap=2m 应合并成一段。
func TestAggregateMergesGaps(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tMax := t0.Add(time.Hour)
	total := map[string]int{"az-a": 4}
	var ivs []nodeOutage
	for i := 0; i < 4; i++ {
		name := "az-a-node-" + string(rune('0'+i))
		// 每个节点分两段：00:00-00:15 与 00:16-00:30，中间 1 分钟恢复
		ivs = append(ivs,
			nodeOutage{node: name, domain: "az-a",
				start: t0.Add(0), end: t0.Add(15 * time.Minute)},
			nodeOutage{node: name, domain: "az-a",
				start: t0.Add(16 * time.Minute), end: t0.Add(30 * time.Minute)},
		)
	}
	got, _ := aggregate(ivs, total, 0.5, time.Minute, 2*time.Minute, t0, tMax)
	if len(got) != 1 {
		t.Fatalf("1 分钟的恢复间隔应被 -mergegap=2m 合并，实际得到 %d 段", len(got))
	}
	if d := got[0].end.Sub(got[0].start); d < 25*time.Minute {
		t.Errorf("合并后的区间应覆盖约 30 分钟，实际 %v", d)
	}
}

// TestAggregateUsesExplicitTotal 是本文件最重要的一条。
//
// 域内总节点数如果只从"出现过的节点"推断，会**系统性高估**故障比例：
// 64 个节点里挂了 30 个（47%）本该不算域级故障，但如果只有 30 个节点
// 出现在输入里，分母就变成 30，比例算成 100%，直接误判。
// 这正是 -total 存在的理由，也是采集时最容易踩的坑。
func TestAggregateUsesExplicitTotal(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tMax := t0.Add(10 * time.Hour)

	var ivs []nodeOutage
	for i := 0; i < 30; i++ {
		n := mk("n", "az-a", 60, 120)
		n.node = "az-a-node-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		ivs = append(ivs, n)
	}

	// ① 不给 -total：分母被推断为 30 → 比例 100% → 误判为域级故障
	got, _ := aggregate(ivs, nil, 0.5, time.Minute, 2*time.Minute, t0, tMax)
	if len(got) != 1 {
		t.Fatalf("前置条件不成立：不指定 total 时应当（错误地）判定为域级故障，实际 %d 段", len(got))
	}

	// ② 给 -total az-a=64：比例 30/64 = 47% < 50% → 正确判定为非域级故障
	got2, _ := aggregate(ivs, map[string]int{"az-a": 64}, 0.5, time.Minute, 2*time.Minute, t0, tMax)
	if len(got2) != 0 {
		t.Errorf("指定 total=64 后，30/64=47%% 不应算作域级故障，实际得到 %d 段", len(got2))
	}
}

// TestAggregateSkipsPlanned 确认 planned 维护被排除（在读取阶段过滤，
// 这里验证的是主流程对空输入的处理）。
func TestAggregateHandlesEmptyInput(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err := aggregate(nil, nil, 0.5, time.Minute, 2*time.Minute, t0, t0.Add(time.Hour))
	if err != nil {
		t.Fatalf("空输入不应报错: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("空输入应得到空结果，实际 %d 段", len(got))
	}
}

func TestParseTotal(t *testing.T) {
	m, err := parseTotal("az-a=64, az-b=32")
	if err != nil {
		t.Fatal(err)
	}
	if m["az-a"] != 64 || m["az-b"] != 32 {
		t.Errorf("解析结果不对: %v", m)
	}
	if _, err := parseTotal("az-a"); err == nil {
		t.Error("缺少 = 应当报错")
	}
	if _, err := parseTotal("az-a=x"); err == nil {
		t.Error("非整数计数应当报错")
	}
}

func TestMergeIntervals(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	in := []domInterval{
		{"a", t0.Add(0), t0.Add(10 * time.Minute)},
		{"a", t0.Add(11 * time.Minute), t0.Add(20 * time.Minute)},  // 间隔 1 分钟
		{"a", t0.Add(60 * time.Minute), t0.Add(70 * time.Minute)}, // 间隔 40 分钟
	}
	got := mergeIntervals(in, 2*time.Minute)
	if len(got) != 2 {
		t.Fatalf("2 分钟容差下应合并成 2 段，实际 %d 段", len(got))
	}
	if d := got[0].end.Sub(got[0].start); d != 20*time.Minute {
		t.Errorf("合并后第一段应为 20 分钟，实际 %v", d)
	}
}
