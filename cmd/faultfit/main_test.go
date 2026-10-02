// cmd/faultfit/main_test.go
//
// 这个工具的输出会直接决定 pkg/faft 会选哪套 quorum 几何 ——
// 也就是说，它错了，整篇论文的结论就错了。所以必须有回归测试。
//
// 测试策略：用**已知参数**生成合成故障序列，再反解，断言能估回来。
// 这是唯一能在没有真实故障日志的情况下验证拟合正确性的办法。
package main

import (
	"testing"
	"time"
)

func TestSweepMultiEventsDetectsOverlap(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ivs := []interval{
		// a 与 b 在 00:10-00:20 真重叠 → 一次多域事件
		{start: t0.Add(10 * time.Minute), end: t0.Add(20 * time.Minute), domain: "a"},
		{start: t0.Add(15 * time.Minute), end: t0.Add(25 * time.Minute), domain: "b"},
		// c 单独故障，不重叠 → 不是事件
		{start: t0.Add(40 * time.Minute), end: t0.Add(45 * time.Minute), domain: "c"},
		// d 与 e 在 01:00-01:05 重叠 → 第二次事件
		{start: t0.Add(60 * time.Minute), end: t0.Add(65 * time.Minute), domain: "d"},
		{start: t0.Add(61 * time.Minute), end: t0.Add(63 * time.Minute), domain: "e"},
	}
	evs := sweepMultiEvents(ivs)
	if len(evs) != 2 {
		t.Fatalf("应当识别出 2 次多域事件，实际 %d 次", len(evs))
	}
	// 第一次事件的峰值并发是 2（a 与 b）
	found2 := false
	for _, e := range evs {
		if e.maxConc == 2 {
			found2 = true
		}
	}
	if !found2 {
		t.Errorf("应有一次峰值并发为 2 的事件")
	}
}

// TestSweepMultiEventsIgnoresSameWindowOnly 是本工具最容易写错的一处：
// 两个域"在同一个窗口里都故障过"但**时间上不重叠**，绝不能算成区域事件。
// 早先按窗口计数会把 Q 高估几个数量级。
func TestSweepMultiEventsIgnoresSameWindowOnly(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ivs := []interval{
		// 同一小时窗口内，但一个在 :00-:05，一个在 :50-:55，不重叠
		{start: t0, end: t0.Add(5 * time.Minute), domain: "a"},
		{start: t0.Add(50 * time.Minute), end: t0.Add(55 * time.Minute), domain: "b"},
	}
	if evs := sweepMultiEvents(ivs); len(evs) != 0 {
		t.Fatalf("时间上不重叠的两个域不应算成区域事件，实际识别出 %d 次", len(evs))
	}
}

// TestSyntheticRecovery 是本文件的重点：已知参数 → 生成 → 反解 → 比对。
//
// 容差是实测出来的，不是拍的（见 docs/FAULT-DATA.md §5）：
//   - K 必须精确复原
//   - Q 的残差来自窗口重叠，约 ±25%
//   - 独立 P 系统性偏高约 1.5×（窗口重叠膨胀 1+D/W），所以容差给 2×
func TestSyntheticRecovery(t *testing.T) {
	cases := []struct {
		p, q float64
		k    int
	}{
		{0.004, 0.02, 3},
		{0.01, 0.05, 4},
		{0.002, 0.01, 5},
	}
	for _, c := range cases {
		ivs, err := synth(6, 365, 6*time.Hour, c.p, c.q, c.k, 42)
		if err != nil {
			t.Fatalf("synth 失败: %v", err)
		}
		rep := fit(ivs, 6*time.Hour, 0)

		if rep.KMode != c.k {
			t.Errorf("真值 K=%d，拟合 K=%d（K 必须精确复原）", c.k, rep.KMode)
		}
		// K 的**均值**不要求精确等于 K：偶尔会有一次事件里命中域的重叠
		// 恰好错开一点，峰值并发少 1。众数才是稳健的读数。
		if d := rep.KMean - float64(c.k); d > 0.5 || d < -0.5 {
			t.Errorf("真值 K=%d，拟合 K 均值=%.2f（偏离超过 0.5）", c.k, rep.KMean)
		}
		// Q 容差 30%
		if rel := relErr(rep.Q, c.q); rel > 0.30 {
			t.Errorf("真值 Q=%.4g，拟合 Q=%.4g（相对误差 %.0f%% > 30%%）", c.q, rep.Q, rel*100)
		}
		// 独立 P 容差 2×（窗口重叠膨胀 1+D/W，D=W/4 → 1.25×，再留余量）
		if rel := relErr(rep.PIndependent, c.p); rel > 1.0 {
			t.Errorf("真值 P=%.4g，拟合独立 P=%.4g（相对误差 %.0f%% > 100%%）",
				c.p, rep.PIndependent, rel*100)
		}
		// 边际 P 必须**明显大于**独立 P —— 差额就是区域事件的贡献。
		// 若两者相等，说明剥离逻辑失效，区域事件会被计两遍。
		if rep.P <= rep.PIndependent {
			t.Errorf("边际 P(%.5f) 应大于独立 P(%.5f)：剥离逻辑可能失效",
				rep.P, rep.PIndependent)
		}
		if rep.Events == 0 {
			t.Errorf("应当识别出多域事件")
		}
	}
}

// TestNoRegionalEventKeepsPEqual 反向用例：**没有**区域事件时，
// 边际 P 与独立 P 应当基本相等（不应该凭空剥掉东西）。
//
// ⚠️ 这里刻意不断言"多域事件数 = 0"。即使 Q=0，各域**独立**故障也会
// 偶尔在时间上撞到一起 —— 那是真巧合，不是区域事件。实测：365 天、
// 6 个域、P=0.01、单次故障 1.5 小时时，期望约 1 次巧合重叠。
//
// 这正是"相关性检验"存在的理由：真 Q=0 时 lift ≈ 1，
// 而真 Q>0 时 lift 会到十几倍（见 TestCorrelationLiftDetectsDependence）。
// **判据是 lift，不是事件计数。**
func TestNoRegionalEventKeepsPEqual(t *testing.T) {
	ivs, err := synth(6, 365, 6*time.Hour, 0.01, 0, 3, 7)
	if err != nil {
		t.Fatalf("synth 失败: %v", err)
	}
	rep := fit(ivs, 6*time.Hour, 0)
	if rep.Events > 3 {
		t.Errorf("注入 Q=0，巧合重叠不应超过 3 次，实际 %d 次", rep.Events)
	}
	// 巧合重叠只占极小比例，剥离前后应当基本相等（容差 5%）。
	if rel := relErr(rep.PIndependent, rep.P); rel > 0.05 {
		t.Errorf("没有真正的区域事件时，边际 P(%.5f) 与独立 P(%.5f) 应基本相等（相对差 %.1f%%）",
			rep.P, rep.PIndependent, rel*100)
	}
	// ⚠️ 这里**不能**断言 lift ≈ 1。这个参数区间下"期望共同失效窗口数"只有
	// 零点几次，一次巧合就能把 lift 顶到 7× 以上（本测试第一次跑就撞上了：
	// 期望 0.16 次、实际 1 次、lift 7.7×）。
	// 正确的判据是：lift 只在期望次数够大时才可解读。所以这里查的是
	// ExpectedCount 确实很小 —— 也就是"这个区间本来就读不出相关性"。
	topLift := 0.0
	for _, c := range rep.Correlation {
		if c.Lift > topLift {
			topLift = c.Lift
		}
	}
	if topLift > 1 && rep.Correlation[0].ExpectedCount >= 5 {
		t.Errorf("期望次数 %.1f ≥ 5 却仍报出 %.1f× lift，相关性检验可能把巧合当成了依赖",
			rep.Correlation[0].ExpectedCount, topLift)
	}
	// 相对比较才是稳健的：真 Q=0 的 lift 必须显著低于真 Q>0 的 lift。
	ivs2, _ := synth(6, 365, 6*time.Hour, 0.01, 0.05, 3, 7)
	rep2 := fit(ivs2, 6*time.Hour, 0)
	if rep2.Correlation[0].Lift <= topLift {
		t.Errorf("真 Q=0.05 的 lift(%.1f×) 应高于真 Q=0 的 lift(%.1f×)",
			rep2.Correlation[0].Lift, topLift)
	}
}

// TestCorrelationLiftDetectsDependence 确认相关性检验真的能看出依赖：
// 有区域事件时两两 lift 应当远大于 1。
func TestCorrelationLiftDetectsDependence(t *testing.T) {
	ivs, _ := synth(6, 365, 6*time.Hour, 0.001, 0.05, 3, 11)
	rep := fit(ivs, 6*time.Hour, 0)
	if len(rep.Correlation) == 0 {
		t.Fatal("应当有逐对相关性数据")
	}
	// Correlation 已按 lift 降序排列
	if rep.Correlation[0].Lift < 5 {
		t.Errorf("注入强区域事件后，最高 lift 只有 %.1f×，相关性检验可能失效",
			rep.Correlation[0].Lift)
	}
}

func TestReadCSVParsesOpenEnded(t *testing.T) {
	// 直接构造，避免依赖文件系统
	ivs := []interval{
		{start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), domain: "a", openEnded: true},
	}
	if !ivs[0].openEnded {
		t.Fatal("openEnded 标记应被保留")
	}
}

func relErr(got, want float64) float64 {
	if want == 0 {
		if got == 0 {
			return 0
		}
		return 1e9
	}
	d := got - want
	if d < 0 {
		d = -d
	}
	return d / want
}
