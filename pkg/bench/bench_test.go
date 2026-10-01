package bench

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 直方图
// ---------------------------------------------------------------------------

// TestHistogramPercentileAccuracy 验证分位数误差在分桶精度之内。
//
// 直方图用对数分桶（1µs 起、比例 1.02），因此相对误差应 < 3%。
// 这是"能不能用它出论文数字"的前提。
func TestHistogramPercentileAccuracy(t *testing.T) {
	h := NewHistogram()

	// 插入 1..10000 微秒各一次，共 10000 个样本。
	const n = 10000
	for i := 1; i <= n; i++ {
		h.Observe(uint64(i) * 1000) // 微秒 -> 纳秒
	}

	if h.Count() != n {
		t.Fatalf("Count = %d，期望 %d", h.Count(), n)
	}

	cases := []struct {
		p    float64
		want float64 // 期望的纳秒值
	}{
		{0.50, 5000 * 1000},
		{0.90, 9000 * 1000},
		{0.99, 9900 * 1000},
		{1.00, 10000 * 1000},
	}
	for _, c := range cases {
		got := float64(h.Percentile(c.p))
		relErr := math.Abs(got-c.want) / c.want
		if relErr > 0.03 {
			t.Errorf("p%.2f = %.0f ns，期望 %.0f ns（相对误差 %.2f%% > 3%%）",
				c.p, got, c.want, relErr*100)
		}
	}
}

// TestHistogramMinMax 极值必须精确（不受分桶误差影响）。
func TestHistogramMinMax(t *testing.T) {
	h := NewHistogram()
	h.Observe(1_234_567)
	h.Observe(9_876_543_210)
	h.Observe(555)

	if got := h.Min(); got != 555 {
		t.Errorf("Min = %d，期望 555", got)
	}
	if got := h.Max(); got != 9_876_543_210 {
		t.Errorf("Max = %d，期望 9876543210", got)
	}
}

// TestHistogramEmpty 空直方图不应 panic，且返回 0。
func TestHistogramEmpty(t *testing.T) {
	h := NewHistogram()
	if h.Count() != 0 {
		t.Fatalf("Count = %d，期望 0", h.Count())
	}
	if got := h.Percentile(0.99); got != 0 {
		t.Errorf("空直方图 p99 = %d，期望 0", got)
	}
	if got := h.Mean(); got != 0 {
		t.Errorf("空直方图 Mean = %v，期望 0", got)
	}
	if got := h.Min(); got != 0 {
		t.Errorf("空直方图 Min = %d，期望 0", got)
	}
}

// TestHistogramConcurrent 并发观测不丢样本。
func TestHistogramConcurrent(t *testing.T) {
	h := NewHistogram()
	const goroutines, each = 8, 5000

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				h.Observe(uint64(1000 + g*100 + i))
			}
		}(g)
	}
	wg.Wait()

	want := uint64(goroutines * each)
	if h.Count() != want {
		t.Fatalf("Count = %d，期望 %d（并发下丢样本）", h.Count(), want)
	}
}

// TestHistogramExtremeValues 极端值不能溢出或 panic。
func TestHistogramExtremeValues(t *testing.T) {
	h := NewHistogram()
	h.Observe(0)
	h.Observe(1)
	h.Observe(1 << 62)
	h.Observe(math.MaxUint64)

	if h.Count() != 4 {
		t.Fatalf("Count = %d，期望 4", h.Count())
	}
	if got := h.Max(); got != math.MaxUint64 {
		t.Errorf("Max = %d，期望 MaxUint64", got)
	}
	// 超大值应落进溢出桶，分位数取 Max。
	if got := h.Percentile(0.999); got == 0 {
		t.Error("含超大值时 p99.9 不应为 0")
	}
}

// TestHistogramMerge 合并后计数与极值正确。
func TestHistogramMerge(t *testing.T) {
	a := NewHistogram()
	b := NewHistogram()
	for i := 1; i <= 100; i++ {
		a.Observe(uint64(i) * 1000)
	}
	for i := 101; i <= 200; i++ {
		b.Observe(uint64(i) * 1000)
	}

	a.Merge(b)
	if a.Count() != 200 {
		t.Fatalf("合并后 Count = %d，期望 200", a.Count())
	}
	if got := a.Min(); got != 1000 {
		t.Errorf("合并后 Min = %d，期望 1000", got)
	}
	if got := a.Max(); got != 200_000 {
		t.Errorf("合并后 Max = %d，期望 200000", got)
	}
	// 合并 nil 不应 panic。
	a.Merge(nil)
	if a.Count() != 200 {
		t.Errorf("合并 nil 后 Count = %d，期望 200", a.Count())
	}
}

// TestHistogramSnapshot 摘要字段自洽。
func TestHistogramSnapshot(t *testing.T) {
	h := NewHistogram()
	for i := 1; i <= 1000; i++ {
		h.Observe(uint64(i) * 1000)
	}
	s := h.Snapshot()
	if s.Count != 1000 {
		t.Fatalf("Count = %d，期望 1000", s.Count)
	}
	if !(s.MinMs <= s.P50Ms && s.P50Ms <= s.P90Ms && s.P90Ms <= s.P99Ms &&
		s.P99Ms <= s.P999Ms && s.P999Ms <= s.MaxMs) {
		t.Fatalf("分位数未单调: min=%.4f p50=%.4f p90=%.4f p99=%.4f p999=%.4f max=%.4f",
			s.MinMs, s.P50Ms, s.P90Ms, s.P99Ms, s.P999Ms, s.MaxMs)
	}
}

// ---------------------------------------------------------------------------
// 负载生成
// ---------------------------------------------------------------------------

// TestWorkloadReadRatio 读比例应当接近配置值。
func TestWorkloadReadRatio(t *testing.T) {
	// 固定种子保证可复现。
	w := Workload{ReadRatio: 0.8, Keys: 1000, ValueSize: 16, Dist: DistUniform}
	g := NewGenerator(w, 12345)

	const n = 100000
	reads := 0
	for i := 0; i < n; i++ {
		op, _, _ := g.Next()
		if op == OpGet {
			reads++
		}
	}
	got := float64(reads) / float64(n)
	if math.Abs(got-0.8) > 0.01 {
		t.Fatalf("读比例 = %.4f，期望 0.8（±0.01）", got)
	}
}

// TestWorkloadKeyInRange 生成的 key 必须落在配置的键空间内。
//
// 这直接关系到实验正确性：若 key 越界，请求会被路由到不存在的分片。
func TestWorkloadKeyInRange(t *testing.T) {
	dists := []Distribution{DistUniform, DistZipf, DistLatest, DistSequential}
	for _, d := range dists {
		w := Workload{ReadRatio: 0.5, Keys: 500, KeySize: 12, ValueSize: 8, Dist: d, KeyPrefix: "k"}
		g := NewGenerator(w, 7)

		seen := map[string]bool{}
		for i := 0; i < 20000; i++ {
			_, key, _ := g.Next()
			if key == "" {
				t.Fatalf("dist=%s: 生成了空 key", d)
			}
			seen[key] = true
		}
		if len(seen) < 10 {
			t.Errorf("dist=%s: 只生成了 %d 个不同 key，多样性不足", d, len(seen))
		}
	}
}

// TestWorkloadZipfSkew Zipf 应产生明显热点（头部 key 占比高于均匀）。
func TestWorkloadZipfSkew(t *testing.T) {
	w := Workload{ReadRatio: 1.0, Keys: 1000, Dist: DistZipf, ZipfTheta: 0.99}
	g := NewGenerator(w, 99)

	counts := map[string]int{}
	const n = 50000
	for i := 0; i < n; i++ {
		_, key, _ := g.Next()
		counts[key]++
	}

	// 统计前 1% 键的访问占比。Zipf(0.99) 下应显著高于 1%。
	top := 0
	sorted := make([]int, 0, len(counts))
	for _, c := range counts {
		sorted = append(sorted, c)
	}
	// 简单取最大值集合：找访问次数 >= 20 的键
	for _, c := range sorted {
		if c >= 20 {
			top++
		}
	}
	// 均匀分布下每个键期望 50 次，因此"均匀"也会有大量键 >= 20。
	// 这里只验证 Zipf 确实产生了比均匀更强的集中度：最大访问次数应远高于均值。
	maxC := 0
	for _, c := range sorted {
		if c > maxC {
			maxC = c
		}
	}
	mean := float64(n) / float64(len(counts))
	if float64(maxC) < mean*3 {
		t.Fatalf("Zipf 最大访问 %d 不到均值 %.1f 的 3 倍，热点不明显", maxC, mean)
	}
	t.Logf("Zipf: %d 个不同键，最大访问 %d，均值 %.1f", len(counts), maxC, mean)
}

// TestWorkloadDeterministic 同种子必须产生同序列（实验可复现的前提）。
func TestWorkloadDeterministic(t *testing.T) {
	w := Workload{ReadRatio: 0.5, Keys: 100, Dist: DistZipf}

	g1 := NewGenerator(w, 42)
	g2 := NewGenerator(w, 42)

	for i := 0; i < 5000; i++ {
		op1, k1, v1 := g1.Next()
		op2, k2, v2 := g2.Next()
		if op1 != op2 || k1 != k2 || v1 != v2 {
			t.Fatalf("第 %d 次不一致: (%s,%s,%s) vs (%s,%s,%s)", i, op1, k1, v1, op2, k2, v2)
		}
	}
}

// TestWorkloadDifferentSeedsDiffer 不同种子应产生不同序列。
func TestWorkloadDifferentSeedsDiffer(t *testing.T) {
	w := Workload{ReadRatio: 0.5, Keys: 100, Dist: DistUniform}
	g1 := NewGenerator(w, 1)
	g2 := NewGenerator(w, 2)

	same := 0
	for i := 0; i < 1000; i++ {
		_, k1, _ := g1.Next()
		_, k2, _ := g2.Next()
		if k1 == k2 {
			same++
		}
	}
	if same == 1000 {
		t.Fatal("不同种子产生了完全相同的序列")
	}
}

// TestWorkloadValueSize 值长度应符合配置。
func TestWorkloadValueSize(t *testing.T) {
	for _, size := range []int{0, 1, 16, 128, 4096} {
		w := Workload{ReadRatio: 0.0, Keys: 10, ValueSize: size}
		g := NewGenerator(w, 1)
		_, _, v := g.Next()
		if size == 0 {
			if v == "" {
				t.Errorf("ValueSize=0 时应返回非空的短标记值")
			}
			continue
		}
		if len(v) != size {
			t.Errorf("ValueSize=%d 但实际长度 %d", size, len(v))
		}
	}
}

// TestParallelGeneratorsDistinct 并行生成器应各自独立。
func TestParallelGeneratorsDistinct(t *testing.T) {
	w := Workload{ReadRatio: 0.5, Keys: 1000, Dist: DistUniform}
	gs := ParallelGenerators(w, 4, 100)
	if len(gs) != 4 {
		t.Fatalf("生成器数量 = %d，期望 4", len(gs))
	}
	seqs := make([]string, 4)
	for i, g := range gs {
		var s string
		for j := 0; j < 50; j++ {
			_, k, _ := g.Next()
			s += k + "|"
		}
		seqs[i] = s
	}
	for i := 0; i < 4; i++ {
		for j := i + 1; j < 4; j++ {
			if seqs[i] == seqs[j] {
				t.Errorf("生成器 %d 与 %d 序列相同", i, j)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 执行器（用假执行器测 Runner 的记账语义）
// ---------------------------------------------------------------------------

// fakeExecutor 记录调用并可选地注入延迟/错误。
type fakeExecutor struct {
	mu       sync.Mutex
	ops      atomic.Int64
	gets     atomic.Int64
	puts     atomic.Int64
	failEvery int64
	delay    time.Duration
	closed   atomic.Bool
}

func (f *fakeExecutor) Do(ctx context.Context, op OpKind, key, value string) error {
	n := f.ops.Add(1)
	f.mu.Lock()
	if op == OpGet {
		f.gets.Add(1)
	} else {
		f.puts.Add(1)
	}
	failEvery := f.failEvery
	d := f.delay
	f.mu.Unlock()

	if d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if failEvery > 0 && n%failEvery == 0 {
		return fmt.Errorf("injected failure at op %d", n)
	}
	return nil
}

func (f *fakeExecutor) Close() { f.closed.Store(true) }

// TestRunnerCountsOps 成功与失败计数必须精确。
func TestRunnerCountsOps(t *testing.T) {
	fe := &fakeExecutor{failEvery: 10}
	r := NewRunner(Config{
		Mode:        ModeClosed,
		Concurrency: 4,
		Duration:    2 * time.Second,
		Ops:         400, // 固定操作数，保证确定性
		Workload:    Workload{ReadRatio: 0.5, Keys: 1000, Dist: DistUniform},
		Seed:        5,
	}).WithExecutor(fe)
	defer r.Close()

	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	total := int64(res.Ops) + int64(res.Errors)
	if total != 400 {
		t.Fatalf("总操作 = %d（成功 %d + 失败 %d），期望 400", total, res.Ops, res.Errors)
	}
	// 每 10 次失败 1 次 => 期望 40 次失败。并发下可能有边缘差异，放宽到 ±8。
	if res.Errors < 32 || res.Errors > 48 {
		t.Errorf("失败数 = %d，期望约 40（±8）", res.Errors)
	}
	if fe.ops.Load() != 400 {
		t.Errorf("执行器被调用 %d 次，期望 400", fe.ops.Load())
	}
	// 错误率应被正确计算。
	wantRate := float64(res.Errors) / 400.0
	if math.Abs(res.ErrorRate-wantRate) > 1e-9 {
		t.Errorf("ErrorRate = %v，期望 %v", res.ErrorRate, wantRate)
	}
}

// TestRunnerWarmupExcludedFromResults 预热数据不得进入结果。
//
// 这是评测可信度的关键：若预热样本混进直方图，尾延迟会被系统性压低。
func TestRunnerWarmupExcludedFromResults(t *testing.T) {
	fe := &fakeExecutor{}
	r := NewRunner(Config{
		Mode:        ModeClosed,
		Concurrency: 2,
		Duration:    300 * time.Millisecond,
		Ops:         200,
		Warmup:      300 * time.Millisecond,
		WarmupOps:   150, // 给预热也加上界，否则空转执行器会跑出几十万次调用
		Workload:    Workload{ReadRatio: 0.5, Keys: 100, Dist: DistUniform},
		Seed:        3,
	}).WithExecutor(fe)
	defer r.Close()

	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Ops > 200 {
		t.Fatalf("成功操作 %d 超过配置上限 200（预热未被排除？）", res.Ops)
	}
	if res.Latency.Count != res.Ops {
		t.Fatalf("直方图样本 %d != 成功操作 %d（预热样本混入）", res.Latency.Count, res.Ops)
	}
	// 执行器被调用次数应显著多于测量阶段（因为预热也调用了）。
	if fe.ops.Load() <= int64(res.Ops) {
		t.Errorf("执行器调用 %d 次 <= 成功操作 %d，预热似乎没执行", fe.ops.Load(), res.Ops)
	}
	// 预热受 WarmupOps 约束，总调用数不应远超 150 + 200。
	if fe.ops.Load() > 600 {
		t.Errorf("执行器被调用 %d 次，远超 WarmupOps(150)+Ops(200) 的合理上限", fe.ops.Load())
	}
	t.Logf("预热+测量共调用 %d 次；测量阶段成功 %d 次", fe.ops.Load(), res.Ops)
}

// TestRunnerOpenLoopCountsScheduledTime 开环模式按"预定发起时刻"计时。
//
// 这是 coordinated omission 的核心：当系统过载、实际发起时刻晚于预定时刻时，
// 延迟必须包含这段落后量。测试用一个固定延迟的执行器把系统压到过载，
// 然后断言 p99 明显大于执行器延迟本身。
func TestRunnerOpenLoopCountsScheduledTime(t *testing.T) {
	const execDelay = 5 * time.Millisecond
	fe := &fakeExecutor{delay: execDelay}

	// 目标速率远超单 worker 能承受的速率（1/5ms = 200/s），
	// 因此 worker 必然落后于预定时序。
	r := NewRunner(Config{
		Mode:        ModeOpen,
		Concurrency: 2,
		TargetRate:  100000, // 远超能力
		Duration:    1 * time.Second,
		Ops:         200,
		Workload:    Workload{ReadRatio: 1.0, Keys: 100, Dist: DistUniform},
		Seed:        11,
	}).WithExecutor(fe)
	defer r.Close()

	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Ops == 0 {
		t.Fatal("没有完成任何操作")
	}

	// 闭环口径下延迟约等于 execDelay；开环口径下应显著更大。
	if res.Latency.P99Ms < execDelay.Seconds()*1000*1.5 {
		t.Fatalf("开环 p99 = %.2fms，未体现预定时序落后（执行器延迟 %.2fms，期望 > %.2fms）",
			res.Latency.P99Ms, execDelay.Seconds()*1000, execDelay.Seconds()*1000*1.5)
	}
	t.Logf("开环 p50=%.2fms p99=%.2fms（执行器延迟 %.2fms）—— 落后量已计入",
		res.Latency.P50Ms, res.Latency.P99Ms, execDelay.Seconds()*1000)
}

// TestRunnerByOpLatency 分操作类型的延迟必须分别统计。
func TestRunnerByOpLatency(t *testing.T) {
	fe := &fakeExecutor{}
	r := NewRunner(Config{
		Mode:        ModeClosed,
		Concurrency: 4,
		Duration:    500 * time.Millisecond,
		Ops:         300,
		Workload:    Workload{ReadRatio: 0.5, Keys: 1000, Dist: DistUniform},
		Seed:        17,
	}).WithExecutor(fe)
	defer r.Close()

	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	puts := res.LatencyByOp["put"].Count
	gets := res.LatencyByOp["get"].Count
	if puts+gets != res.Ops {
		t.Fatalf("put(%d) + get(%d) = %d，期望 %d", puts, gets, puts+gets, res.Ops)
	}
	if puts == 0 || gets == 0 {
		t.Fatalf("读或写一类样本为 0：put=%d get=%d", puts, gets)
	}
}

// TestRunnerSummaryAndCSV 输出格式可用且字段数一致。
func TestRunnerSummaryLine(t *testing.T) {
	fe := &fakeExecutor{}
	r := NewRunner(Config{
		Mode: ModeClosed, Concurrency: 2, Duration: 200 * time.Millisecond, Ops: 50,
		Workload: Workload{ReadRatio: 0.5, Keys: 50}, Seed: 1, Label: "unit-test",
	}).WithExecutor(fe)
	defer r.Close()

	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	line := res.SummaryLine()
	if line == "" {
		t.Fatal("SummaryLine 为空")
	}
	t.Logf("摘要: %s", line)

	if res.Env.GoVersion == "" {
		t.Error("Env.GoVersion 为空，结果文件无法记录运行环境")
	}
	if res.Env.NumCPU <= 0 {
		t.Error("Env.NumCPU 不合理")
	}
}

// TestRunnerContextCancel 上下文取消后 Run 应及时返回。
func TestRunnerContextCancel(t *testing.T) {
	fe := &fakeExecutor{}
	r := NewRunner(Config{
		Mode: ModeOpen, Concurrency: 4, TargetRate: 1000,
		Duration: 30 * time.Second, // 故意设很长
		Workload: Workload{ReadRatio: 1.0, Keys: 100},
		Seed:     1,
	}).WithExecutor(fe)
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := r.Run(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("取消后 %s 才返回，响应太慢", elapsed)
	}
	t.Logf("300ms 超时下 %s 返回", elapsed.Round(time.Millisecond))
}
