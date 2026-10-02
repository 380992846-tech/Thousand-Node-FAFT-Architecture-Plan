// pkg/bench/runner.go
//
// 实验执行器：把负载打到集群上，采集吞吐与延迟分布，输出可复现的结果文件。
//
// 设计要点（这些正是评测可信度的来源）：
//
//  1. 显式记录发压模式（closed/open），并对 open-loop 使用"预定发起时刻"计时，
//     从而不掩盖 coordinated omission 造成的尾延迟。
//  2. 预热阶段与测量阶段分离，预热数据不进入结果。
//  3. 每个 worker 独立直方图，最后合并——避免用一个全局锁把测量本身变成瓶颈。
//  4. 结果包含完整的环境与配置元数据，使一次运行可以被别人重放。
package bench

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Config 实验配置。
type Config struct {
	// Cluster 集群规模描述（写入结果，便于归因）。
	Shards   int
	Replicas int
	Learners int

	// Workload 负载。
	Workload Workload
	// Mode 发压模式。
	Mode Mode
	// Concurrency closed-loop 的并发数；open-loop 的 worker 数。
	Concurrency int
	// TargetRate open-loop 目标速率（ops/s）；0 表示不限速（尽力而为）。
	TargetRate int

	// Duration 测量时长。与 Ops 二者取先到者。
	Duration time.Duration
	// Ops 测量阶段的目标操作数；0 表示仅按 Duration。
	Ops int
	// Warmup 预热时长。
	Warmup time.Duration
	// WarmupOps 预热阶段的操作数上限；0 表示只按 Warmup 时长。
	WarmupOps int

	// Endpoints 数据面地址（router 或某个节点）。
	Endpoints []string
	// Seed 随机种子。
	Seed int64
	// Label 本次运行的标签（写入结果）。
	Label string
	// Notes 自由文本备注（例如"非持久化配置"）。
	Notes string

	// SampleInterval 采样间隔，用于输出时间序列（0 表示不采样）。
	SampleInterval time.Duration
}

func (c Config) withDefaults() Config {
	if c.Concurrency <= 0 {
		c.Concurrency = 16
	}
	if c.Mode == "" {
		c.Mode = ModeClosed
	}
	if c.Duration <= 0 {
		c.Duration = 10 * time.Second
	}
	if c.Seed == 0 {
		c.Seed = 42
	}
	if len(c.Endpoints) == 0 {
		c.Endpoints = []string{"127.0.0.1:8080"}
	}
	return c
}

// Sample 时间序列的一个采样点。
type Sample struct {
	AtMs       int64   `json:"at_ms"`
	WindowOps  uint64  `json:"window_ops"`
	Throughput float64 `json:"throughput"`
	P99Ms      float64 `json:"p99_ms"`
}

// Result 一次实验的结果。
type Result struct {
	Label   string `json:"label"`
	Mode    string `json:"mode"`
	Notes   string `json:"notes"`
	Cluster struct {
		Shards   int `json:"shards"`
		Replicas int `json:"replicas"`
		Learners int `json:"learners"`
		Nodes    int `json:"nodes"`
	} `json:"cluster"`
	Workload   string `json:"workload"`
	Concurrency int   `json:"concurrency"`
	TargetRate  int   `json:"target_rate"`

	Ops        uint64  `json:"ops"`
	Errors     uint64  `json:"errors"`
	ErrorRate  float64 `json:"error_rate"`
	DurationMs float64 `json:"duration_ms"`
	Throughput float64 `json:"throughput"`

	// ErrorClasses 错误分类计数。
	//
	// 为什么必须有：只报 "errors=983" 是**没法排查**的 —— 是客户端 500ms 超时？
	// 是 503 非 Leader？是连接被拒？三者的含义完全不同，混在一个数字里
	// 会让人把"压测客户端扛不住"读成"集群出问题了"。
	// 实测就是这么撞上的：单分片并发 32 时报 44% 错误，而并发 16 时 0 错误，
	// 不分类根本看不出原因。
	ErrorClasses map[string]int `json:"error_classes,omitempty"`

	Latency Snapshot `json:"latency"`

	// LatencyByOp 分操作类型的延迟。
	LatencyByOp map[string]Snapshot `json:"latency_by_op"`

	Samples []Sample `json:"samples,omitempty"`

	// 以下是 Multi-Raft 的结构量，不是性能量 —— 但它们决定性能数字怎么读。
	//
	// Leader 分布：每个节点当了几个分片的 Leader。
	// 随机选举会让少数节点承担大部分分片的写负载，成为瓶颈；
	// 所以"吞吐随分片数上升"必须与分布一起报，否则可能只是这一轮恰好均匀。
	LeaderDist map[string]int `json:"leader_distribution,omitempty"`
	// LeaderMax/LeaderMean：分布的不均衡度（最大 / 均值）。
	LeaderMax  int     `json:"leader_max,omitempty"`
	LeaderMean float64 `json:"leader_mean,omitempty"`
	// ShardsWithLeader：当前已经选出 Leader 的分片数。
	ShardsWithLeader int `json:"shards_with_leader,omitempty"`

	Env EnvInfo `json:"env"`
}

// EnvInfo 运行环境，用于可复现性记录。
type EnvInfo struct {
	GoVersion string `json:"go_version"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	NumCPU    int    `json:"num_cpu"`
	StartedAt string `json:"started_at"`
	Hostname  string `json:"hostname,omitempty"`
}

// Runner 执行实验。
type Runner struct {
	cfg Config

	latency *Histogram
	latPut  *Histogram
	latGet  *Histogram

	ops    atomic.Uint64
	errors atomic.Uint64

	// errMu/errClasses 错误分类计数（错误信息 → 次数）。
	// 用 map 而不是按类型分支：错误来自 HTTP 客户端、路由层、Raft 层三处，
	// 事先枚举不全；按文本归类能覆盖住所有实际出现过的错误。
	errMu      sync.Mutex
	errClasses map[string]int

	endpoints []string
	rr        atomic.Uint64

	exec Executor

	samplesMu sync.Mutex
	samples   []Sample
}

// NewRunner 创建执行器。
func NewRunner(cfg Config) *Runner {
	cfg = cfg.withDefaults()
	r := &Runner{
		cfg:       cfg,
		latency:   NewHistogram(),
		latPut:    NewHistogram(),
		latGet:    NewHistogram(),
		endpoints: cfg.Endpoints,
	}
	r.exec = NewHTTPExecutor(cfg.Endpoints, 10*time.Second, cfg.Concurrency*4)
	return r
}

// WithExecutor 替换底层执行器（用于单元测试或进程内直连实验）。
func (r *Runner) WithExecutor(e Executor) *Runner {
	r.exec = e
	return r
}

// Close 释放执行器资源。
func (r *Runner) Close() {
	if r.exec != nil {
		r.exec.Close()
	}
}

// nextEndpoint 轮询选择后端。
func (r *Runner) nextEndpoint() string {
	if len(r.endpoints) == 1 {
		return r.endpoints[0]
	}
	i := r.rr.Add(1)
	return r.endpoints[int(i)%len(r.endpoints)]
}

// noteError 记录一次错误及其分类。
//
// 分类键做归一化：去掉易变部分（端口号、时长、地址），
// 否则每一处不同的地址都会生成一个新键，分类表会爆掉、
// 也就失去了"一眼看出是哪类错误"的意义。
func (r *Runner) noteError(err error) {
	r.errors.Add(1)
	if err == nil {
		return
	}
	k := classifyError(err.Error())

	r.errMu.Lock()
	if r.errClasses == nil {
		r.errClasses = map[string]int{}
	}
	// 上限保护：真出现大量不同错误时，避免 map 无限增长。
	if len(r.errClasses) < 64 {
		r.errClasses[k]++
	}
	r.errMu.Unlock()
}

// classifyError 把错误文本归一化成分类键。
func classifyError(msg string) string {
	const maxLen = 160
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "…"
	}
	// 127.0.0.1:18001 → 127.0.0.1:PORT；"after 500ms" 之类也去掉数字。
	msg = reHostPort.ReplaceAllString(msg, "$1:PORT")
	msg = reDuration.ReplaceAllString(msg, "DURATION")
	return msg
}

var (
	reHostPort = regexp.MustCompile(`(127\.0\.0\.1|localhost|\[::1\]):\d+`)
	reDuration = regexp.MustCompile(`\b\d+(\.\d+)?(ns|µs|us|ms|s)\b`)
)

// Run 执行一次实验。
func (r *Runner) Run(ctx context.Context) (*Result, error) {
	started := time.Now()

	// 1. 预热。
	//
	// 预热阶段同样受 WarmupOps 约束（若配置了）。不设上界时，
	// 在一个"零成本"的执行器上 300ms 可以跑出 65 万次调用
	// （本包测试中实测），会白白烧 CPU 并把页面缓存搅乱。
	if r.cfg.Warmup > 0 {
		wctx, cancel := context.WithTimeout(ctx, r.cfg.Warmup)
		r.runPhase(wctx, r.cfg.Warmup, r.cfg.WarmupOps, true)
		cancel()
		// 预热数据清零，避免污染测量结果。
		r.latency = NewHistogram()
		r.latPut = NewHistogram()
		r.latGet = NewHistogram()
		r.ops.Store(0)
		r.errors.Store(0)
	}

	// 2. 测量。
	measureStart := time.Now()
	var stopSampler func()
	if r.cfg.SampleInterval > 0 {
		stopSampler = r.startSampler(ctx, measureStart)
	}

	r.runPhase(ctx, r.cfg.Duration, r.cfg.Ops, false)
	measureDur := time.Since(measureStart)
	if stopSampler != nil {
		stopSampler()
	}

	// 3. 汇总。
	ops := r.ops.Load()
	errs := r.errors.Load()

	res := &Result{
		Label:       r.cfg.Label,
		Mode:        string(r.cfg.Mode),
		Notes:       r.cfg.Notes,
		Workload:    r.cfg.Workload.Summary(),
		Concurrency: r.cfg.Concurrency,
		TargetRate:  r.cfg.TargetRate,
		Ops:         ops,
		Errors:      errs,
		DurationMs:  float64(measureDur.Microseconds()) / 1000.0,
		Latency:     r.latency.Snapshot(),
		LatencyByOp: map[string]Snapshot{
			"put": r.latPut.Snapshot(),
			"get": r.latGet.Snapshot(),
		},
		Env: r.envInfo(started),
	}
	res.Cluster.Shards = r.cfg.Shards
	res.Cluster.Replicas = r.cfg.Replicas
	res.Cluster.Learners = r.cfg.Learners
	res.Cluster.Nodes = r.cfg.Shards * (r.cfg.Replicas + r.cfg.Learners)

	if measureDur > 0 {
		res.Throughput = float64(ops) / measureDur.Seconds()
	}
	if total := ops + errs; total > 0 {
		res.ErrorRate = float64(errs) / float64(total)
	}

	r.samplesMu.Lock()
	res.Samples = append([]Sample(nil), r.samples...)
	r.samplesMu.Unlock()

	r.errMu.Lock()
	if len(r.errClasses) > 0 {
		res.ErrorClasses = make(map[string]int, len(r.errClasses))
		for k, v := range r.errClasses {
			res.ErrorClasses[k] = v
		}
	}
	r.errMu.Unlock()

	return res, nil
}

// runPhase 运行一个阶段。warmup 为 true 时不累计到结果计数器。
func (r *Runner) runPhase(ctx context.Context, dur time.Duration, targetOps int, warmup bool) {
	if r.cfg.Mode == ModeOpen {
		r.runOpen(ctx, dur, targetOps, warmup)
		return
	}
	r.runClosed(ctx, dur, targetOps, warmup)
}

// runClosed 闭循环：每个 worker 串行发请求。
func (r *Runner) runClosed(ctx context.Context, dur time.Duration, targetOps int, warmup bool) {
	deadline := time.Now().Add(dur)

	var wg sync.WaitGroup
	var issued atomic.Uint64

	for w := 0; w < r.cfg.Concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			gen := NewGenerator(r.cfg.Workload, r.cfg.Seed+int64(worker)*7919)
			lat := NewHistogram()
			latPut := NewHistogram()
			latGet := NewHistogram()
			defer func() {
				if !warmup {
					r.latency.Merge(lat)
					r.latPut.Merge(latPut)
					r.latGet.Merge(latGet)
				}
			}()

			for {
				if ctx.Err() != nil || time.Now().After(deadline) {
					return
				}
				if targetOps > 0 && issued.Load() >= uint64(targetOps) {
					return
				}
				op, key, val := gen.Next()
				n := issued.Add(1)
				if targetOps > 0 && n > uint64(targetOps) {
					return
				}

				t0 := time.Now()
				err := r.execute(ctx, op, key, val)
				el := uint64(time.Since(t0).Nanoseconds())

				if err != nil {
					if !warmup {
						r.noteError(err)
					}
					continue
				}
				if !warmup {
					lat.Observe(el)
					if op == OpPut {
						latPut.Observe(el)
					} else {
						latGet.Observe(el)
					}
					r.ops.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
}

// runOpen 开循环：按预定发起时刻（schedule）计算延迟，暴露 coordinated omission。
func (r *Runner) runOpen(ctx context.Context, dur time.Duration, targetOps int, warmup bool) {
	deadline := time.Now().Add(dur)

	// 每个 worker 按 stride 错开发起时刻。
	stride := time.Duration(0)
	if r.cfg.TargetRate > 0 {
		stride = time.Duration(float64(time.Second) * float64(r.cfg.Concurrency) / float64(r.cfg.TargetRate))
		if stride <= 0 {
			stride = time.Microsecond
		}
	}

	var wg sync.WaitGroup
	var issued atomic.Uint64

	for w := 0; w < r.cfg.Concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			gen := NewGenerator(r.cfg.Workload, r.cfg.Seed+int64(worker)*7919)
			lat := NewHistogram()
			latPut := NewHistogram()
			latGet := NewHistogram()
			defer func() {
				if !warmup {
					r.latency.Merge(lat)
					r.latPut.Merge(latPut)
					r.latGet.Merge(latGet)
				}
			}()

			next := time.Now().Add(time.Duration(worker) * stride / time.Duration(maxInt(r.cfg.Concurrency, 1)))

			for {
				now := time.Now()
				if ctx.Err() != nil || now.After(deadline) {
					return
				}
				if targetOps > 0 && issued.Load() >= uint64(targetOps) {
					return
				}

				// 等待到预定时刻。若已经落后（系统过载），立即发起，
				// 但**仍以预定时刻计时**，因此落后会如实体现在延迟里。
				if stride > 0 && next.After(now) {
					timer := time.NewTimer(next.Sub(now))
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						return
					}
				}
				scheduled := next

				op, key, val := gen.Next()
				n := issued.Add(1)
				if targetOps > 0 && n > uint64(targetOps) {
					return
				}

				err := r.execute(ctx, op, key, val)
				el := uint64(time.Since(scheduled).Nanoseconds())

				if err != nil {
					if !warmup {
						r.noteError(err)
					}
				} else if !warmup {
					lat.Observe(el)
					if op == OpPut {
						latPut.Observe(el)
					} else {
						latGet.Observe(el)
					}
					r.ops.Add(1)
				}

				if stride > 0 {
					next = next.Add(stride)
				} else {
					next = time.Now()
				}
			}
		}(w)
	}
	wg.Wait()
}

// startSampler 启动周期性吞吐采样（用于吞吐-时间曲线，便于发现稳态漂移）。
func (r *Runner) startSampler(ctx context.Context, start time.Time) func() {
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		t := time.NewTicker(r.cfg.SampleInterval)
		defer t.Stop()
		var last uint64
		var lastAt = start
		for {
			select {
			case <-sctx.Done():
				return
			case now := <-t.C:
				cur := r.ops.Load()
				dt := now.Sub(lastAt).Seconds()
				var tp float64
				if dt > 0 {
					tp = float64(cur-last) / dt
				}
				r.samplesMu.Lock()
				r.samples = append(r.samples, Sample{
					AtMs:       now.Sub(start).Milliseconds(),
					WindowOps:  cur - last,
					Throughput: tp,
					P99Ms:      nsToMs(float64(r.latency.Percentile(0.99))),
				})
				r.samplesMu.Unlock()
				last, lastAt = cur, now
			}
		}
	}()

	return func() { cancel(); <-done }
}

func (r *Runner) envInfo(started time.Time) EnvInfo {
	host, _ := os.Hostname()
	return EnvInfo{
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		StartedAt: started.Format(time.RFC3339),
		Hostname:  host,
	}
}

// WriteJSON 写出结果。
func (res *Result) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(res)
}

// WriteCSV 把结果追加为 CSV 的一行（表头固定，便于跨运行汇总）。
func (res *Result) WriteCSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	header := []string{
		"label", "mode", "shards", "replicas", "learners", "nodes",
		"workload", "concurrency", "target_rate",
		"ops", "errors", "error_rate", "duration_ms", "throughput",
		"p50_ms", "p90_ms", "p99_ms", "p99_9_ms", "max_ms", "mean_ms",
	}
	if err := cw.Write(header); err != nil {
		return err
	}
	row := []string{
		res.Label, res.Mode,
		strconv.Itoa(res.Cluster.Shards), strconv.Itoa(res.Cluster.Replicas),
		strconv.Itoa(res.Cluster.Learners), strconv.Itoa(res.Cluster.Nodes),
		res.Workload, strconv.Itoa(res.Concurrency), strconv.Itoa(res.TargetRate),
		strconv.FormatUint(res.Ops, 10), strconv.FormatUint(res.Errors, 10),
		formatFloat(res.ErrorRate), formatFloat(res.DurationMs), formatFloat(res.Throughput),
		formatFloat(res.Latency.P50Ms), formatFloat(res.Latency.P90Ms),
		formatFloat(res.Latency.P99Ms), formatFloat(res.Latency.P999Ms),
		formatFloat(res.Latency.MaxMs), formatFloat(res.Latency.MeanMs),
	}
	return cw.Write(row)
}

func formatFloat(f float64) string { return strconv.FormatFloat(f, 'f', 4, 64) }

// SummaryLine 单行摘要，用于终端输出。
func (res *Result) SummaryLine() string {
	return fmt.Sprintf("%-24s %-6s shards=%-3d r=%-2d %8.0f ops/s  p50=%.2fms p99=%.2fms p99.9=%.2fms  err=%.3f%%",
		res.Label, res.Mode, res.Cluster.Shards, res.Cluster.Replicas,
		res.Throughput, res.Latency.P50Ms, res.Latency.P99Ms, res.Latency.P999Ms,
		res.ErrorRate*100)
}

// SortResults 按吞吐降序排列。
func SortResults(rs []*Result) {
	sort.Slice(rs, func(i, j int) bool { return rs[i].Throughput > rs[j].Throughput })
}
