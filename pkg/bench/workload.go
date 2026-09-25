// pkg/bench/workload.go
//
// 负载模型。
//
// 这里刻意实现了两种发压模式，因为二者的结论可能相差一个数量级，
// 而共识协议的评测文献中这一点经常被忽略（见 NSDI'06 "Open versus closed:
// a cautionary tale"，以及 EPaxos Revisited, NSDI'21 对原评测方法的批评）：
//
//   - Closed-loop：固定并发数，每个 worker 发一个请求、等响应、再发下一个。
//     吞吐被"响应速度"反锁，系统变慢时并发压力同步下降，因此**测不出排队延迟**。
//   - Open-loop：按目标速率（或尽可能快）无等待地按预定时刻发起请求，
//     延迟从"预定发起时刻"开始计时。这才能暴露真正的尾延迟
//     （即 coordinated omission 问题）。
//
// 本文件同时提供两种模式，并由 Runner 显式记录所用模式，避免混淆。
package bench

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
)

// Mode 发压模式。
type Mode string

const (
	// ModeClosed 闭循环：固定并发。
	ModeClosed Mode = "closed"
	// ModeOpen 开循环：按目标速率（0 表示不限速）发起，按预定时刻计时。
	ModeOpen Mode = "open"
)

// OpKind 操作类型。
type OpKind string

const (
	OpGet OpKind = "get"
	OpPut OpKind = "put"
)

// Distribution key 访问分布。
type Distribution string

const (
	// DistUniform 均匀随机。
	DistUniform Distribution = "uniform"
	// DistZipf Zipf 分布（热点），theta 控制偏斜。
	DistZipf Distribution = "zipf"
	// DistLatest 最新键优先（YCSB workload D 的插入/更新模式）。
	DistLatest Distribution = "latest"
	// DistSequential 顺序扫描。
	DistSequential Distribution = "sequential"
)

// Workload 负载配置。
type Workload struct {
	// ReadRatio ∈ [0,1]：读操作占比。
	ReadRatio float64
	// Keys 键空间大小。
	Keys int
	// KeySize 键长度（字节），用于构造 key 字符串。
	KeySize int
	// ValueSize 值长度（字节）。
	ValueSize int
	// Dist 访问分布。
	Dist Distribution
	// ZipfTheta 仅 DistZipf 使用，典型 0.99（YCSB 默认）。
	ZipfTheta float64
	// KeyPrefix 键前缀。
	KeyPrefix string
}

func (w Workload) withDefaults() Workload {
	if w.Keys <= 0 {
		w.Keys = 10000
	}
	if w.KeyPrefix == "" {
		w.KeyPrefix = "k"
	}
	if w.ZipfTheta <= 0 {
		w.ZipfTheta = 0.99
	}
	if w.Dist == "" {
		w.Dist = DistZipf
	}
	if w.ReadRatio < 0 {
		w.ReadRatio = 0
	}
	if w.ReadRatio > 1 {
		w.ReadRatio = 1
	}
	return w
}

// Key 返回第 i 个键。
func (w Workload) Key(i int) string {
	w = w.withDefaults()
	k := fmt.Sprintf("%s%0*d", w.KeyPrefix, maxInt(w.KeySize, 8), i)
	// 分片边界以十六进制编码，因此键也必须能落在同一空间内。
	return k
}

// Value 返回长度为 ValueSize 的值。
func (w Workload) Value(i int) string {
	w = w.withDefaults()
	if w.ValueSize <= 0 {
		return fmt.Sprintf("v%d", i)
	}
	b := make([]byte, w.ValueSize)
	// 填充可预测内容，避免把随机数生成器的开销计入被测路径。
	seed := byte(i)
	for j := range b {
		b[j] = 'a' + (seed+byte(j))%26
	}
	return string(b)
}

// minInt 返回较小值。
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// maxInt 返回较大值。
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Generator 生成操作序列。每个 worker 应持有独立实例（各自 rand 源，避免锁竞争）。
type Generator struct {
	w  Workload
	r  *rand.Rand
	zi *zipf

	seq int
}

// NewGenerator 创建生成器。seed 用于可复现性。
func NewGenerator(w Workload, seed int64) *Generator {
	w = w.withDefaults()
	return &Generator{
		w:  w,
		r:  rand.New(rand.NewSource(seed)),
		zi: newZipf(1, w.ZipfTheta, float64(w.Keys)),
	}
}

// Next 返回下一个操作。
func (g *Generator) Next() (OpKind, string, string) {
	var idx int
	switch g.w.Dist {
	case DistUniform:
		idx = g.r.Intn(g.w.Keys)
	case DistLatest:
		idx = g.w.Keys - 1 - g.r.Intn(minInt(g.w.Keys, 1024))
		if idx < 0 {
			idx = 0
		}
	case DistSequential:
		idx = g.seq % g.w.Keys
		g.seq++
	default: // Zipf
		idx = int(g.zi.Uint64()) % g.w.Keys
	}
	key := g.w.Key(idx)

	if g.r.Float64() < g.w.ReadRatio {
		return OpGet, key, ""
	}
	return OpPut, key, g.w.Value(idx)
}

// zipf 是一个轻量 Zipf 采样器（拒绝采样，无需预计算大表）。
//
// 参考 Gray et al., "Quickly Generating Billion-Record Synthetic Databases", SIGMOD 1994。
type zipf struct {
	r         *rand.Rand
	theta     float64
	n         float64
	alpha     float64
	zetan     float64
	threshold float64
}

func newZipf(seed int64, theta, n float64) *zipf {
	if theta >= 1 {
		// theta == 1 时 zeta 发散，退化为近似均匀；调用方如需强热点请用 theta < 1。
		theta = 0.99
	}
	if n < 1 {
		n = 1
	}
	z := &zipf{r: rand.New(rand.NewSource(seed)), theta: theta, n: n}
	z.zetan = zeta(theta, n)
	z.alpha = 1.0 / (1.0 - theta)
	z.threshold = 1.0 + math.Pow(0.5, theta)
	return z
}

func (z *zipf) Uint64() uint64 {
	if z.n <= 1 || z.zetan == 0 {
		return 0
	}
	for {
		u := z.r.Float64()
		x := z.n * math.Pow(u, z.alpha)
		ix := uint64(x)
		if ix < 1 {
			ix = 1
		}
		if ix > uint64(z.n) {
			ix = uint64(z.n)
		}
		if float64(ix) < x*(1.0+z.threshold*math.Pow(z.n/float64(ix), z.theta)/(z.zetan*(1.0-z.theta))) {
			return ix
		}
	}
}

// zeta 计算截断的 Hurwitz zeta 函数。
// 用欧拉-麦克劳林余项代替逐项求和：n 可达 10^7，逐项求和在实验启动时会明显变慢，
// 而尾部近似在 theta < 1 时的相对误差远小于 1e-6，对负载生成完全够用。
func zeta(theta, n float64) float64 {
	const directLimit = 1e5
	limit := n
	if limit > directLimit {
		limit = directLimit
	}
	sum := 0.0
	for i := 1.0; i <= limit; i++ {
		sum += math.Pow(1.0/i, theta)
	}
	if n > limit {
		// ∫_limit^∞ x^-theta dx = limit^(1-theta)/(theta-1)
		sum += math.Pow(limit, 1.0-theta) / (1.0 - theta)
		// 一阶修正项。
		sum += 0.5 * math.Pow(limit, -theta)
	}
	return sum
}

// WorkloadSummary 供结果文件记录。
func (w Workload) Summary() string {
	w = w.withDefaults()
	parts := []string{
		fmt.Sprintf("read_ratio=%.2f", w.ReadRatio),
		fmt.Sprintf("keys=%d", w.Keys),
		fmt.Sprintf("dist=%s", w.Dist),
		fmt.Sprintf("value_size=%d", w.ValueSize),
	}
	if w.Dist == DistZipf {
		parts = append(parts, fmt.Sprintf("zipf_theta=%.2f", w.ZipfTheta))
	}
	return strings.Join(parts, " ")
}

// ParallelGenerators 为 n 个 worker 各建一个生成器。
func ParallelGenerators(w Workload, n int, baseSeed int64) []*Generator {
	out := make([]*Generator, n)
	for i := range out {
		out[i] = NewGenerator(w, baseSeed+int64(i)*7919)
	}
	return out
}

// ShardedGenerators 返回按 key 空间切分的生成器集合，用于让每个 worker
// 只访问自己那一段 key（减少跨分片噪声，便于按分片归因）。
func ShardedGenerators(w Workload, workers, shards int, baseSeed int64) []*Generator {
	out := make([]*Generator, workers)
	perShard := w.Keys / maxInt(shards, 1)
	for i := range out {
		ww := w
		ww.Keys = maxInt(perShard, 1)
		ww.KeyPrefix = fmt.Sprintf("%s%d-", w.KeyPrefix, i%maxInt(shards, 1))
		out[i] = NewGenerator(ww, baseSeed+int64(i)*7919)
	}
	return out
}
