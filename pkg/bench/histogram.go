// pkg/bench/histogram.go
//
// 对数分桶延迟直方图。
//
// 为什么不用 HDR Histogram 库：本项目的实验需要把直方图整份序列化进 CSV/JSON 结果文件，
// 并且必须能在不引入外部依赖的前提下复现。一个 4096 桶的对数直方图在 1µs–100s 范围内
// 的相对误差 < 2%，对 p50/p99/p99.9 的呈现足够，且实现只有几十行、可审计。
package bench

import (
	"math"
	"sync/atomic"
)

// Histogram 并发安全的对数分桶直方图。零值可用。
type Histogram struct {
	buckets []atomic.Uint64
	count   atomic.Uint64
	sumNs   atomic.Uint64

	// minNs/maxNs 记录真实极值（不受分桶误差影响）。
	minNs atomic.Uint64
	maxNs atomic.Uint64

	// 配置：桶 i 覆盖 [min*ratio^i, min*ratio^(i+1))。
	minNsCfg float64
	ratio    float64
}

// 默认配置：1µs 起，覆盖到约 100s。
const (
	defaultMinNs = 1_000.0
	defaultRatio = 1.02
	maxBuckets   = 4096
	// 溢出桶下标。
	overflowBucket = maxBuckets - 1
)

// NewHistogram 创建直方图。
func NewHistogram() *Histogram {
	h := &Histogram{minNsCfg: defaultMinNs, ratio: defaultRatio}
	h.buckets = make([]atomic.Uint64, maxBuckets)
	h.minNs.Store(math.MaxUint64)
	return h
}

func (h *Histogram) bucketIndex(ns uint64) int {
	if h.buckets == nil {
		return overflowBucket
	}
	v := float64(ns)
	if v <= h.minNsCfg {
		return 0
	}
	i := int(math.Log(v/h.minNsCfg) / math.Log(h.ratio))
	if i < 0 {
		return 0
	}
	if i >= overflowBucket {
		return overflowBucket
	}
	return i
}

// Observe 记录一次观测（单位：纳秒）。
func (h *Histogram) Observe(ns uint64) {
	h.count.Add(1)
	h.sumNs.Add(ns)
	h.buckets[h.bucketIndex(ns)].Add(1)

	// CAS 更新极值。
	for {
		cur := h.minNs.Load()
		if ns >= cur || h.minNs.CompareAndSwap(cur, ns) {
			break
		}
	}
	for {
		cur := h.maxNs.Load()
		if ns <= cur || h.maxNs.CompareAndSwap(cur, ns) {
			break
		}
	}
}

// Count 观测次数。
func (h *Histogram) Count() uint64 { return h.count.Load() }

// Mean 平均值（纳秒）。
func (h *Histogram) Mean() float64 {
	n := h.count.Load()
	if n == 0 {
		return 0
	}
	return float64(h.sumNs.Load()) / float64(n)
}

// Min / Max 观测极值（纳秒）。
func (h *Histogram) Min() uint64 {
	v := h.minNs.Load()
	if v == math.MaxUint64 {
		return 0
	}
	return v
}

func (h *Histogram) Max() uint64 { return h.maxNs.Load() }

// Percentile 返回 p 分位（纳秒）。p ∈ (0,1]。
func (h *Histogram) Percentile(p float64) uint64 {
	n := h.count.Load()
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return h.Min()
	}
	if p >= 1 {
		return h.Max()
	}
	target := uint64(math.Ceil(p * float64(n)))

	var cum uint64
	for i := 0; i < len(h.buckets); i++ {
		c := h.buckets[i].Load()
		if c == 0 {
			continue
		}
		cum += c
		if cum >= target {
			if i == overflowBucket {
				return h.Max()
			}
			// 返回桶上界，作为保守估计。
			hi := h.minNsCfg * math.Pow(h.ratio, float64(i+1))
			return uint64(hi)
		}
	}
	return h.Max()
}

// Merge 把 other 的观测合并进 h（用于汇总各 worker 的直方图）。
// 合并会丢失对方的分桶细节精度，因此只在必要时使用。
func (h *Histogram) Merge(other *Histogram) {
	if other == nil || other.count.Load() == 0 {
		return
	}
	h.count.Add(other.count.Load())
	h.sumNs.Add(other.sumNs.Load())
	for i := range h.buckets {
		if c := other.buckets[i].Load(); c > 0 {
			h.buckets[i].Add(c)
		}
	}
	if mn := other.Min(); mn > 0 {
		for {
			cur := h.minNs.Load()
			if mn >= cur || h.minNs.CompareAndSwap(cur, mn) {
				break
			}
		}
	}
	if mx := other.Max(); mx > 0 {
		for {
			cur := h.maxNs.Load()
			if mx <= cur || h.maxNs.CompareAndSwap(cur, mx) {
				break
			}
		}
	}
}

// Snapshot 可序列化的直方图摘要。
type Snapshot struct {
	Count  uint64  `json:"count"`
	MeanMs float64 `json:"mean_ms"`
	MinMs  float64 `json:"min_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P90Ms  float64 `json:"p90_ms"`
	P99Ms  float64 `json:"p99_ms"`
	P999Ms float64 `json:"p99_9_ms"`
	MaxMs  float64 `json:"max_ms"`
}

// Snapshot 生成摘要。
func (h *Histogram) Snapshot() Snapshot {
	return Snapshot{
		Count:  h.Count(),
		MeanMs: nsToMs(h.Mean()),
		MinMs:  nsToMs(float64(h.Min())),
		P50Ms:  nsToMs(float64(h.Percentile(0.50))),
		P90Ms:  nsToMs(float64(h.Percentile(0.90))),
		P99Ms:  nsToMs(float64(h.Percentile(0.99))),
		P999Ms: nsToMs(float64(h.Percentile(0.999))),
		MaxMs:  nsToMs(float64(h.Max())),
	}
}

func nsToMs(ns float64) float64 { return ns / 1e6 }
