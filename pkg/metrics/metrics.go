// pkg/metrics/metrics.go
//
// 指标采集器。
//
// 相对原始版本的修复（见 docs/BUGS.md）：
//
//	BUG-8  shardID 在 NewMetricsCollector 后由 SetShardID 赋值，但在千节点场景下从未被调用，
//	       导致所有分片指标都落在 "0" 标签上；且该字段是无锁读写的数据竞争。
//	       现在 shardID 在构造时传入并只读。
//	BUG-9  使用 promauto 全局注册：同进程内创建第二个节点（"单进程多分片"实验所必需）
//	       会因重复注册而 panic。现在每个 Collector 持有独立的 prometheus.Registry。
package metrics

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Collector 一个节点/分片的指标采集器。
type Collector struct {
	nodeID  string
	shardID int
	// shardLabel 预计算，避免每次观测都做 strconv。
	shardLabel string

	ops    atomic.Uint64
	errors atomic.Uint64

	registry *prometheus.Registry

	operationsTotal   *prometheus.CounterVec
	operationDuration *prometheus.HistogramVec
	raftState         *prometheus.GaugeVec
	applyLatency      prometheus.Histogram
}

// MetricsCollector 保留旧名称作为别名，避免破坏外部调用。
type MetricsCollector = Collector

// NewMetricsCollector 创建采集器。传入独立的 Registry 可避免全局注册冲突（BUG-9）。
// registry 为 nil 时创建私有 Registry。
func NewMetricsCollector(nodeID string, shardID int, registry ...*prometheus.Registry) *Collector {
	reg := prometheus.NewRegistry()
	if len(registry) > 0 && registry[0] != nil {
		reg = registry[0]
	}

	c := &Collector{
		nodeID:     nodeID,
		shardID:    shardID,
		shardLabel: strconv.Itoa(shardID),
		registry:   reg,
	}

	c.operationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kv_operations_total",
			Help: "Total number of KV operations.",
		},
		[]string{"node", "shard", "operation", "status"},
	)
	c.operationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kv_operation_duration_seconds",
			Help:    "Latency of KV operations in seconds.",
			Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"node", "shard", "operation"},
	)
	c.raftState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kv_raft_state",
			Help: "Raft state (0=follower, 1=candidate, 2=leader, 3=shutdown).",
		},
		[]string{"node", "shard"},
	)
	c.applyLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kv_apply_duration_seconds",
			Help:    "Time spent inside FSM.Apply.",
			Buckets: prometheus.DefBuckets,
		},
	)

	reg.MustRegister(c.operationsTotal, c.operationDuration, c.raftState, c.applyLatency)
	return c
}

// Registry 返回该采集器的注册表，供 /metrics 使用。
func (c *Collector) Registry() *prometheus.Registry { return c.registry }

// NodeID 节点标识。
func (c *Collector) NodeID() string { return c.nodeID }

// ShardID 分片标识。
func (c *Collector) ShardID() int { return c.shardID }

// RecordOperation 记录一次成功操作。
func (c *Collector) RecordOperation(op string, count int64) {
	c.ops.Add(uint64(count))
	c.operationsTotal.WithLabelValues(c.nodeID, c.shardLabel, op, "ok").Add(float64(count))
}

// RecordError 记录一次失败操作。
func (c *Collector) RecordError(op string) {
	c.errors.Add(1)
	c.operationsTotal.WithLabelValues(c.nodeID, c.shardLabel, op, "error").Inc()
}

// RecordLatency 记录操作延迟。
func (c *Collector) RecordLatency(op string, d time.Duration) {
	c.operationDuration.WithLabelValues(c.nodeID, c.shardLabel, op).Observe(d.Seconds())
}

// RecordApplyLatency 记录状态机应用耗时。
func (c *Collector) RecordApplyLatency(d time.Duration) {
	c.applyLatency.Observe(d.Seconds())
}

// SetRaftState 记录 Raft 角色。
func (c *Collector) SetRaftState(state int) {
	c.raftState.WithLabelValues(c.nodeID, c.shardLabel).Set(float64(state))
}

// Ops 累计成功操作数。
func (c *Collector) Ops() uint64 { return c.ops.Load() }

// Errors 累计失败操作数。
func (c *Collector) Errors() uint64 { return c.errors.Load() }
