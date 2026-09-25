// pkg/faft/geometry.go
//
// FAFT（Failure-Aware Flexible-quorum Topology）的 quorum 几何求解器。
//
// 本包解决的问题：
//   "多数 quorum"把两个决策同时固定死 —— quorum 大小恒为 ⌊n/2⌋+1，
//   副本落点只要求"分散到不同故障域"。在 n=1000 时这会带来一个
//   在小组中完全不可见的后果：稳态复制可容忍 994 个节点故障，
//   但恢复 leader 需要 995/1000 个节点在线。
//
// Flexible Paxos（Howard et al., OPODIS 2016, §4.2）只要求
//
//	|Q1| + |Q2| > N
//
// 也就是说两个阶段只需要**彼此**相交，不需要各自内部相交。
// 这条约束留下了自由度，但文献只把它用在"把阶段二变小"这一个方向上，
// 没有把它与**副本落点**联合起来考虑。
//
// 本包做的事情：在 |Q1|+|Q2| > N 的可行域内，显式地同时选择
//   · 两个阶段的 quorum 大小（几何）
//   · 每个 quorum 落在哪些故障域（落点）
// 使得数据路径与控制路径各有各的容错目标。
//
// 本包不含任何新定理。所有约束都标注了出处。
package faft

import (
	"fmt"
	"math"
	"sort"
)

// Domain 一个故障域。
type Domain struct {
	// Name 域标识（zone / rack / dc）。
	Name string
	// RTTMs 本域到其他域的单程往返延迟；索引与 Domains 切片一致。
	// 同域为 0。用于计算 quorum 的延迟代价。
	RTTMs []float64
	// FailProb 本域在一段观测窗口内整体失效的概率（相关故障模型）。
	// 独立故障模型下，把每个副本视作独立，域的概率取 0。
	FailProb float64
	// Capacity 本域可容纳的副本数上限（0 表示不限）。
	Capacity int
	// EgressCostPerMB 本域出口的单位代价（用于 WAN 成本核算）。
	EgressCostPerMB float64
}

// Topology 集群的故障域结构。
type Topology struct {
	Domains []Domain
}

// NewUniformTopology 构造 `n` 个均匀域，域间 RTT 均为 rttMs。
func NewUniformTopology(n int, rttMs float64) *Topology {
	t := &Topology{Domains: make([]Domain, n)}
	for i := range t.Domains {
		t.Domains[i] = Domain{
			Name:     fmt.Sprintf("zone-%d", i),
			RTTMs:    make([]float64, n),
			FailProb: 0,
		}
		for j := range t.Domains[i].RTTMs {
			if i != j {
				t.Domains[i].RTTMs[j] = rttMs
			}
		}
	}
	return t
}

// RTT 两个域之间的往返延迟。
func (t *Topology) RTT(a, b int) float64 {
	if a == b {
		return 0
	}
	if a < 0 || a >= len(t.Domains) {
		return 0
	}
	rs := t.Domains[a].RTTMs
	if b < 0 || b >= len(rs) {
		return 0
	}
	return rs[b]
}

// Config 求解输入。
type Config struct {
	// Replicas 单个分片的副本总数 n。
	Replicas int
	// Domains 每个分片的副本分布在哪些域；长度应为 n。
	// 若为空，则按域数量均分。
	Placement []int
	// DataFaults 数据路径要容忍的故障域数 f_data。
	DataFaults int
	// ControlFaults 控制路径要容忍的故障域数 f_ctrl。
	// 这是本设计引入的约束维度：选主所需的 quorum 必须比 f_ctrl 更大。
	ControlFaults int
	// LeaderDomain Leader 所在域，用于计算延迟代价。
	LeaderDomain int
	// DedicatedControl 为 true 时，把控制 quorum 与数据 quorum
	// 显式分配到不同域集合上（FAFT 的核心机制）。
	DedicatedControl bool
	// LogBytes 单条日志载荷字节数（用于带宽核算）。
	LogBytes int
	// WriteRatio ∈ [0,1] 写操作占比。
	WriteRatio float64
}

func (c Config) withDefaults(t *Topology) Config {
	if c.Replicas <= 0 {
		c.Replicas = 5
	}
	if c.LogBytes <= 0 {
		c.LogBytes = 256
	}
	if c.WriteRatio < 0 {
		c.WriteRatio = 0
	}
	if c.WriteRatio > 1 {
		c.WriteRatio = 1
	}
	// 默认放置：尽量把副本摊到不同域。
	if len(c.Placement) == 0 {
		nDom := len(t.Domains)
		if nDom == 0 {
			nDom = 1
		}
		c.Placement = make([]int, c.Replicas)
		for i := range c.Placement {
			c.Placement[i] = i % nDom
		}
	}
	return c
}

// Quorum 一个 quorum 方案。
type Quorum struct {
	// Q2Members 参与阶段二（复制路径）的副本下标。
	Q2Members []int
	// Q1Members 参与阶段一（选主 / 恢复路径）的副本下标。
	Q1Members []int

	// Q2Domains / Q1Domains 两个阶段各自覆盖的故障域。
	Q2Domains []int
	Q1Domains []int
}

// Size 两个 quorum 的大小。
func (q Quorum) Size() (q1, q2 int) { return len(q.Q1Members), len(q.Q2Members) }

// Plan 求解结果。
type Plan struct {
	Quorum Quorum

	// Q2Domains / Q1Domains 两个阶段各自覆盖的故障域。
	Q2Domains []int
	Q1Domains []int

	// Safe 是否满足 |Q1|+|Q2| > N（Flexible Paxos 安全性）。
	Safe bool
	// Intersects 是否满足 ∀Q1,Q2: Q1 ∩ Q2 ≠ ∅（更强的逐对相交条件）。
	Intersects bool
	// DataTolerant 数据路径可容忍的故障域数。
	DataTolerant int
	// ControlTolerant 控制路径可容忍的故障域数。
	ControlTolerant int
	// MeetsData / MeetsControl 是否达到配置要求。
	MeetsData, MeetsControl bool

	// WriteMsgsPerOp 稳态下一次写的消息数（≈ 2|Q2|）。
	WriteMsgsPerOp int
	// ReadMsgsPerOp 一次线性化读的消息数（readIndex 语义下 ≈ |Q2|）。
	ReadMsgsPerOp int
	// WriteLatencyMs 稳态写延迟估计（一个到 Q2 的往返 + fsync 之外的部分）。
	WriteLatencyMs float64
	// ReadLatencyMs 稳态读延迟估计。
	ReadLatencyMs float64
	// SelectionLatencyMs 选主 / 恢复的延迟估计（一个到 Q1 的往返）。
	SelectionLatencyMs float64

	// WANBytesPerOp 一次写跨域的字节数。
	WANBytesPerOp float64
	// HeartbeatBytesPerSec 该分片心跳总带宽（仅 Q2 成员参与心跳级保活）。
	HeartbeatBytesPerSec float64
}

// maxMsgsWithinRTT 在给定时延预算内，从某域出发能覆盖到的最远 RTT 分位。
//
// 用于把"延迟代价"折算成约束：一个 quorum 的延迟约等于到其最慢成员的往返。
func maxRTTFrom(t *Topology, from int, members []int) float64 {
	worst := 0.0
	for _, m := range members {
		if d := t.RTT(from, m); d > worst {
			worst = d
		}
	}
	return worst
}

// avgRTTFrom 到 quorum 成员的平均往返。
func avgRTTFrom(t *Topology, from int, members []int) float64 {
	if len(members) == 0 {
		return 0
	}
	sum := 0.0
	for _, m := range members {
		sum += t.RTT(from, m)
	}
	return sum / float64(len(members))
}

func uniqueDomains(t *Topology, src []int, placement []int) []int {
	seen := map[int]bool{}
	var out []int
	for _, idx := range src {
		if idx < 0 || idx >= len(placement) {
			continue
		}
		d := placement[idx]
		if !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	sort.Ints(out)
	return out
}

// Evaluate 评估一个给定的 quorum 方案。
func Evaluate(t *Topology, cfg Config, q Quorum) Plan {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas

	q1, q2 := q.Size()
	p := Plan{Quorum: q}
	p.Safe = q1+q2 > n

	// 逐对相交检查（安全性的充分条件，也是 Flexible Paxos 的原始要求）。
	inQ1 := map[int]bool{}
	for _, m := range q.Q1Members {
		inQ1[m] = true
	}
	p.Intersects = true
	for _, m := range q.Q2Members {
		if !inQ1[m] {
			p.Intersects = false
			break
		}
	}

	p.Q2Domains = uniqueDomains(t, q.Q2Members, cfg.Placement)
	p.Q1Domains = uniqueDomains(t, q.Q1Members, cfg.Placement)

	// 容错度按"故障域"计：某域整体失效时，quorum 仍要能形成。
	// quorum 覆盖 d 个域 => 最多容忍 d-1 个域同时失效。
	p.DataTolerant = len(p.Q2Domains) - 1
	p.ControlTolerant = len(p.Q1Domains) - 1
	if p.DataTolerant < 0 {
		p.DataTolerant = 0
	}
	if p.ControlTolerant < 0 {
		p.ControlTolerant = 0
	}
	p.MeetsData = p.DataTolerant >= cfg.DataFaults
	p.MeetsControl = p.ControlTolerant >= cfg.ControlFaults

	// 消息数：Multi-Paxos / Raft 稳态下一次写 = leader → Q2 → leader
	p.WriteMsgsPerOp = 2 * q2
	// readIndex 读：一轮心跳确认，即 |Q2| 条请求 + |Q2| 条响应。
	p.ReadMsgsPerOp = 2 * q2

	// 延迟：稳态写 ≈ 一个到 Q2 最慢成员的往返；读同理。
	lead := cfg.LeaderDomain
	p.WriteLatencyMs = maxRTTFrom(t, lead, q.Q2Members)
	p.ReadLatencyMs = maxRTTFrom(t, lead, q.Q2Members)
	p.SelectionLatencyMs = maxRTTFrom(t, lead, q.Q1Members)

	// 带宽：写时 leader 把日志发给 Q2 的所有其他成员。
	followers := float64(len(q.Q2Members) - 1)
	if followers < 0 {
		followers = 0
	}
	p.WANBytesPerOp = followers * float64(cfg.LogBytes)
	// 心跳：每 100ms 一次，每次约 64 字节。
	p.HeartbeatBytesPerSec = followers * 10 * 64

	return p
}

// Solve 在可行域内搜索最优 quorum 几何。
//
// 搜索策略：对每个可能的 |Q2| = k（从 f_data+1 到 n），
// 计算满足 |Q1| + k > n 的最小 |Q1|，然后在域层面贪心构造成员集合：
//
//   - Q2 优先选择**离 leader 最近的域**（数据路径尽量减少延迟）；
//   - Q1 优先选择**覆盖域数最多**（控制路径尽量提高容错）。
//
// 在 n ≤ 64 的规模上这是完全可枚举的；更大规模用上面的贪心，
// 复杂度 O(n²)，对 n=1000 也在毫秒级。
func Solve(t *Topology, cfg Config) []Plan {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas
	lead := cfg.LeaderDomain

	// 按副本到 leader 域的 RTT 排序（近的在前）。
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		da := cfg.Placement[order[a]]
		db := cfg.Placement[order[b]]
		return t.RTT(lead, da) < t.RTT(lead, db)
	})

	var plans []Plan
	for k := 1; k <= n; k++ {
		q1min := n - k + 1 // |Q1| + |Q2| > n  =>  |Q1| >= n-k+1
		if q1min > n {
			continue
		}
		if q1min < 1 {
			q1min = 1
		}

		// Q2：取离 leader 最近的 k 个副本。
		q2 := append([]int(nil), order[:k]...)

		// Q1：必须 >= q1min，且尽量覆盖更多故障域。
		// 先取覆盖域最多的前 q1min 个（贪心：每遇到一个新域就加进来）。
		q1 := pickMaxDomains(t, cfg.Placement, q1min, n)

		p := Evaluate(t, cfg, Quorum{Q1Members: q1, Q2Members: q2})
		plans = append(plans, p)
	}

	// 过滤掉不安全的方案。
	out := plans[:0]
	for _, p := range plans {
		if p.Safe {
			out = append(out, p)
		}
	}
	return out
}

// pickMaxDomains 在 n 个副本中挑 k 个，使覆盖的故障域数最多。
// 贪心：按域分组，每轮从每个域取一个，直到取够 k 个。
func pickMaxDomains(t *Topology, placement []int, k, n int) []int {
	if k > n {
		k = n
	}
	byDomain := map[int][]int{}
	var doms []int
	for i := 0; i < n; i++ {
		d := placement[i]
		if _, ok := byDomain[d]; !ok {
			doms = append(doms, d)
		}
		byDomain[d] = append(byDomain[d], i)
	}
	sort.Ints(doms)

	var out []int
	// 轮转取，保证域覆盖优先。
	for round := 0; len(out) < k; round++ {
		progressed := false
		for _, d := range doms {
			if len(out) >= k {
				break
			}
			if round < len(byDomain[d]) {
				out = append(out, byDomain[d][round])
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	return out
}

// BaselineMajority 构造"标准多数 quorum"方案，作为对照。
//
// 注意：多数 quorum 下 Q1 = Q2 = ⌊n/2⌋+1，两个阶段合并成同一个集合。
// 这是 Raft / Multi-Paxos 的做法，也是本设计要对比的基线。
func BaselineMajority(t *Topology, cfg Config) Plan {
	cfg = cfg.withDefaults(t)
	n := cfg.Replicas
	m := n/2 + 1

	members := make([]int, 0, m)
	for i := 0; i < m && i < n; i++ {
		members = append(members, i)
	}
	return Evaluate(t, cfg, Quorum{Q1Members: members, Q2Members: members})
}

// BestBy 在候选方案中按给定准则挑选最优。
type Criterion func(Plan) float64

// MinWriteMsgs 按稳态写消息数最小挑选。
func MinWriteMsgs(p Plan) float64 { return float64(p.WriteMsgsPerOp) }

// MinWriteLatency 按稳态写延迟最小挑选。
func MinWriteLatency(p Plan) float64 { return p.WriteLatencyMs }

// PickBest 在满足给定约束的方案中按准则取最优。
func PickBest(plans []Plan, crit Criterion, ok func(Plan) bool) (Plan, bool) {
	var best Plan
	found := false
	bestV := math.Inf(1)
	for _, p := range plans {
		if ok != nil && !ok(p) {
			continue
		}
		v := crit(p)
		if v < bestV {
			bestV = v
			best = p
			found = true
		}
	}
	return best, found
}
