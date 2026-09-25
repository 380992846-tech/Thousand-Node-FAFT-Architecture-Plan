// pkg/cluster/cluster.go
//
// 单进程多分片集群：在一个 Go 进程里起 N 个分片 × R 个副本，用于可复现实验。
//
// 为什么需要它：
//   - 原始项目的 scripts/start-cluster.sh 为 200×5 = 1000 个节点各起一个 OS 进程，
//     还要 mkdir 1000 个目录、绑定 1000 个端口。在一台机器上即使用 5 节点分片，
//     进程/端口/FD 开销也会把测量结果淹没，根本测不出协议本身的性质。
//   - BUG-27：原脚本端口算法 port = 8000 + shard*10 + node，在 200 分片时
//     最大端口 = 8000+199*10+4 = 9994，而 shard=200 会算到 10000+，直接越界；
//     且 shard 每增加 1 就吃掉 10 个端口，实际只用了 5 个。这里改为线性分配。
//   - BUG-9：原 metrics 用 promauto 全局注册，同进程创建第二个节点即 panic。
//     这里每个节点独立 Registry，才能在同一进程内比较不同配置。
//
// 本包不依赖 etcd：拓扑由进程内的 LocalTopology 提供，因此实验可离线复现。
package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/distributed-kv/kvstore/pkg/metadata"
	"github.com/distributed-kv/kvstore/pkg/nodeapi"
	"github.com/distributed-kv/kvstore/pkg/raft"
)

// Member 一个分片副本。
type Member struct {
	NodeID   string
	ShardID  int
	RaftAddr string
	HTTPAddr string
	Store    *raft.KVStore
	Server   *nodeapi.Server
	ln       net.Listener
	srv      *http.Server
}

// Cluster 单进程多分片集群。
type Cluster struct {
	mu      sync.RWMutex
	members map[int][]*Member // shardID -> members
	topo    *metadata.LocalTopology
	dir     string

	cfg Config
}

// Config 集群配置。
type Config struct {
	// Shards 分片数。
	Shards int
	// Replicas 每分片副本数（投票成员）。
	Replicas int
	// Learners 每分片额外的非投票副本（用于验证 witness/learner 方向）。
	Learners int
	// BaseRaftPort 第一个 Raft 端口；后续按 +1 线性分配。
	BaseRaftPort int
	// BaseHTTPPort 第一个数据面端口。
	BaseHTTPPort int
	// Dir 数据目录根；每个副本一个子目录。
	Dir string
	// Host 监听主机，默认 127.0.0.1。
	Host string
	// HeartbeatTimeout / ElectionTimeout 传给 Raft。
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	// NoSync 为 true 时关闭 fsync（仅用于对照实验，明确标注为非持久化配置）。
	NoSync bool
	// CleanDir 为 true 时启动前清空 Dir。
	CleanDir bool
	// LogOutput 传给 Raft 的输出；默认丢弃，避免千节点刷屏。
	LogOutput io.Writer
	// KeySpaceStart 分片 0 的起始 key。
	KeySpaceStart string
}

func (c Config) withDefaults() Config {
	if c.Shards <= 0 {
		c.Shards = 1
	}
	if c.Replicas <= 0 {
		c.Replicas = 3
	}
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}
	if c.BaseRaftPort == 0 {
		c.BaseRaftPort = 18000
	}
	if c.BaseHTTPPort == 0 {
		c.BaseHTTPPort = 28000
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 100 * time.Millisecond
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 500 * time.Millisecond
	}
	if c.LogOutput == nil {
		c.LogOutput = io.Discard
	}
	return c
}

// New 创建并启动集群。
//
// 每个分片独立选举：首个副本 Bootstrap，其余副本通过 Leader 的 /join 入组。
func New(cfg Config) (*Cluster, error) {
	cfg = cfg.withDefaults()
	if cfg.Dir == "" {
		return nil, errors.New("cluster: Dir required")
	}
	if cfg.CleanDir {
		if err := os.RemoveAll(cfg.Dir); err != nil {
			return nil, fmt.Errorf("cluster: clean dir: %w", err)
		}
	}

	c := &Cluster{
		members: make(map[int][]*Member, cfg.Shards),
		topo:    metadata.NewLocalTopology(cfg.KeySpaceStart),
		dir:     cfg.Dir,
		cfg:     cfg,
	}

	for shard := 0; shard < cfg.Shards; shard++ {
		if err := c.startShard(shard); err != nil {
			c.Close()
			return nil, fmt.Errorf("cluster: start shard %d: %w", shard, err)
		}
	}

	c.publishTopology()
	return c, nil
}

func (c *Cluster) startShard(shard int) error {
	total := c.cfg.Replicas + c.cfg.Learners
	members := make([]*Member, 0, total)

	for i := 0; i < total; i++ {
		idx := shard*total + i
		m := &Member{
			NodeID:   fmt.Sprintf("n%d-%d", shard, i),
			ShardID:  shard,
			RaftAddr: fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.BaseRaftPort+idx),
			HTTPAddr: fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.BaseHTTPPort+idx),
		}
		if err := c.startMember(m); err != nil {
			return err
		}
		members = append(members, m)
	}

	// 首副本引导。
	first := members[0]
	res, err := first.Store.Bootstrap()
	if err != nil {
		return fmt.Errorf("bootstrap %s: %w", first.NodeID, err)
	}
	if _, err := first.Store.WaitForLeader(15 * time.Second); err != nil {
		return fmt.Errorf("shard %d: %w", shard, err)
	}
	_ = res

	// 其余副本入组。
	for i := 1; i < len(members); i++ {
		m := members[i]
		nonvoter := i >= c.cfg.Replicas
		if err := c.joinWithRetry(context.Background(), first.HTTPAddr, m, nonvoter); err != nil {
			return err
		}
	}

	c.mu.Lock()
	c.members[shard] = members
	c.mu.Unlock()
	return nil
}

func (c *Cluster) startMember(m *Member) error {
	dir := fmt.Sprintf("%s/%s", c.dir, m.NodeID)
	store, err := raft.NewKVStore(raft.Config{
		NodeID:           m.NodeID,
		ShardID:          m.ShardID,
		BindAddr:         m.RaftAddr,
		HTTPAddr:         m.HTTPAddr,
		RaftDir:          dir,
		HeartbeatTimeout: c.cfg.HeartbeatTimeout,
		ElectionTimeout:  c.cfg.ElectionTimeout,
		LogOutput:        c.cfg.LogOutput,
	})
	if err != nil {
		return fmt.Errorf("start %s: %w", m.NodeID, err)
	}
	m.Store = store

	srv := nodeapi.New(store, nil, store.Registry(), nodeapi.Config{
		HTTPAddr:      m.HTTPAddr,
		AdvertiseAddr: m.RaftAddr,
		Logger:        discardLogger(),
	})

	ln, err := net.Listen("tcp", m.HTTPAddr)
	if err != nil {
		_ = store.Shutdown()
		return fmt.Errorf("listen %s: %w", m.HTTPAddr, err)
	}
	m.ln = ln
	m.srv = &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	m.Server = srv
	go func() { _ = m.srv.Serve(ln) }()
	return nil
}

func (c *Cluster) joinWithRetry(ctx context.Context, leaderHTTP string, m *Member, nonvoter bool) error {
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = nodeapi.JoinCluster(cctx, leaderHTTP, m.NodeID, m.RaftAddr, m.ShardID, nonvoter)
		cancel()
		if lastErr == nil {
			return nil
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("join %s via %s: %w", m.NodeID, leaderHTTP, lastErr)
}

// publishTopology 生成进程内拓扑：把 key 空间等分给各分片。
func (c *Cluster) publishTopology() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	shards := make([]int, 0, len(c.members))
	for id := range c.members {
		shards = append(shards, id)
	}
	sort.Ints(shards)

	for _, id := range shards {
		ms := c.members[id]
		nodes := make([]string, 0, len(ms))
		leader := ""
		for _, m := range ms {
			if m.Store.IsLeader() {
				leader = m.HTTPAddr
			}
			nodes = append(nodes, m.HTTPAddr)
		}
		start, end := c.topo.RangeForShard(id, len(shards))
		c.topo.SetShard(&metadata.ShardInfo{
			ID: id, Nodes: nodes, Leader: leader,
			StartKey: start, EndKey: end,
			Status: metadata.ShardActive, Term: ms[0].Store.Term(),
		})
	}
}

// Topology 返回进程内拓扑，可直接交给 router/client。
func (c *Cluster) Topology() *metadata.LocalTopology { return c.topo }

// Members 返回某分片的副本。
func (c *Cluster) Members(shard int) []*Member {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]*Member(nil), c.members[shard]...)
}

// AllMembers 返回所有副本。
func (c *Cluster) AllMembers() []*Member {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*Member, 0, len(c.members)*c.cfg.Replicas)
	for _, ms := range c.members {
		out = append(out, ms...)
	}
	return out
}

// Leaders 返回每个分片当前 Leader 的数据面地址（可能为空）。
func (c *Cluster) Leaders() map[int]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[int]string, len(c.members))
	for id, ms := range c.members {
		for _, m := range ms {
			if m.Store.IsLeader() {
				out[id] = m.HTTPAddr
				break
			}
		}
	}
	return out
}

// RefreshTopology 重新读取各分片 Leader 并更新拓扑。
func (c *Cluster) RefreshTopology() { c.publishTopology() }

// LeaderCount 统计当前 Leader 数（含唯一性检查的输入）。
func (c *Cluster) LeaderCount() int {
	return len(c.Leaders())
}

// DuplicateLeaders 返回同时认为自己是 Leader 的 (shard, term) 组合。
// 这是安全性检查：同一分片、同一任期内不应出现两个 Leader。
func (c *Cluster) DuplicateLeaders() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []string
	for id, ms := range c.members {
		byTerm := map[uint64]int{}
		for _, m := range ms {
			if m.Store.IsLeader() {
				byTerm[m.Store.Term()]++
			}
		}
		for term, n := range byTerm {
			if n > 1 {
				out = append(out, fmt.Sprintf("shard %d term %d: %d leaders", id, term, n))
			}
		}
	}
	return out
}

// WaitForLeaders 等待所有分片选出 Leader。
func (c *Cluster) WaitForLeaders(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.LeaderCount() == len(c.members) {
			c.publishTopology()
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("cluster: only %d/%d shards have a leader after %s",
		c.LeaderCount(), len(c.members), timeout)
}

// StopMember 模拟节点崩溃（硬停，等价于 kill -9）。
func (c *Cluster) StopMember(nodeID string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ms := range c.members {
		for _, m := range ms {
			if m.NodeID == nodeID {
				if m.srv != nil {
					_ = m.srv.Close()
				}
				return m.Store.Shutdown()
			}
		}
	}
	return fmt.Errorf("cluster: node %s not found", nodeID)
}

// Close 关闭整个集群。
func (c *Cluster) Close() {
	for _, m := range c.AllMembers() {
		if m.srv != nil {
			_ = m.srv.Close()
		}
		if m.Store != nil {
			_ = m.Store.Shutdown()
		}
	}
	c.topo.Close()
}

// String 集群摘要。
func (c *Cluster) String() string {
	return fmt.Sprintf("cluster{shards=%d replicas=%d learners=%d dir=%s}",
		c.cfg.Shards, c.cfg.Replicas, c.cfg.Learners, c.dir)
}

// HTTPAddrs 返回所有数据面地址。
func (c *Cluster) HTTPAddrs() []string {
	ms := c.AllMembers()
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.HTTPAddr)
	}
	return out
}

// discardLogger 返回一个丢弃全部输出的 slog.Logger。
// Raft 自身的日志通过 Config.LogOutput 单独控制（默认 io.Discard）。
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// ParseShardSpec 解析 "200x5" 形式的分片规格。
func ParseShardSpec(s string) (shards, replicas int, err error) {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(s)), "x")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("cluster: bad spec %q, want e.g. 16x5", s)
	}
	if _, err = fmt.Sscanf(parts[0], "%d", &shards); err != nil {
		return 0, 0, err
	}
	if _, err = fmt.Sscanf(parts[1], "%d", &replicas); err != nil {
		return 0, 0, err
	}
	if shards <= 0 || replicas <= 0 {
		return 0, 0, fmt.Errorf("cluster: shards and replicas must be positive")
	}
	return shards, replicas, nil
}
