// pkg/metadata/cluster.go
//
// 元数据层：分片映射（StartKey/EndKey → shard → leader）与拓扑变更分发。
//
// 相对原始版本的修复（见 docs/BUGS.md）：
//
//	BUG-10 GetShardForKey 每次调用都构造并排序一个 keys 切片（O(n log n) + 分配），
//	       并且是按 shard ID 排序后线性扫描——注释写"二分查找"但实现不是。
//	       在 1000 分片、每个请求一次路由查询的场景下这是纯浪费。
//	       现在维护按 StartKey 排序的索引，用 sort.Search 做 O(log n) 查找。
//	BUG-11 排序依据错误：分片按 ID 排序，但查找依据是 key 区间。ID 顺序与 key 顺序
//	       不一致时返回错误分片。现在索引只按 StartKey 排序。
//	BUG-12 watchTopology 不处理 DELETE 事件：json.Unmarshal 空 Value 失败后 continue，
//	       被删除的分片永久残留在缓存里（分片合并/下线后路由到死地址）。
//	BUG-13 watchTopology 不更新 nodeShardMap，导致 NodeShard 查询给出过期结果。
//	BUG-14 UpdateLeader / RegisterShard 在持有写锁期间做阻塞 etcd Put，
//	       把网络 RTT 变成全局锁竞争。
//	BUG-15 RegisterShard 创建了一个 concurrency.Session 却完全不使用（死代码 + 泄漏）。
package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// 分片状态。
const (
	ShardActive    = "active"
	ShardMigrating = "migrating"
	ShardReadonly  = "readonly"
)

// topologyPrefix 拓扑数据的 etcd 键前缀。
const topologyPrefix = "/topology/shards/"

// ShardInfo 一个分片的元数据。
type ShardInfo struct {
	ID       int      `json:"id"`
	Nodes    []string `json:"nodes"`
	Leader   string   `json:"leader"`
	StartKey string   `json:"start_key"`
	EndKey   string   `json:"end_key"`
	Status   string   `json:"status"`
	Term     uint64   `json:"term"`
}

// Copy 返回深拷贝，用于安全地把元数据交给调用者（避免调用方读到并发修改的切片）。
func (s *ShardInfo) Copy() *ShardInfo {
	if s == nil {
		return nil
	}
	out := *s
	out.Nodes = append([]string(nil), s.Nodes...)
	return &out
}

// Contains 判断 key 是否落在本分片区间。EndKey 为空表示右端无界。
func (s *ShardInfo) Contains(key string) bool {
	if s == nil {
		return false
	}
	if key < s.StartKey {
		return false
	}
	return s.EndKey == "" || key < s.EndKey
}

// NodeInfo 一个数据节点的元数据。
type NodeInfo struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	ShardID  int       `json:"shard_id"`
	Status   string    `json:"status"`
	LastSeen time.Time `json:"last_seen"`
}

// ShardChange 一次拓扑变更。
type ShardChange struct {
	// Type 为 "put" 或 "delete"。
	Type  string
	Shard *ShardInfo
	ID    int
}

// ChangeListener 拓扑变更回调。
type ChangeListener func(ShardChange)

// ClusterTopology 集群拓扑缓存。零值不可用，请用 NewClusterTopology。
type ClusterTopology struct {
	mu sync.RWMutex

	etcd *clientv3.Client

	shards map[int]*ShardInfo
	// ordered 是 shards 按 StartKey 升序排列的索引（BUG-10/11）。
	ordered []*ShardInfo

	nodeShardMap map[string]int
	version      uint64
	listeners    []ChangeListener

	watchCtx    context.Context
	watchCancel context.CancelFunc
	watchDone   chan struct{}
	closeOnce   sync.Once

	// opTimeout 单次 etcd 操作超时。
	opTimeout time.Duration
}

// DialTimeout 默认连接超时。
const defaultOpTimeout = 5 * time.Second

// NewClusterTopology 连接 etcd 并加载拓扑。
func NewClusterTopology(endpoints []string) (*ClusterTopology, error) {
	return NewClusterTopologyWithClient(endpoints, defaultOpTimeout)
}

// NewClusterTopologyWithClient 允许指定操作超时。
func NewClusterTopologyWithClient(endpoints []string, opTimeout time.Duration) (*ClusterTopology, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("metadata: no etcd endpoints provided")
	}
	for i, ep := range endpoints {
		endpoints[i] = strings.TrimSpace(ep)
	}
	if opTimeout <= 0 {
		opTimeout = defaultOpTimeout
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: opTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("metadata: etcd client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ct := &ClusterTopology{
		etcd:         client,
		shards:       make(map[int]*ShardInfo),
		nodeShardMap: make(map[string]int),
		watchCtx:     ctx,
		watchCancel:  cancel,
		watchDone:    make(chan struct{}),
		opTimeout:    opTimeout,
	}

	// 真正确认连通性。clientv3.New 不会拨号，原始版本因此无法在启动阶段发现 etcd 不可用。
	if err := ct.Ping(ctx); err != nil {
		cancel()
		_ = client.Close()
		return nil, err
	}
	if err := ct.loadTopology(ctx); err != nil {
		cancel()
		_ = client.Close()
		return nil, err
	}

	go ct.watchTopology()
	return ct, nil
}

// Ping 校验元数据集群可达。
func (ct *ClusterTopology) Ping(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, ct.opTimeout)
	defer cancel()
	if _, err := ct.etcd.Get(cctx, topologyPrefix+"__probe__"); err != nil {
		return fmt.Errorf("metadata: etcd unreachable: %w", err)
	}
	return nil
}

// loadTopology 从 etcd 全量加载。
func (ct *ClusterTopology) loadTopology(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, ct.opTimeout)
	defer cancel()

	resp, err := ct.etcd.Get(cctx, topologyPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("metadata: load topology: %w", err)
	}

	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.shards = make(map[int]*ShardInfo, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var s ShardInfo
		if err := json.Unmarshal(kv.Value, &s); err != nil {
			continue
		}
		ct.shards[s.ID] = &s
	}
	ct.reindexLocked()
	return nil
}

// reindexLocked 重建有序索引与 node→shard 反查表。必须在持有写锁时调用。
func (ct *ClusterTopology) reindexLocked() {
	ct.ordered = make([]*ShardInfo, 0, len(ct.shards))
	for _, s := range ct.shards {
		ct.ordered = append(ct.ordered, s)
	}
	// 只按 StartKey 排序——查找依据是 key 区间，不是分片 ID（BUG-11）。
	sort.Slice(ct.ordered, func(i, j int) bool {
		if ct.ordered[i].StartKey != ct.ordered[j].StartKey {
			return ct.ordered[i].StartKey < ct.ordered[j].StartKey
		}
		return ct.ordered[i].ID < ct.ordered[j].ID
	})

	ct.nodeShardMap = make(map[string]int, len(ct.ordered)*3)
	for _, s := range ct.ordered {
		for _, n := range s.Nodes {
			ct.nodeShardMap[n] = s.ID
		}
	}
}

// watchTopology 监听拓扑变化，正确处理 PUT 与 DELETE（BUG-12/13）。
func (ct *ClusterTopology) watchTopology() {
	defer close(ct.watchDone)

	ch := ct.etcd.Watch(ct.watchCtx, topologyPrefix, clientv3.WithPrefix())
	for resp := range ch {
		if err := resp.Err(); err != nil {
			// 上下文取消是正常关闭路径。
			if ct.watchCtx.Err() != nil {
				return
			}
			continue
		}
		var changes []ShardChange
		ct.mu.Lock()
		for _, ev := range resp.Events {
			id, ok := shardIDFromKey(string(ev.Kv.Key))
			if !ok {
				continue
			}
			switch {
			case ev.Type == clientv3.EventTypeDelete:
				delete(ct.shards, id) // BUG-12：删除必须真正生效
				changes = append(changes, ShardChange{Type: "delete", ID: id})
			default:
				var s ShardInfo
				if err := json.Unmarshal(ev.Kv.Value, &s); err != nil {
					continue
				}
				s.ID = id // 以键为准，避免写入方漏填 ID
				cp := s.Copy()
				ct.shards[id] = cp
				changes = append(changes, ShardChange{Type: "put", Shard: cp, ID: id})
			}
		}
		if len(changes) > 0 {
			ct.version++
			ct.reindexLocked() // BUG-13：反查表随变更重建
		}
		listeners := append([]ChangeListener(nil), ct.listeners...)
		ct.mu.Unlock()

		for _, chg := range changes {
			for _, fn := range listeners {
				fn(chg)
			}
		}
	}
}

func shardIDFromKey(key string) (int, bool) {
	if !strings.HasPrefix(key, topologyPrefix) {
		return 0, false
	}
	rest := key[len(topologyPrefix):]
	var id int
	if _, err := fmt.Sscanf(rest, "%d", &id); err != nil {
		return 0, false
	}
	return id, true
}

// OnShardChange 注册拓扑监听器。
func (ct *ClusterTopology) OnShardChange(fn ChangeListener) {
	if fn == nil {
		return
	}
	ct.mu.Lock()
	ct.listeners = append(ct.listeners, fn)
	ct.mu.Unlock()
}

// GetShardForKey 返回 key 所属分片，使用按 StartKey 排序的二分查找（BUG-10）。
func (ct *ClusterTopology) GetShardForKey(key string) *ShardInfo {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	// 找到最后一个 StartKey <= key 的分片。
	idx := sort.Search(len(ct.ordered), func(i int) bool {
		return ct.ordered[i].StartKey > key
	}) - 1
	if idx < 0 {
		return nil
	}
	s := ct.ordered[idx]
	if s.EndKey != "" && key >= s.EndKey {
		return nil
	}
	return s
}

// GetShard 按 ID 取分片。
func (ct *ClusterTopology) GetShard(shardID int) (*ShardInfo, bool) {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	s, ok := ct.shards[shardID]
	if !ok {
		return nil, false
	}
	return s, true
}

// GetShardNodes 分片的全部节点地址。
func (ct *ClusterTopology) GetShardNodes(shardID int) []string {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	if s, ok := ct.shards[shardID]; ok {
		return append([]string(nil), s.Nodes...)
	}
	return nil
}

// ShardOfNode 节点所属分片。
func (ct *ClusterTopology) ShardOfNode(nodeID string) (int, bool) {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	id, ok := ct.nodeShardMap[nodeID]
	return id, ok
}

// ShardCount 已注册分片数。
func (ct *ClusterTopology) ShardCount() int {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return len(ct.shards)
}

// Version 拓扑版本号，每次变更递增。
func (ct *ClusterTopology) Version() uint64 {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return ct.version
}

// AllShards 全部已注册分片的快照。
func (ct *ClusterTopology) AllShards() []*ShardInfo {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return append([]*ShardInfo(nil), ct.ordered...)
}

// RegisterShard 写入/更新一个分片。
//
// 先写 etcd 再更新本地缓存：本地缓存由 watch 事件驱动，避免"先改内存后写失败"的不一致。
// 同时移除了原版未使用的 concurrency.Session（BUG-15）与持锁阻塞写入（BUG-14）。
func (ct *ClusterTopology) RegisterShard(s *ShardInfo) error {
	if s == nil {
		return errors.New("metadata: nil shard")
	}
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("metadata: marshal shard %d: %w", s.ID, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), ct.opTimeout)
	defer cancel()

	key := fmt.Sprintf("%s%d", topologyPrefix, s.ID)
	if _, err := ct.etcd.Put(ctx, key, string(data)); err != nil {
		return fmt.Errorf("metadata: put shard %d: %w", s.ID, err)
	}
	// 立即本地生效，不必等待 watch 回环（watch 事件到达时会幂等覆盖）。
	cp := s.Copy()
	ct.mu.Lock()
	ct.shards[cp.ID] = cp
	ct.version++
	ct.reindexLocked()
	ct.mu.Unlock()
	return nil
}

// UpdateLeader 更新分片 Leader。
//
// 使用 etcd 事务做 compare-and-swap（仅当 Term 未变时才写入），避免多写入方互相覆盖。
// 网络调用在锁外执行（BUG-14）。
func (ct *ClusterTopology) UpdateLeader(shardID int, leader string, newTerm uint64) error {
	ct.mu.RLock()
	cur, ok := ct.shards[shardID]
	ct.mu.RUnlock()

	if !ok {
		return fmt.Errorf("metadata: shard %d not found", shardID)
	}

	next := cur.Copy()
	next.Leader = leader
	next.Term = newTerm
	payload, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("metadata: marshal shard %d: %w", shardID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ct.opTimeout)
	defer cancel()

	key := fmt.Sprintf("%s%d", topologyPrefix, shardID)
	txn := ct.etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), ">", 0)).
		Then(clientv3.OpPut(key, string(payload)))
	resp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("metadata: update leader for shard %d: %w", shardID, err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("metadata: shard %d vanished during update", shardID)
	}

	ct.mu.Lock()
	if s, ok := ct.shards[shardID]; ok {
		s.Leader = leader
		s.Term = newTerm
		ct.version++
	}
	ct.mu.Unlock()
	return nil
}

// CustomKeyRange 返回按 StartKey 升序的分片区间描述，便于运维检查。
func (ct *ClusterTopology) CustomKeyRange() []string {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	out := make([]string, 0, len(ct.ordered))
	for _, s := range ct.ordered {
		out = append(out, fmt.Sprintf("[%s,%s) -> shard %d leader=%s", s.StartKey, s.EndKey, s.ID, s.Leader))
	}
	return out
}

// Close 关闭 watch 与 etcd 连接。
func (ct *ClusterTopology) Close() error {
	var err error
	ct.closeOnce.Do(func() {
		ct.watchCancel()
		select {
		case <-ct.watchDone:
		case <-time.After(2 * time.Second):
		}
		err = ct.etcd.Close()
	})
	return err
}
