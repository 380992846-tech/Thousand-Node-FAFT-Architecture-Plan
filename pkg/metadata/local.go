// pkg/metadata/local.go
//
// LocalTopology：进程内拓扑，不依赖 etcd。
//
// 存在的理由：要对"分片数 × 副本数"做可复现的参数扫描（这是本文实验部分的核心），
// 就必须能在一次进程里构造多个拓扑视图，而不必为每次实验起一个 etcd 集群。
// 它与 ClusterTopology 实现同一个 Topology 接口，因此 router/client 无需感知差异。
package metadata

import (
	"fmt"
	"sort"
	"sync"
)

// Topology 是路由层与客户端所需的拓扑视图抽象。
//
// ClusterTopology（etcd 支撑）与 LocalTopology（进程内）都实现它。
type Topology interface {
	// GetShardForKey 返回 key 所属分片；无归属时返回 nil。
	GetShardForKey(key string) *ShardInfo
	// AllShards 返回按 StartKey 升序的全部分片。
	AllShards() []*ShardInfo
	// ShardCount 分片数量。
	ShardCount() int
	// Version 拓扑版本号。
	Version() uint64
	// OnShardChange 注册拓扑变更回调。
	OnShardChange(fn ChangeListener)
}

// 编译期断言。
var (
	_ Topology = (*ClusterTopology)(nil)
	_ Topology = (*LocalTopology)(nil)
)

// LocalTopology 进程内拓扑。
type LocalTopology struct {
	mu       sync.RWMutex
	shards   map[int]*ShardInfo
	ordered  []*ShardInfo
	version  uint64
	listen   []ChangeListener
	keyStart string
}

// NewLocalTopology 创建进程内拓扑。keyStart 为分片 0 的起始 key（可为空）。
func NewLocalTopology(keyStart string) *LocalTopology {
	return &LocalTopology{
		shards:   make(map[int]*ShardInfo),
		keyStart: keyStart,
	}
}

// SetShard 写入/覆盖一个分片并广播变更。
func (t *LocalTopology) SetShard(s *ShardInfo) {
	if s == nil {
		return
	}
	cp := s.Copy()

	t.mu.Lock()
	t.shards[cp.ID] = cp
	t.version++
	t.reindexLocked()
	listeners := append([]ChangeListener(nil), t.listen...)
	t.mu.Unlock()

	chg := ShardChange{Type: "put", Shard: cp.Copy(), ID: cp.ID}
	for _, fn := range listeners {
		fn(chg)
	}
}

// DeleteShard 移除一个分片并广播变更。
func (t *LocalTopology) DeleteShard(id int) {
	t.mu.Lock()
	if _, ok := t.shards[id]; !ok {
		t.mu.Unlock()
		return
	}
	delete(t.shards, id)
	t.version++
	t.reindexLocked()
	listeners := append([]ChangeListener(nil), t.listen...)
	t.mu.Unlock()

	chg := ShardChange{Type: "delete", ID: id}
	for _, fn := range listeners {
		fn(chg)
	}
}

func (t *LocalTopology) reindexLocked() {
	t.ordered = t.ordered[:0]
	for _, s := range t.shards {
		t.ordered = append(t.ordered, s)
	}
	sort.Slice(t.ordered, func(i, j int) bool {
		if t.ordered[i].StartKey != t.ordered[j].StartKey {
			return t.ordered[i].StartKey < t.ordered[j].StartKey
		}
		return t.ordered[i].ID < t.ordered[j].ID
	})
}

// GetShardForKey 二分查找。
func (t *LocalTopology) GetShardForKey(key string) *ShardInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	idx := sort.Search(len(t.ordered), func(i int) bool {
		return t.ordered[i].StartKey > key
	}) - 1
	if idx < 0 {
		return nil
	}
	s := t.ordered[idx]
	if s.EndKey != "" && key >= s.EndKey {
		return nil
	}
	return s
}

// AllShards 按 StartKey 升序返回全部（浅拷贝切片，元素为内部指针，只读使用）。
func (t *LocalTopology) AllShards() []*ShardInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return append([]*ShardInfo(nil), t.ordered...)
}

// ShardCount 分片数量。
func (t *LocalTopology) ShardCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.shards)
}

// Version 拓扑版本号。
func (t *LocalTopology) Version() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.version
}

// OnShardChange 注册回调。
func (t *LocalTopology) OnShardChange(fn ChangeListener) {
	if fn == nil {
		return
	}
	t.mu.Lock()
	t.listen = append(t.listen, fn)
	t.mu.Unlock()
}

// Close 清空拓扑（接口一致性用）。
func (t *LocalTopology) Close() { t.mu.Lock(); t.shards = map[int]*ShardInfo{}; t.ordered = nil; t.mu.Unlock() }

// RangeForShard 把一个连续的 key 空间切成 n 份，返回第 idx 份的 [start, end)。
//
// 使用零填充的 16 位十六进制编码，保证字典序 == 数值序，从而与二分查找一致。
func (t *LocalTopology) RangeForShard(idx, n int) (start, end string) {
	const keySpace = 1 << 16
	if n <= 0 {
		return "", ""
	}
	lo := idx * keySpace / n
	hi := (idx + 1) * keySpace / n
	start = encodeKey(t.keyStart, lo)
	if idx == n-1 {
		return start, ""
	}
	return start, encodeKey(t.keyStart, hi)
}

func encodeKey(prefix string, v int) string {
	return fmt.Sprintf("%s%04x", prefix, v)
}

// SplitKeySpace 便捷函数：生成 n 个分片的 [start,end) 区间。
func SplitKeySpace(prefix string, n int) [][2]string {
	t := NewLocalTopology(prefix)
	out := make([][2]string, 0, n)
	for i := 0; i < n; i++ {
		s, e := t.RangeForShard(i, n)
		out = append(out, [2]string{s, e})
	}
	return out
}
