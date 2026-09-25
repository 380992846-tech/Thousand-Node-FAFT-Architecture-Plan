package metadata

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

// buildShards 生成 n 个连续区间。
func buildShards(n int) []*ShardInfo {
	out := make([]*ShardInfo, 0, n)
	t := NewLocalTopology("")
	for i := 0; i < n; i++ {
		s, e := t.RangeForShard(i, n)
		out = append(out, &ShardInfo{
			ID: i, StartKey: s, EndKey: e,
			Nodes:  []string{fmt.Sprintf("node-%d", i)},
			Leader: fmt.Sprintf("node-%d", i),
			Status: ShardActive,
		})
	}
	return out
}

// TestLocalTopologyLookup 验证基本查找语义。
func TestLocalTopologyLookup(t *testing.T) {
	tp := NewLocalTopology("")
	for _, s := range buildShards(16) {
		tp.SetShard(s)
	}

	// 区间是 [start, end)，且最后一个分片 EndKey 为空（无上界）。
	for i, s := range buildShards(16) {
		got := tp.GetShardForKey(s.StartKey)
		if got == nil {
			t.Fatalf("shard %d 的 StartKey %q 查不到分片", i, s.StartKey)
		}
		if got.ID != s.ID {
			t.Fatalf("StartKey %q: 得到分片 %d，期望 %d", s.StartKey, got.ID, s.ID)
		}
	}

	// 落在最后一个分片内的任意大 key 都应命中最后一个分片。
	last := 15
	got := tp.GetShardForKey("zzzzzz")
	if got == nil || got.ID != last {
		t.Fatalf("大 key 应命中最后一个分片，得到 %v", got)
	}

	// 小于最小 StartKey 的 key 无归属。
	if got := tp.GetShardForKey("\x00"); got != nil {
		t.Fatalf("过小的 key 应无归属，得到分片 %d", got.ID)
	}
}

// TestLookupCorrectForAllKeys 用暴力对照验证二分查找在全键空间上正确。
//
// 这是 BUG-10 / BUG-11 的回归测试：
//   - 原实现按分片 ID 排序后线性扫描，ID 顺序与 key 顺序不一致时返回错误分片；
//   - 且每次调用都重建并排序切片。
func TestLookupCorrectForAllKeys(t *testing.T) {
	const n = 64
	shards := buildShards(n)

	tp := NewLocalTopology("")
	for _, s := range shards {
		tp.SetShard(s)
	}

	// 暴力查找作为参照。
	brute := func(key string) int {
		for _, s := range shards {
			if key >= s.StartKey && (s.EndKey == "" || key < s.EndKey) {
				return s.ID
			}
		}
		return -1
	}

	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 20000; i++ {
		key := fmt.Sprintf("%04x", rng.Intn(1<<16))
		want := brute(key)
		got := tp.GetShardForKey(key)
		gotID := -1
		if got != nil {
			gotID = got.ID
		}
		if gotID != want {
			t.Fatalf("key %q: 二分得到 %d，暴力得到 %d", key, gotID, want)
		}
	}
}

// TestLookupCorrectWhenIDsDisagreeWithKeyOrder 是 BUG-11 的定向回归测试。
//
// 构造一组"分片 ID 顺序与 key 区间顺序完全相反"的拓扑。
// 原实现按 ID 排序后扫描，必然返回错误分片。
func TestLookupCorrectWhenIDsDisagreeWithKeyOrder(t *testing.T) {
	tp := NewLocalTopology("")
	// ID 0 拥有最大的 key 区间，ID 2 拥有最小的。
	tp.SetShard(&ShardInfo{ID: 0, StartKey: "8000", EndKey: "", Nodes: []string{"a"}})
	tp.SetShard(&ShardInfo{ID: 1, StartKey: "4000", EndKey: "8000", Nodes: []string{"b"}})
	tp.SetShard(&ShardInfo{ID: 2, StartKey: "0000", EndKey: "4000", Nodes: []string{"c"}})

	cases := map[string]int{
		"0000": 2,
		"3fff": 2,
		"4000": 1,
		"7fff": 1,
		"8000": 0,
		"ffff": 0,
	}
	for key, want := range cases {
		got := tp.GetShardForKey(key)
		if got == nil {
			t.Fatalf("key %q: 无归属，期望分片 %d", key, want)
		}
		if got.ID != want {
			t.Errorf("key %q: 得到分片 %d，期望 %d（按 ID 排序会错）", key, got.ID, want)
		}
	}
}

// TestDeleteShardTakesEffect 是 BUG-12 的回归测试。
//
// 原 watchTopology 对 DELETE 事件直接 continue，被删的分片永久残留在缓存里，
// 之后所有落在那段 key 上的请求都会被路由到一个已经不存在的分片。
func TestDeleteShardTakesEffect(t *testing.T) {
	tp := NewLocalTopology("")
	for _, s := range buildShards(4) {
		tp.SetShard(s)
	}
	if tp.ShardCount() != 4 {
		t.Fatalf("ShardCount = %d，期望 4", tp.ShardCount())
	}

	// 记录变更事件。
	var mu sync.Mutex
	var events []ShardChange
	tp.OnShardChange(func(c ShardChange) {
		mu.Lock()
		events = append(events, c)
		mu.Unlock()
	})

	s := buildShards(4)[1]
	tp.DeleteShard(s.ID)

	if tp.ShardCount() != 3 {
		t.Fatalf("删除后 ShardCount = %d，期望 3", tp.ShardCount())
	}
	// 该区间现在应无归属。
	if got := tp.GetShardForKey(s.StartKey); got != nil {
		t.Fatalf("已删除分片的区间仍返回分片 %d", got.ID)
	}

	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, e := range events {
		if e.Type == "delete" && e.ID == s.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("未收到 delete 事件，收到: %+v", events)
	}
}

// TestReindexAfterMutation 验证增删之后有序索引仍然正确（BUG-13 相关）。
func TestReindexAfterMutation(t *testing.T) {
	tp := NewLocalTopology("")
	for _, s := range buildShards(8) {
		tp.SetShard(s)
	}

	// 删掉中间几个。
	tp.DeleteShard(3)
	tp.DeleteShard(5)

	all := tp.AllShards()
	if len(all) != 6 {
		t.Fatalf("AllShards 长度 = %d，期望 6", len(all))
	}
	// 必须仍按 StartKey 升序。
	if !sort.SliceIsSorted(all, func(i, j int) bool { return all[i].StartKey < all[j].StartKey }) {
		t.Fatal("AllShards 未按 StartKey 升序")
	}

	// 每个现存的 StartKey 都能查到正确的分片。
	for _, s := range all {
		got := tp.GetShardForKey(s.StartKey)
		if got == nil || got.ID != s.ID {
			t.Fatalf("StartKey %q: 得到 %v，期望分片 %d", s.StartKey, got, s.ID)
		}
	}
}

// TestConcurrentAccess 验证拓扑在并发读写下的安全性与一致性。
func TestConcurrentAccess(t *testing.T) {
	tp := NewLocalTopology("")
	for _, s := range buildShards(32) {
		tp.SetShard(s)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var stopOnce sync.Once
	halt := func() { stopOnce.Do(func() { close(stop) }) }

	// 读方。
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				tp.GetShardForKey(fmt.Sprintf("%04x", rng.Intn(1<<16)))
				_ = tp.AllShards()
				_ = tp.Version()
				_ = tp.ShardCount()
			}
		}()
	}

	// 写方。
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				s := buildShards(32)[(id*7+i)%32]
				tp.SetShard(s)
				if i%3 == 0 {
					tp.DeleteShard(s.ID)
				}
			}
			halt()
		}(w)
	}
	wg.Wait()
}

// TestSegmentLookupPerformance 提醒：查找必须是 O(log n)。
//
// 这里不做严格基准断言（会因机器而异），但用 BenchmarkLookup 记录数量级，
// 与原实现的 O(n log n)/次 形成可对比的证据。
func BenchmarkLookup(b *testing.B) {
	for _, n := range []int{16, 200, 1000} {
		b.Run(fmt.Sprintf("shards=%d", n), func(b *testing.B) {
			tp := NewLocalTopology("")
			for _, s := range buildShards(n) {
				tp.SetShard(s)
			}
			rng := rand.New(rand.NewSource(3))
			keys := make([]string, 4096)
			for i := range keys {
				keys[i] = fmt.Sprintf("%04x", rng.Intn(1<<16))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = tp.GetShardForKey(keys[i%len(keys)])
			}
		})
	}
}

// TestSplitKeySpace 验证 key 空间切分覆盖完整且不重叠。
func TestSplitKeySpace(t *testing.T) {
	const n = 37
	ranges := SplitKeySpace("p", n)
	if len(ranges) != n {
		t.Fatalf("区间数 = %d，期望 %d", len(ranges), n)
	}
	for i := 1; i < n; i++ {
		if ranges[i][0] != ranges[i-1][1] {
			t.Fatalf("区间 %d 起点 %q != 上一区间终点 %q", i, ranges[i][0], ranges[i-1][1])
		}
	}
	if ranges[n-1][1] != "" {
		t.Fatalf("最后一个区间应为无上界，得到 %q", ranges[n-1][1])
	}
}
