package raft

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// startSingleNode 在临时目录启动一个单节点 Raft 集群并等它选主。
func startSingleNode(tb testing.TB, commitTimeout time.Duration) *KVStore {
	tb.Helper()
	dir := filepath.Join(tb.TempDir(), "raft")
	ks, err := NewKVStore(Config{
		NodeID:        "bench-node",
		ShardID:       0,
		BindAddr:      "127.0.0.1:0", // 让内核选端口，避免并行测试撞车
		RaftDir:       dir,
		CommitTimeout: commitTimeout,
	})
	if err != nil {
		tb.Fatalf("NewKVStore: %v", err)
	}
	tb.Cleanup(func() { _ = ks.Shutdown() })

	if _, err := ks.Bootstrap(); err != nil {
		tb.Fatalf("Bootstrap: %v", err)
	}
	if _, err := ks.WaitForLeader(15 * time.Second); err != nil {
		tb.Fatalf("WaitForLeader: %v", err)
	}
	return ks
}

// BenchmarkSet 测量写入延迟（含一次 Raft 往返）。
func BenchmarkSet(b *testing.B) {
	ks := startSingleNode(b, 0)
	payload := make([]byte, 128)
	for i := range payload {
		payload[i] = 'x'
	}
	val := string(payload)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ks.Set(fmt.Sprintf("key-%d", i), val); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
}

// BenchmarkGetRaw 测量纯状态机读取（无任何一致性协调）。
// 这是"读路径的成本下限"，用于把协调开销从数据面开销里分离出来。
func BenchmarkGetRaw(b *testing.B) {
	ks := startSingleNode(b, 0)
	for i := 0; i < 1000; i++ {
		if err := ks.Set(fmt.Sprintf("key-%d", i), "v"); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := ks.Get(fmt.Sprintf("key-%d", i%1000)); !ok {
			b.Fatal("missing key")
		}
	}
}

// BenchmarkBarrier 测量单次 Barrier 的成本。
//
// 这是关键诊断：如果 Barrier 本身就要几十毫秒，那么把 Barrier 放进读路径
// （线性化读的最简实现）会直接把读延迟钉死在 CommitTimeout 上，
// 这正是端到端跑出 p50=39.91ms 的原因。
func BenchmarkBarrier(b *testing.B) {
	ks := startSingleNode(b, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ks.Barrier(5 * time.Second); err != nil {
			b.Fatalf("Barrier: %v", err)
		}
	}
}

// BenchmarkBarrierVsCommitTimeout 对照不同 CommitTimeout 下的 Barrier 成本。
func BenchmarkBarrierVsCommitTimeout(b *testing.B) {
	for _, ct := range []time.Duration{
		1 * time.Millisecond,
		5 * time.Millisecond,
		20 * time.Millisecond,
	} {
		b.Run(fmt.Sprintf("commit=%s", ct), func(b *testing.B) {
			ks := startSingleNode(b, ct)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := ks.Barrier(5 * time.Second); err != nil {
					b.Fatalf("Barrier: %v", err)
				}
			}
		})
	}
}

// BenchmarkSetParallel 测量并发写吞吐上限。
func BenchmarkSetParallel(b *testing.B) {
	ks := startSingleNode(b, 0)
	payload := make([]byte, 128)
	for i := range payload {
		payload[i] = 'x'
	}
	val := string(payload)

	var counter int64
	var mu sync.Mutex

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		mu.Lock()
		counter++
		id := counter
		mu.Unlock()
		i := 0
		for pb.Next() {
			if err := ks.Set(fmt.Sprintf("p%d-%d", id, i), val); err != nil {
				b.Errorf("Set: %v", err)
				return
			}
			i++
		}
	})
}

// TestSetGetRoundTrip 验证单节点上的基本读写正确性。
func TestSetGetRoundTrip(t *testing.T) {
	ks := startSingleNode(t, 0)

	if err := ks.Set("alpha", "1"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := ks.Set("beta", "2"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := ks.Delete("alpha"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if v, ok := ks.Get("beta"); !ok || v != "2" {
		t.Fatalf("beta = (%q,%v)，期望 (\"2\",true)", v, ok)
	}
	if _, ok := ks.Get("alpha"); ok {
		t.Fatal("alpha 已删除却仍存在")
	}
	if ks.Len() != 1 {
		t.Fatalf("Len = %d，期望 1", ks.Len())
	}
}

// TestBootstrapIdempotent 是 BUG-3 的回归测试。
//
// 原实现：状态为 Follower/Candidate 时直接 return（逻辑反了），
// 且重启后重复调用 BootstrapCluster 会返回 "already bootstrapped" 错误。
func TestBootstrapIdempotent(t *testing.T) {
	ks := startSingleNode(t, 0)

	// 第二次 Bootstrap 必须是无害的空操作，而不是错误。
	res, err := ks.Bootstrap()
	if err != nil {
		t.Fatalf("重复 Bootstrap 报错: %v", err)
	}
	if !res.AlreadyInitialized {
		t.Fatalf("重复 Bootstrap 应报告 AlreadyInitialized，得到 %+v", res)
	}
	if res.ExistingVoters != 1 {
		t.Fatalf("已有投票成员数 = %d，期望 1", res.ExistingVoters)
	}
}

// TestNonLeaderRejectsWriteAndMembership 验证非 Leader 上的写与成员变更被拒绝。
//
// BUG-4 相关：原 Join 在本地节点上调用 AddVoter，只有本地恰好是 Leader 才生效。
func TestNonLeaderRejectsWriteAndMembership(t *testing.T) {
	// 单节点集群里它就是 Leader；这里构造一个"未引导"的节点来验证非 Leader 路径。
	dir := filepath.Join(t.TempDir(), "raft")
	ks, err := NewKVStore(Config{
		NodeID:   "lonely",
		BindAddr: "127.0.0.1:0",
		RaftDir:  dir,
	})
	if err != nil {
		t.Fatalf("NewKVStore: %v", err)
	}
	defer ks.Shutdown()

	// 未引导 => 不是 Leader。
	if ks.IsLeader() {
		t.Fatal("未引导的节点不应是 Leader")
	}
	if err := ks.Set("k", "v"); err != ErrNotLeader {
		t.Fatalf("非 Leader 上的 Set 应返回 ErrNotLeader，得到 %v", err)
	}
	if err := ks.AddVoter("other", "127.0.0.1:1"); err != ErrNotLeader {
		t.Fatalf("非 Leader 上的 AddVoter 应返回 ErrNotLeader，得到 %v", err)
	}
	if err := ks.RemoveServer("other"); err != ErrNotLeader {
		t.Fatalf("非 Leader 上的 RemoveServer 应返回 ErrNotLeader，得到 %v", err)
	}
}

// TestConfigurationReflectsBootstrap 验证引导后配置里正好有本节点。
func TestConfigurationReflectsBootstrap(t *testing.T) {
	ks := startSingleNode(t, 0)
	cfg := ks.Configuration()
	if len(cfg) != 1 {
		t.Fatalf("配置成员数 = %d，期望 1", len(cfg))
	}
	if string(cfg[0].ID) != "bench-node" {
		t.Fatalf("成员 ID = %q，期望 bench-node", cfg[0].ID)
	}
}

// TestThreeNodeReplication 三节点复制正确性（本包内建集群，不依赖 pkg/cluster）。
func TestThreeNodeReplication(t *testing.T) {
	base := t.TempDir()

	mk := func(id, addr string) *KVStore {
		ks, err := NewKVStore(Config{
			NodeID:   id,
			BindAddr: addr,
			RaftDir:  filepath.Join(base, id),
		})
		if err != nil {
			t.Fatalf("NewKVStore(%s): %v", id, err)
		}
		t.Cleanup(func() { _ = ks.Shutdown() })
		return ks
	}

	n1 := mk("n1", "127.0.0.1:0")
	n2 := mk("n2", "127.0.0.1:0")
	n3 := mk("n3", "127.0.0.1:0")

	if _, err := n1.Bootstrap(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := n1.WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}

	// 把 n2/n3 以投票成员加入。
	if err := n1.AddVoter("n2", n2.BindAddr()); err != nil {
		t.Fatalf("AddVoter n2: %v", err)
	}
	if err := n1.AddVoter("n3", n3.BindAddr()); err != nil {
		t.Fatalf("AddVoter n3: %v", err)
	}

	if got := len(n1.Configuration()); got != 3 {
		t.Fatalf("配置成员数 = %d，期望 3", got)
	}

	// 写 200 个键，等待全部副本追上。
	const n = 200
	for i := 0; i < n; i++ {
		if err := n1.Set(fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	// 等 follower 应用完毕。
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if n2.Len() == n && n3.Len() == n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n2.Len() != n {
		t.Fatalf("n2 键数 = %d，期望 %d", n2.Len(), n)
	}
	if n3.Len() != n {
		t.Fatalf("n3 键数 = %d，期望 %d", n3.Len(), n)
	}

	// 抽查内容一致。
	for _, i := range []int{0, 99, 199} {
		key := fmt.Sprintf("k%03d", i)
		want := fmt.Sprintf("v%d", i)
		for _, ks := range []*KVStore{n1, n2, n3} {
			if v, ok := ks.Get(key); !ok || v != want {
				t.Fatalf("%s: %s = (%q,%v)，期望 (%q,true)", ks.NodeID(), key, v, ok, want)
			}
		}
	}
}

// TestLeaderElectionAfterCrash 验证 leader 崩溃后能在合理时间内重新选主。
func TestLeaderElectionAfterCrash(t *testing.T) {
	base := t.TempDir()

	mk := func(id, addr string) *KVStore {
		ks, err := NewKVStore(Config{
			NodeID:   id,
			BindAddr: addr,
			RaftDir:  filepath.Join(base, id),
		})
		if err != nil {
			t.Fatalf("NewKVStore(%s): %v", id, err)
		}
		return ks
	}

	nodes := []*KVStore{
		mk("a1", "127.0.0.1:0"),
		mk("a2", "127.0.0.1:0"),
		mk("a3", "127.0.0.1:0"),
	}

	if _, err := nodes[0].Bootstrap(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := nodes[0].WaitForLeader(15 * time.Second); err != nil {
		t.Fatalf("wait leader: %v", err)
	}
	for _, n := range nodes[1:] {
		if err := nodes[0].AddVoter(n.NodeID(), n.BindAddr()); err != nil {
			t.Fatalf("AddVoter %s: %v", n.NodeID(), err)
		}
	}

	// 找到 leader 并杀掉它。
	var leader *KVStore
	for _, n := range nodes {
		if n.IsLeader() {
			leader = n
		}
	}
	if leader == nil {
		t.Fatal("没有 leader")
	}
	start := time.Now()
	if err := leader.Shutdown(); err != nil {
		t.Fatalf("shutdown leader: %v", err)
	}

	// 剩余两个节点应在选举超时量级内选出新 leader。
	var survivors []*KVStore
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
			defer n.Shutdown()
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range survivors {
			if n.IsLeader() {
				t.Logf("新 leader %s，耗时 %s", n.NodeID(), time.Since(start).Round(time.Millisecond))
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("15s 内未选出新 leader")
}
