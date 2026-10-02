package raft

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// startBatchingNode 启动一个指定批处理配置的单节点集群。
// batchSize<=1 表示关闭批处理。
//
// 若启用了批处理，会等到批循环真正就绪再返回 —— 否则在当选 Leader 后的
// 那个 ~50ms 窗口里，写会走单条提案路径，把冷启动噪声混进测量。
func startBatchingNode(tb testing.TB, batchSize int, batchWait time.Duration) *KVStore {
	tb.Helper()
	dir := filepath.Join(tb.TempDir(), "raft")
	ks, err := NewKVStore(Config{
		NodeID:    "batch-node",
		BindAddr:  "127.0.0.1:0",
		RaftDir:   dir,
		BatchSize: batchSize,
		BatchWait: batchWait,
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
	if batchSize > 1 && !ks.WaitBatchReady(5*time.Second) {
		tb.Fatalf("批处理循环未在 5s 内就绪")
	}
	return ks
}

// BenchmarkSetBatch_Off 关闭批处理：每条命令一次提案 + 一次 fsync。
// 这是写路径的基线。
func BenchmarkSetBatch_Off(b *testing.B) {
	ks := startBatchingNode(b, 1, 0)
	val := make([]byte, 128)
	for i := range val {
		val[i] = 'x'
	}
	v := string(val)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ks.Set(fmt.Sprintf("key-%d", i), v); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
}

// BenchmarkSetBatch_On 开启批处理，批大小 64，攒批窗口 2ms。
//
// 预期：单次操作的平均耗时显著下降（fsync 被摊薄），
// 代价是轻载下每条命令多等最多 BatchWait。
func BenchmarkSetBatch_On(b *testing.B) {
	ks := startBatchingNode(b, 64, 2*time.Millisecond)
	val := make([]byte, 128)
	for i := range val {
		val[i] = 'x'
	}
	v := string(val)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ks.Set(fmt.Sprintf("key-%d", i), v); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
	b.StopTimer()
	st := ks.BatchStats()
	b.ReportMetric(st.Amortization, "cmds/fync")
}

// BenchmarkSetBatch_Sizes 扫批大小，用于找出 fsync 摊薄的拐点。
func BenchmarkSetBatch_Sizes(b *testing.B) {
	val := make([]byte, 128)
	for i := range val {
		val[i] = 'x'
	}
	v := string(val)

	for _, size := range []int{1, 8, 32, 128, 512} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			ks := startBatchingNode(b, size, 2*time.Millisecond)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := ks.Set(fmt.Sprintf("k%d-%d", size, i), v); err != nil {
					b.Fatalf("Set: %v", err)
				}
			}
			b.StopTimer()
			st := ks.BatchStats()
			b.ReportMetric(st.Amortization, "cmds/fsync")
			b.ReportMetric(float64(st.Batches), "fsyncs")
		})
	}
}

// BenchmarkSetParallelBatch_Off 并发写，关闭批处理。
func BenchmarkSetParallelBatch_Off(b *testing.B) {
	benchParallelWrites(b, 1, 0)
}

// BenchmarkSetParallelBatch_On 并发写，开启批处理。
//
// 这才是批处理真正发挥作用的场景：并发请求自然聚成批，
// 不需要靠 BatchWait 去等。
func BenchmarkSetParallelBatch_On(b *testing.B) {
	benchParallelWrites(b, 128, 1*time.Millisecond)
}

func benchParallelWrites(b *testing.B, batchSize int, wait time.Duration) {
	ks := startBatchingNode(b, batchSize, wait)
	val := make([]byte, 128)
	for i := range val {
		val[i] = 'x'
	}
	v := string(val)

	var ctr atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := ctr.Add(1)
		i := 0
		for pb.Next() {
			if err := ks.Set(fmt.Sprintf("p%d-%d", id, i), v); err != nil {
				b.Errorf("Set: %v", err)
				return
			}
			i++
		}
	})
	b.StopTimer()
	st := ks.BatchStats()
	b.ReportMetric(st.Amortization, "cmds/fsync")
}

// ---------------------------------------------------------------------------
// 正确性
// ---------------------------------------------------------------------------

// TestBatchRoundTrip 验证批处理下所有键都被正确写入。
//
// 注意这里的 n 必须能被 workers 整除：早先版本的 n=500, workers=8
// 会让每个 worker 只跑 62 次（整数除法），实际总数是 496 而不是 500，
// 表现为"测试报丢数据"，实为测试自身算错。now 取 512 以避免这个陷阱。
func TestBatchRoundTrip(t *testing.T) {
	ks := startBatchingNode(t, 32, time.Millisecond)

	const workers = 8
	const perWorker = 64
	const total = workers * perWorker

	var wg sync.WaitGroup
	var okCount atomic.Int64
	errs := make(chan error, total)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("w%d-k%d", w, i)
				if err := ks.Set(key, fmt.Sprintf("v%d-%d", w, i)); err != nil {
					errs <- fmt.Errorf("Set(%s): %w", key, err)
					return
				}
				okCount.Add(1)
			}
		}(w)
	}
	wg.Wait()
	close(errs)

	var firstErr error
	errCount := 0
	for err := range errs {
		if firstErr == nil {
			firstErr = err
		}
		errCount++
	}
	if errCount > 0 {
		t.Fatalf("%d 次 Set 失败，第一个错误: %v", errCount, firstErr)
	}

	st := ks.BatchStats()
	t.Logf("Set 成功 %d 次；键数 %d；FSM applied=%d cmds=%d；enqueued=%d flushed=%d released=%d",
		okCount.Load(), ks.Len(), st.FSMApplied, st.FSMCommands, st.Enqueued, st.Flushed, st.Released)

	if int(okCount.Load()) != total {
		t.Fatalf("成功 Set %d 次，期望 %d（说明有 worker 提前退出）", okCount.Load(), total)
	}

	// 逐个键核对内容，而不是只看数量。
	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			key := fmt.Sprintf("w%d-k%d", w, i)
			want := fmt.Sprintf("v%d-%d", w, i)
			got, ok := ks.Get(key)
			if !ok {
				t.Fatalf("缺失: %s", key)
			}
			if got != want {
				t.Fatalf("%s = %q，期望 %q", key, got, want)
			}
		}
	}
	if got := ks.Len(); got != total {
		t.Fatalf("键数 = %d，期望 %d", got, total)
	}
}

// TestBatchDeleteInBatch 验证批内混合 set/delete 的顺序语义。
func TestBatchDeleteInBatch(t *testing.T) {
	ks := startBatchingNode(t, 64, 2*time.Millisecond)

	// 先写入。
	for i := 0; i < 100; i++ {
		if err := ks.Set(fmt.Sprintf("k%03d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	// 并发地删一半。
	var wg sync.WaitGroup
	for i := 0; i < 100; i += 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := ks.Delete(fmt.Sprintf("k%03d", i)); err != nil {
				t.Errorf("Delete: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if got := ks.Len(); got != 50 {
		t.Fatalf("键数 = %d，期望 50", got)
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("k%03d", i)
		_, ok := ks.Get(key)
		wantPresent := i%2 == 1
		if ok != wantPresent {
			t.Errorf("%s 存在=%v，期望 %v", key, ok, wantPresent)
		}
	}
}

// TestBatchAmortization 验证批处理确实在摊薄 fsync。
//
// 这是写路径优化的直接证据：Commands/Batches 就是平均每批命令数。
func TestBatchAmortization(t *testing.T) {
	ks := startBatchingNode(t, 64, 2*time.Millisecond)

	const workers, each = 16, 40
	var wg sync.WaitGroup
	var errCount atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := ks.Set(fmt.Sprintf("w%d-%d", w, i), "v"); err != nil {
					errCount.Add(1)
					t.Errorf("Set(w%d-%d): %v", w, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	st := ks.BatchStats()
	total := workers * each
	t.Logf("期望 %d 条；FSM applied=%d cmds=%d；批统计 enqueued=%d flushed=%d "+
		"direct(超时)=%d direct(退化)=%d released=%d batches=%d commands=%d 摊薄=%.2f",
		total, st.FSMApplied, st.FSMCommands, st.Enqueued, st.Flushed,
		st.DirectTimeout, st.DirectDegraded, st.Released, st.Batches, st.Commands, st.Amortization)
	if n := errCount.Load(); n > 0 {
		t.Fatalf("%d 个 Set 返回了错误", n)
	}

	// 分类账恒等式：每条命令都要有去处。
	//
	// ⚠️ 这里原本写的是 `Enqueued + Direct == FSMCommands + Released`，
	// 在 BatchMaxWait=2ms 的配置下**时通时不通**（实测 5 次运行 3 次失败），
	// 看起来像偶发竞态，实际是恒等式漏掉了超时路径：
	//
	//	Enqueued       = Flushed + DirectTimeout + Released
	//	FSMCommands    = Flushed + DirectTimeout + DirectDegraded   （无失败时）
	//
	// DirectTimeout 的那批命令**既**入过队、**又**走了单条提案，
	// 所以把它加在 Enqueued 那一侧就会重复计数。只有从未入队的
	// DirectDegraded 才该出现在那一侧。
	if st.Enqueued+st.DirectDegraded != int64(st.FSMCommands)+st.Released {
		t.Fatalf("命令去向对不上：入队 %d + 未入队单条 %d != FSM 应用 %d + 释放 %d",
			st.Enqueued, st.DirectDegraded, st.FSMCommands, st.Released)
	}
	if st.Released != 0 {
		t.Fatalf("有 %d 条命令被释放（不应发生）", st.Released)
	}
	// 入队的命令要么经批提交，要么超时后自己单条提交。
	if st.Flushed+st.DirectTimeout != st.Enqueued-st.Released {
		t.Fatalf("入队 %d 条：经批 %d + 超时单条 %d + 释放 %d ≠ 入队总数",
			st.Enqueued, st.Flushed, st.DirectTimeout, st.Released)
	}
	// 经批提交 + 两条单条路径必须覆盖全部命令。
	if st.Commands+st.Direct != int64(total) {
		t.Fatalf("经批 %d + 单条 %d = %d，期望 %d",
			st.Commands, st.Direct, st.Commands+st.Direct, total)
	}
	// FSM 命令数必须等于总数 —— 验证批内解码没有丢命令。
	if st.FSMCommands != uint64(total) {
		t.Fatalf("FSM 应用命令数 = %d，期望 %d", st.FSMCommands, total)
	}
	if got := ks.Len(); got != total {
		t.Fatalf("键数 = %d，期望 %d", got, total)
	}

	t.Logf("批次数(fsync) = %d, 命令数 = %d, 摊薄 = %.2f 命令/fsync, "+
		"FSM applied=%d cmds=%d",
		st.Batches, st.Commands, st.Amortization, st.FSMApplied, st.FSMCommands)

	// 诊断恒等式（同上，第二次核对，防止前面某条 Fatalf 被改坏）。
	t.Logf("诊断: 入队=%d 经批提交=%d 超时单条=%d 未入队单条=%d 释放=%d",
		st.Enqueued, st.Flushed, st.DirectTimeout, st.DirectDegraded, st.Released)
	if st.Enqueued+st.DirectDegraded != int64(st.FSMCommands)+st.Released {
		t.Fatalf("命令去向对不上：入队 %d + 未入队单条 %d != FSM 应用 %d + 释放 %d",
			st.Enqueued, st.DirectDegraded, st.FSMCommands, st.Released)
	}
	if st.Flushed+st.DirectTimeout != st.Enqueued-st.Released {
		t.Fatalf("入队 %d 条：经批 %d + 超时单条 %d ≠ %d（释放应为 0）",
			st.Enqueued, st.Flushed, st.DirectTimeout, st.Enqueued-st.Released)
	}

	// 16 个并发 worker 下至少应该摊到 2 条以上；否则批处理没生效。
	if st.Amortization < 1.5 {
		t.Fatalf("摊薄倍数只有 %.2f，批处理可能未生效（批次数 %d）", st.Amortization, st.Batches)
	}
	// FSM 应用的命令数必须等于总命令数 —— 验证批内解码没有丢命令。
	if st.FSMCommands != uint64(total) {
		t.Fatalf("FSM 应用命令数 = %d，期望 %d", st.FSMCommands, total)
	}
}

// TestBatchDisabledFallsBack 验证 BatchSize=1 时退化为单条提案。
func TestBatchDisabledFallsBack(t *testing.T) {
	ks := startBatchingNode(t, 1, 0)

	for i := 0; i < 50; i++ {
		if err := ks.Set(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if got := ks.Len(); got != 50 {
		t.Fatalf("键数 = %d，期望 50", got)
	}
	st := ks.BatchStats()
	if st.Batches != 0 {
		t.Fatalf("批处理已关闭，不应有批次记录，得到 %d", st.Batches)
	}
	// 关闭批处理时，每条命令一个日志条目。
	if st.FSMApplied != 50 {
		t.Fatalf("FSM 条目数 = %d，期望 50", st.FSMApplied)
	}
}

// TestBatchEncodeDecodeRoundTrip 编解码往返。
func TestBatchEncodeDecodeRoundTrip(t *testing.T) {
	cases := [][]Command{
		{{Op: opSet, Key: "a", Value: "1"}},
		{{Op: opSet, Key: "a", Value: "1"}, {Op: opDelete, Key: "b"}},
		{{Op: opSet, Key: "", Value: ""}},
		{{Op: opSet, Key: "键", Value: "值"}, {Op: opSet, Key: "k/2", Value: "v"}},
	}
	for i, want := range cases {
		enc, err := encodeBatch(want)
		if err != nil {
			t.Fatalf("case %d encode: %v", i, err)
		}
		got, err := decodeBatch(enc)
		if err != nil {
			t.Fatalf("case %d decode: %v", i, err)
		}
		if len(got) != len(want) {
			t.Fatalf("case %d: 得到 %d 条，期望 %d 条", i, len(got), len(want))
		}
		for j := range want {
			if got[j] != want[j] {
				t.Errorf("case %d item %d: 得到 %+v，期望 %+v", i, j, got[j], want[j])
			}
		}
	}
}

// TestBatchDecodeRejectsGarbage 畸形输入必须被拒绝，不得 panic。
func TestBatchDecodeRejectsGarbage(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{1},
		{0xff, 0xff, 0xff, 0xff},                   // count 巨大但无数据
		{0, 0, 0, 1},                                // count=1 但无内容
		{0, 0, 0, 1, byte(opSet), 0, 0, 0, 9},       // klen=9 但无 key
	}
	for i, c := range cases {
		if _, err := decodeBatch(c); err == nil {
			t.Errorf("case %d: 期望解码失败，却成功了", i)
		}
	}
}

// TestBatchEmptyDecodesToNothing 空批（0 条命令）应合法解码为空。
func TestBatchEmptyDecodesToNothing(t *testing.T) {
	enc, err := encodeBatch(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBatch(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("得到 %d 条，期望 0 条", len(got))
	}
}

// TestBatchSurvivesLeadershipChurn 验证换主过程中批队列的等待者不会永久阻塞。
func TestBatchSurvivesLeadershipChurn(t *testing.T) {
	base := t.TempDir()

	mk := func(id string) *KVStore {
		ks, err := NewKVStore(Config{
			NodeID:    id,
			BindAddr:  "127.0.0.1:0",
			RaftDir:   filepath.Join(base, id),
			BatchSize: 32,
			BatchWait: 2 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewKVStore(%s): %v", id, err)
		}
		t.Cleanup(func() { _ = ks.Shutdown() })
		return ks
	}

	n1 := mk("c1")
	n2 := mk("c2")
	n3 := mk("c3")

	if _, err := n1.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	if _, err := n1.WaitForLeader(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n1.AddVoter("c2", n2.AdvertiseAddr()); err != nil {
		t.Fatal(err)
	}
	if err := n1.AddVoter("c3", n3.AdvertiseAddr()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)

	// 找到 leader 并杀掉它；同时在写的 goroutine 必须能在有限时间内收到错误，
	// 而不是永久挂在 submit 的等待上。
	var leader *KVStore
	for _, n := range []*KVStore{n1, n2, n3} {
		if n.IsLeader() {
			leader = n
		}
	}
	if leader == nil {
		t.Fatal("无 leader")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			// 错误是预期的；这里只要求它**返回**。
			_ = leader.Set(fmt.Sprintf("k%d", i), "v")
		}
	}()

	time.Sleep(30 * time.Millisecond)
	if err := leader.Shutdown(); err != nil {
		t.Fatalf("shutdown leader: %v", err)
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("leader 关闭后写入 goroutine 永久阻塞 —— 批队列的等待者没有被释放")
	}
}
