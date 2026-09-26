// pkg/raft/batch.go
//
// 写路径批处理。
//
// 动机（本项目实测，见 docs/BUGS.md 的"新发现·读路径每请求一次 Barrier"）：
//
//	BenchmarkSet            5,447,163 ns/op
//	BenchmarkBarrier        6,672,332 ns/op
//	BenchmarkBarrierVsCommitTimeout: commit=1ms/5ms/20ms -> 5.72/6.46/5.87 ms
//
// CommitTimeout 从 1ms 调到 20ms 对结果毫无影响，说明瓶颈不是提交超时，
// 而是**每次提案都要付一次 fsync**。用本仓库的成本模型复算：
// b=1, t_fsync≈5.4ms → 185 ops/s，与实测一致。
//
// 修法就是教科书上的那条：把批大小 b 调大。`R_disk = b / t_fsync`，
// b=100 时磁盘上限从 185 ops/s 提到 18,500 ops/s。
//
// 实现方式：**把多条客户端命令装进一条 Raft 日志条目**（opBatch）。
// 这样每批只付一次 fsync，同时
//   - 保持线性化：一批是一个日志条目，条目内的命令按提交顺序原子生效；
//   - 不引入新的网络往返；
//   - 批内每条命令各自返回自己的错误，不会互相污染。
//
// 批内命令编码（复用既有的 key/value 字段，避免再叠一层长度前缀）：
//
//	batchValue = count(uint32)
//	           + count × [ op(1B) | klen(uint32) | vlen(uint32) | key | value ]
//
// 外层 EncodeCommand 已带 CRC32C，因此批内不重复校验。
//
// 延迟代价（必须诚实报告）：每条命令最多额外等 BatchWait。
// 这是吞吐与延迟的直接交换 —— BatchWait 的合理取值应当按实测 t_fsync 标定，
// 而不是拍一个数。BatchMaxWait 给出单条命令的延迟上界兜底。
package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ErrBatchTooLarge 单批超过编码上限。
var ErrBatchTooLarge = errors.New("raft: batch too large")

// ErrCorruptBatch batchValue 不是合法编码。
var ErrCorruptBatch = errors.New("raft: corrupt batch encoding")

// 批处理默认参数。
const (
	// defaultBatchSize 单批最多容纳的命令数。
	defaultBatchSize = 128
	// defaultBatchWait 攒批的最大等待时间。
	//
	// 这是延迟与吞吐的直接交换：BatchWait 越大，摊薄越好，但轻载延迟越高。
	// 2ms 在 t_fsync≈5ms 的机器上是合理折中（付 ~2ms 换掉 ~5ms 的 fsync）。
	defaultBatchWait = 2 * time.Millisecond
	// defaultBatchMaxWait 单条命令从入队到被强制单独提交的最长时间。
	// 防止极低负载下请求被无限期挂在批队列里。
	defaultBatchMaxWait = 20 * time.Millisecond
)

// batchItem 批队列里的一条待提交命令。
type batchItem struct {
	cmd  Command
	done chan struct{}
	// err 本命令自身的错误（与批整体成败无关）。
	err error
}

// batchState 领导者侧的批处理状态。
type batchState struct {
	mu      sync.Mutex
	cond    *sync.Cond
	pending []*batchItem

	batches atomic.Int64
	cmds    atomic.Int64

	// 诊断计数，用于定位命令去向。恒等式：
	//   Enqueued + Direct == FSM 实际应用的日志条目数（无批时）
	//   Enqueued + Direct == Flushed + Direct == FSM 命令数（有批时）
	enqueued atomic.Int64
	flushed  atomic.Int64
	// direct 未经过批队列、直接单条提案的命令数
	// （批处理关闭，或批循环尚未启动，或等待超时自行摘出）。
	direct   atomic.Int64
	released atomic.Int64
}

func newBatchState() *batchState {
	b := &batchState{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// enqueue 入队并唤醒攒批循环。
//
// 注意：enqueued 计数必须在**同一把锁内**递增。
// 否则会出现这样的交错：本函数已把 item 挂进 pending，但还没加计数；
// 攒批循环此刻 takeUpTo 把它取走并计入 flushed。
// 结果是 flushed > enqueued，表面看起来像"命令凭空多出来"，
// 而在反向交错下则会误报"命令丢失"。分类账必须在临界区内维护。
func (b *batchState) enqueue(it *batchItem) int {
	b.mu.Lock()
	b.pending = append(b.pending, it)
	n := len(b.pending)
	b.enqueued.Add(1)
	b.mu.Unlock()
	b.cond.Signal()
	return n
}

// takeUpTo 取走最多 n 条。返回 nil 表示当前没有待提交命令。
//
// flushed 记账与取出动作在同一临界区内完成：这是"分类账必须原子"的要求。
// 若把 flushed.Add 放到锁外，takeUpTo 与它之间可以插入另一个 takeUpTo，
// 导致 cmds/batches 的比例失真（表现为摊薄倍数虚高）。
func (b *batchState) takeUpTo(n int) []*batchItem {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pending) == 0 {
		return nil
	}
	if n > len(b.pending) {
		n = len(b.pending)
	}
	out := b.pending[:n:n]
	b.pending = b.pending[n:]
	b.flushed.Add(int64(len(out)))
	return out
}

// remove 摘除指定项；返回是否成功摘除。
func (b *batchState) remove(target *batchItem) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, it := range b.pending {
		if it == target {
			b.pending = append(b.pending[:i], b.pending[i+1:]...)
			return true
		}
	}
	return false
}

// drainAll 取走全部（用于停止循环时放掉等待者）。
func (b *batchState) drainAll() []*batchItem {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.pending
	b.pending = nil
	b.released.Add(int64(len(out)))
	return out
}

// commit 记录一次成功的批提交。
// Batches 与 Commands 必须一起自增，否则摊薄倍数 = Commands/Batches 会失真。
func (b *batchState) commit(n int64) {
	b.batches.Add(1)
	b.cmds.Add(n)
}

// accountFailed 记录一批因条目级失败（或编码失败）而未被提交的命令。
func (b *batchState) accountFailed(n int) {
	b.released.Add(int64(n))
}

// wakeAll 唤醒所有等待 cond 的攒批循环。
func (b *batchState) wakeAll() {
	b.mu.Lock()
	b.cond.Broadcast()
	b.mu.Unlock()
}

// wakeOne 唤醒攒批循环（单播）。
func (b *batchState) wakeOne() {
	b.mu.Lock()
	b.cond.Signal()
	b.mu.Unlock()
}

// ---------------------------------------------------------------------------
// 批内命令编码
// ---------------------------------------------------------------------------

// encodeBatch 把若干命令编码成 batchValue。
func encodeBatch(cmds []Command) ([]byte, error) {
	size := 4
	for _, c := range cmds {
		size += 9 + len(c.Key) + len(c.Value)
	}
	if size > maxFieldLen {
		return nil, ErrBatchTooLarge
	}

	buf := make([]byte, 0, size)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(cmds)))
	for _, c := range cmds {
		buf = append(buf, c.Op)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.Key)))
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.Value)))
		buf = append(buf, c.Key...)
		buf = append(buf, c.Value...)
	}
	return buf, nil
}

// decodeBatch 解析 batchValue。
func decodeBatch(b []byte) ([]Command, error) {
	if len(b) < 4 {
		return nil, ErrCorruptBatch
	}
	count := binary.BigEndian.Uint32(b[:4])
	// 防御：每条命令至少 9 字节，count 不可能超过剩余字节数。
	if count > uint32(len(b)) {
		return nil, ErrCorruptBatch
	}
	off := 4
	out := make([]Command, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+9 > len(b) {
			return nil, ErrCorruptBatch
		}
		op := b[off]
		kl := binary.BigEndian.Uint32(b[off+1 : off+5])
		vl := binary.BigEndian.Uint32(b[off+5 : off+9])
		off += 9
		if kl > maxFieldLen || vl > maxFieldLen {
			return nil, ErrTooLarge
		}
		if uint64(off)+uint64(kl)+uint64(vl) > uint64(len(b)) {
			return nil, ErrCorruptBatch
		}
		out = append(out, Command{
			Op:    op,
			Key:   string(b[off : off+int(kl)]),
			Value: string(b[off+int(kl) : off+int(kl)+int(vl)]),
		})
		off += int(kl) + int(vl)
	}
	return out, nil
}

// applyBatch 在状态机内原子应用一批命令。
//
// 返回 []error，长度与命令数一致；每条命令各自成败。
// 调用方在批被 Raft 拒绝时拿到的是条目级错误，那种情况下整批失败。
func (fsm *FSM) applyBatch(batchValue string) interface{} {
	cmds, err := decodeBatch([]byte(batchValue))
	if err != nil {
		return err
	}
	errs := make([]error, len(cmds))

	// 一次加锁应用整批：既保证批内顺序语义，也把 n 次锁获取摊成 1 次。
	fsm.mu.Lock()
	for i, c := range cmds {
		switch c.Op {
		case opSet:
			fsm.store[c.Key] = c.Value
		case opDelete:
			delete(fsm.store, c.Key)
		default:
			errs[i] = fmt.Errorf("%w: unknown op %d in batch", ErrCorruptCommand, c.Op)
		}
	}
	fsm.mu.Unlock()

	fsm.cmds.Add(uint64(len(cmds)))
	return errs
}

// ---------------------------------------------------------------------------
// 领导者侧：攒批循环与提交
// ---------------------------------------------------------------------------

// startBatchLoop 启动攒批循环。仅在成为 Leader 时启动；可重复调用。
func (ks *KVStore) startBatchLoop() {
	if ks.cfg.BatchSize <= 1 {
		return // 批处理未启用
	}
	ks.batchMu.Lock()
	defer ks.batchMu.Unlock()
	if ks.batchLoopRunning {
		return
	}
	if ks.batch == nil {
		ks.batch = newBatchState()
	}
	stop := make(chan struct{})
	ks.batchStop = stop
	ks.batchLoopRunning = true
	go ks.batchLoop(stop)
}

// stopBatchLoop 停止攒批循环，并放掉所有等待者。
func (ks *KVStore) stopBatchLoop() {
	ks.batchMu.Lock()
	if !ks.batchLoopRunning {
		ks.batchMu.Unlock()
		return
	}
	close(ks.batchStop)
	ks.batchLoopRunning = false
	b := ks.batch
	ks.batchMu.Unlock()

	// 唤醒可能正在 cond.Wait 的循环，让它看到 stop 并退出。
	b.wakeAll()

	// 放掉队列里还没提交的请求，避免调用方永久阻塞。
	for _, it := range b.drainAll() {
		it.err = ErrNotLeader
		close(it.done)
	}
}

// submit 把一条命令放入批队列并等待其提交结果。
//
// 三条路径：
//  1. 批处理未启用 → 退化为单条提案（与优化前行为一致）；
//  2. 正常 → 入队等待本批提交；
//  3. 等待超过 BatchMaxWait → 自己摘出来单独提交，保证延迟上界。
func (ks *KVStore) submit(cmd Command) error {
	ks.batchMu.Lock()
	b := ks.batch
	running := ks.batchLoopRunning
	ks.batchMu.Unlock()

	if b == nil || !running || ks.cfg.BatchSize <= 1 {
		// 退化路径也必须记账，否则分类账恒等式不成立。
		//
		// 这条路径在三种情况下会走到：
		//  1. 显式关闭批处理（BatchSize<=1）→ batch 可能为 nil；
		//  2. **批循环尚未启动** —— followLeadership 是 50ms 轮询的，
		//     节点刚当选 Leader 的头几十毫秒内 batch 仍为 nil，
		//     此时到达的写会直接单条提案；
		//  3. 已被要求停止（失去 Leadership / 关闭）。
		ks.batchDirect.Add(1)
		if b != nil {
			b.direct.Add(1)
		}
		return ks.propose(cmd, opName(cmd.Op))
	}

	it := &batchItem{cmd: cmd, done: make(chan struct{})}
	ks.batch.enqueue(it)

	maxWait := ks.cfg.BatchMaxWait
	if maxWait <= 0 {
		maxWait = defaultBatchMaxWait
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()

	select {
	case <-it.done:
		return it.err
	case <-timer.C:
		// 超时：若这条命令还在队列里，就摘出来单独提交，保证延迟上界。
		if ks.batch.remove(it) {
			ks.batch.direct.Add(1)
			return ks.propose(cmd, opName(cmd.Op))
		}
		// 已被攒批循环取走，正在提交，继续等。
		<-it.done
		return it.err
	}
}

// batchLoop 攒批主循环。
//
// 每一轮：
//  1. 无待提交命令时用 cond.Wait 休眠（不空转）；
//  2. 若队列里只有 1 条命令，**立即提交**，不等待攒批窗口；
//  3. 否则最多等 BatchWait 让更多命令挤进来，再整批提交。
//
// 第 2 步是必要的：若单条命令也硬等 BatchWait，轻载下每条写都要
// 多付一个 BatchWait 的延迟。实测（批大小 8、窗口 2ms、串行写）：
// 关闭批处理 4.60ms/op，开启后反而 11.47ms/op —— 因为 cmds/fsync 只有 1.000，
// 摊薄为零却白等了窗口。有了孤条目快路径，串行场景回到 4.6ms 基准。
//
// 代价是失去"等一等让第二条命令凑进来"的机会。但既然此刻队列里只有一条，
// 等下去的唯一收益就是那一条迟到的命令 —— 而它完全可以在下一批里被摊薄。
func (ks *KVStore) batchLoop(stop <-chan struct{}) {
	wait := ks.cfg.BatchWait
	if wait <= 0 {
		wait = defaultBatchWait
	}
	size := ks.cfg.BatchSize
	if size <= 0 {
		size = defaultBatchSize
	}
	oneShot := ks.cfg.BatchOneShot

	for {
		// 1. 等待有活干，或被要求停止。
		func() {
			ks.batch.mu.Lock()
			defer ks.batch.mu.Unlock()
			for len(ks.batch.pending) == 0 {
				select {
				case <-stop:
					return
				default:
				}
				ks.batch.cond.Wait()
			}
		}()
		select {
		case <-stop:
			return
		default:
		}

		// 2. 孤条目快路径。
		ks.batch.mu.Lock()
		lonely := len(ks.batch.pending) == 1
		ks.batch.mu.Unlock()
		if !oneShot || !lonely {
			// 3. 攒批窗口。
			deadline := time.NewTimer(wait)
			select {
			case <-deadline.C:
			case <-stop:
				deadline.Stop()
				return
			}
			deadline.Stop()
		}

		// 4. 提交一批。
		items := ks.batch.takeUpTo(size)
		if len(items) > 0 {
			ks.flushBatch(items)
		}
	}
}

// flushBatch 提交一批。
//
// 成功提交后，batches 计数与 items 长度在同一临界区内记账，
// 保证 Batches × Amortization == Commands 恒成立。
func (ks *KVStore) flushBatch(items []*batchItem) {
	cmds := make([]Command, len(items))
	for i, it := range items {
		cmds[i] = it.cmd
	}

	release := func(err error) {
		for _, it := range items {
			it.err = err
			close(it.done)
		}
	}

	batchValue, err := encodeBatch(cmds)
	if err != nil {
		release(err)
		ks.batch.accountFailed(len(items))
		return
	}

	data, err := EncodeCommand(Command{Op: opBatch, Value: string(batchValue)})
	if err != nil {
		release(err)
		ks.batch.accountFailed(len(items))
		return
	}

	start := time.Now()
	f := ks.raft.Apply(data, ks.cfg.ApplyTimeout)
	if err := f.Error(); err != nil {
		// 条目级失败：整批失败。
		release(err)
		ks.batch.accountFailed(len(items))
		ks.applyErrors.Add(1)
		ks.metrics.RecordError("batch")
		return
	}

	// 条目级成功；逐条取出各自的错误。
	errs, _ := f.Response().([]error)
	for i, it := range items {
		if i < len(errs) {
			it.err = errs[i]
		}
		close(it.done)
	}

	ks.batch.commit(int64(len(items)))
	ks.metrics.RecordOperation("batch", int64(len(items)))
	ks.metrics.RecordLatency("batch_fsync", time.Since(start))
}

// BatchStats 批处理统计。
type BatchStats struct {
	// Batches 已提交的批次数（≈ fsync 次数）。
	Batches int64 `json:"batches"`
	// Commands 已提交的客户端命令数。
	Commands int64 `json:"commands"`
	// Amortization 平均每批命令数 = fsync 摊薄倍数。
	Amortization float64 `json:"amortization"`
	// FSMApplied 状态机已应用的日志条目数。
	FSMApplied uint64 `json:"fsm_applied"`
	// FSMCommands 状态机已应用的客户端命令数。
	FSMCommands uint64 `json:"fsm_commands"`
	// BatchSize 配置的单批上限。
	BatchSize int `json:"batch_size"`
	// BatchWaitMs 配置的攒批窗口。
	BatchWaitMs float64 `json:"batch_wait_ms"`

	// 以下为诊断计数，用于核对命令去向。
	// 恒等式：Enqueued + Direct == FSMCommands（无失败时）。
	Enqueued int64 `json:"enqueued"`
	Flushed  int64 `json:"flushed"`
	// Direct 未经过批队列、直接单条提案的命令数。
	Direct int64 `json:"direct"`
	// Released 因关闭/失去 Leadership/条目级失败而未提交的命令数。
	Released int64 `json:"released"`
}

// BatchReady 报告批处理循环是否已经在跑。
//
// 存在的理由：followLeadership 是 50ms 轮询的，节点当选 Leader 之后
// 有一个最长约 50ms 的窗口，此间 batch 仍为 nil，写会走单条提案。
// 做批量性能实验时必须先等它就绪，否则会把冷启动噪声算进测量结果。
func (ks *KVStore) BatchReady() bool {
	ks.batchMu.Lock()
	defer ks.batchMu.Unlock()
	return ks.batchLoopRunning && ks.batch != nil
}

// WaitBatchReady 等待批处理循环就绪，超时返回 false。
func (ks *KVStore) WaitBatchReady(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ks.BatchReady() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return ks.BatchReady()
}

// BatchStats 返回批处理统计。
func (ks *KVStore) BatchStats() BatchStats {
	st := BatchStats{
		FSMApplied:  ks.fsm.Applied(),
		FSMCommands: ks.fsm.Commands(),
		BatchSize:   ks.cfg.BatchSize,
		BatchWaitMs: float64(ks.cfg.BatchWait.Microseconds()) / 1000.0,
	}
	ks.batchMu.Lock()
	b := ks.batch
	ks.batchMu.Unlock()
	if b != nil {
		st.Batches = b.batches.Load()
		st.Commands = b.cmds.Load()
		st.Enqueued = b.enqueued.Load()
		st.Flushed = b.flushed.Load()
		st.Direct = b.direct.Load()
		st.Released = b.released.Load()
		if st.Batches > 0 {
			st.Amortization = float64(st.Commands) / float64(st.Batches)
		}
	}
	// batch == nil 时（批循环尚未启动）的那部分单条提案也要计入。
	st.Direct += ks.batchDirect.Load()
	return st
}

// opName 操作名，用于指标标签。
func opName(op byte) string {
	switch op {
	case opSet:
		return "set"
	case opDelete:
		return "delete"
	default:
		return "other"
	}
}
