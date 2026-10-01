// cmd/raftbench/net.go
//
// 在不改动 fork 的前提下，给 in-memory 传输注入"网络"。
//
// ── 为什么需要它 ────────────────────────────────────────────────────
// InmemTransport 的 RPC 是进程内 channel 传递，一次 AppendEntries 往返
// 只要几微秒。在这样的介质上量"少等一个副本"（FlexiRaft 的 |Q2| 变小）
// 几乎看不出差别 —— 不是机制不成立，而是被测量的介质里根本没有延迟。
// 要把共识层的 quorum 几何效应测出来，介质必须具备真实网络的两个特征：
// 单程延迟，以及延迟的抖动。
//
// ── 注入点：发送侧 ──────────────────────────────────────────────────
// InmemTransport 内部是 `peer.consumerCh <- rpc` 直接写对端的私有 channel，
// 且 Connect() 会做 `t.(*InmemTransport)` 类型断言 —— 所以接收侧包不住，
// 只能在**发送侧**包。三条路径都覆盖：
//
//	AppendEntries         同步复制路径（新 follower 追赶、心跳）
//	AppendEntriesPipeline 稳态复制路径（replication.go 的 pipelineSend）
//	RequestVote / InstallSnapshot / TimeoutNow  选举与快照路径
//
// ── 关于 pipeline 的返回 future ─────────────────────────────────────
// pipelineSend（replication.go:511）写的是：
//
//	if _, err := p.AppendEntries(req, new(AppendEntriesResponse)); err != nil {
//
// 它**丢弃**了 future，响应全部从 pipelineDecode 读 p.Consumer()。
// 因此延迟可以放在独立 goroutine 里异步完成，同步返回 (nil, nil) 不会
// 丢信息 —— 只要 Consumer() 仍然转发底层管道的 future。这里显式依赖
// 了该行为；若将来 fork 升级导致 pipelineSend 开始使用返回值，本文件
// 会静默失效（响应仍在，只是错误传播丢失），故在此标注。
//
// ── 顺序性 ─────────────────────────────────────────────────────────
// 真实网络（TCP）保证按序交付。pipeline 路径上每个 RPC 由独立 goroutine
// 延迟后 enqueue，用 prev/mine 链保证 **enqueue 顺序** 与提交顺序一致；
// 同时多个 RPC 仍可同时在途 —— 这不是零 BDP 的串行链路。
//
// ── 纪律 ───────────────────────────────────────────────────────────
// 这是**注入**的延迟，不是实测的网络延迟。所有结果必须带上
// InjectedDelayUs / InjectedJitterSigma 字段，不得当作生产网络实测。
package main

import (
	"io"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	fraft "github.com/distributed-kv/kvstore/third_party/flexiraft"
)

// rpcCounters 各节点的 RPC 计数（原子）。
type rpcCounters struct {
	appendEntries atomic.Int64
	requestVote   atomic.Int64
	installSnap   atomic.Int64
	timeoutNow    atomic.Int64
	pipelineAE    atomic.Int64
}

// delayTransport 包装 *InmemTransport：计数 + 注入单程延迟。
//
// 注意 Connect/Disconnect/Consumer 等全部由内嵌的 *InmemTransport 提供，
// 且 **Connect 必须用原始 *InmemTransport**（它对参数做类型断言）。
type delayTransport struct {
	*fraft.InmemTransport

	counters *rpcCounters

	medianNs int64   // 注入延迟的中位数（纳秒）；<=0 表示不注入
	sigma    float64 // 对数正态形状参数；<=0 表示常量延迟

	mu  sync.Mutex
	rnd *rand.Rand
}

func newDelayTransport(addr fraft.ServerAddress, median time.Duration, sigma float64, seed int64, c *rpcCounters) (fraft.ServerAddress, *delayTransport) {
	realAddr, raw := fraft.NewInmemTransport(addr)
	return realAddr, &delayTransport{
		InmemTransport: raw,
		counters:       c,
		medianNs:       int64(median),
		sigma:          sigma,
		rnd:            rand.New(rand.NewSource(seed)),
	}
}

// sample 采一次延迟。延迟服从对数正态分布，**中位数**等于配置值：
//
//	delay = median * exp(sigma * z),  z ~ N(0,1)
//
// sigma=0 时退化为常量延迟 median。
// 用中位数而不是均值作配置值，是为了让参数含义稳定：
// 对数正态的均值 = median*exp(sigma²/2)，会随 sigma 漂移。
func (d *delayTransport) sample() time.Duration {
	if d.medianNs <= 0 {
		return 0
	}
	if d.sigma <= 0 {
		return time.Duration(d.medianNs)
	}
	d.mu.Lock()
	z := d.rnd.NormFloat64()
	d.mu.Unlock()
	v := float64(d.medianNs) * math.Exp(d.sigma*z)
	// 上限 2s：超过就没必要精确了，而且会让 RPC 超时满天飞。
	if v > 2e9 {
		v = 2e9
	}
	return time.Duration(v)
}

func (d *delayTransport) AppendEntries(id fraft.ServerID, target fraft.ServerAddress, args *fraft.AppendEntriesRequest, resp *fraft.AppendEntriesResponse) error {
	d.counters.appendEntries.Add(1)
	if wait := d.sample(); wait > 0 {
		time.Sleep(wait)
	}
	return d.InmemTransport.AppendEntries(id, target, args, resp)
}

func (d *delayTransport) RequestVote(id fraft.ServerID, target fraft.ServerAddress, args *fraft.RequestVoteRequest, resp *fraft.RequestVoteResponse) error {
	d.counters.requestVote.Add(1)
	if wait := d.sample(); wait > 0 {
		time.Sleep(wait)
	}
	return d.InmemTransport.RequestVote(id, target, args, resp)
}

func (d *delayTransport) InstallSnapshot(id fraft.ServerID, target fraft.ServerAddress, args *fraft.InstallSnapshotRequest, resp *fraft.InstallSnapshotResponse, data io.Reader) error {
	d.counters.installSnap.Add(1)
	if wait := d.sample(); wait > 0 {
		time.Sleep(wait)
	}
	return d.InmemTransport.InstallSnapshot(id, target, args, resp, data)
}

func (d *delayTransport) TimeoutNow(id fraft.ServerID, target fraft.ServerAddress, args *fraft.TimeoutNowRequest, resp *fraft.TimeoutNowResponse) error {
	d.counters.timeoutNow.Add(1)
	if wait := d.sample(); wait > 0 {
		time.Sleep(wait)
	}
	return d.InmemTransport.TimeoutNow(id, target, args, resp)
}

func (d *delayTransport) AppendEntriesPipeline(id fraft.ServerID, target fraft.ServerAddress) (fraft.AppendPipeline, error) {
	inner, err := d.InmemTransport.AppendEntriesPipeline(id, target)
	if err != nil {
		return nil, err
	}
	return &delayPipeline{
		inner:    inner,
		d:        d,
		inflight: make(chan struct{}, maxInflightPerPeer),
	}, nil
}

// delayPipeline 见文件头"关于 pipeline 的返回 future"。
type delayPipeline struct {
	inner fraft.AppendPipeline
	d     *delayTransport

	mu   sync.Mutex
	prev chan struct{} // 上一个 RPC 完成 enqueue 后关闭

	inflight chan struct{} // 在途上限，超出即在 AppendEntries 里阻塞（背压）
}

// maxInflightPerPeer 模拟链路的带宽-时延积（BDP）上限。
//
// 没有它，延迟 goroutine 会无界堆积：leader 不会因为 follower 没回包而
// 减速，队列越排越长，延迟发散。真实网络不会这样 —— 拥塞窗口会兜住。
const maxInflightPerPeer = 512

func (p *delayPipeline) AppendEntries(args *fraft.AppendEntriesRequest, resp *fraft.AppendEntriesResponse) (fraft.AppendFuture, error) {
	wait := p.d.sample()
	p.d.counters.pipelineAE.Add(1)

	// ── 延迟语义：release = 调用时刻 + delay，且不早于前一个 RPC 的 enqueue ──
	//
	//	enqueue_i = max(t_i + d_i, enqueue_{i-1})
	//
	// 这是 TCP 的模型：多个 RPC 可以同时在途（流水线），但按序交付。
	//
	// ⚠️ 早先的实现写成「先等前一个 enqueue，再 sleep(d)」，于是
	// enqueue_i = enqueue_{i-1} + d_i —— 延迟变成了**累加**，每个 follower
	// 的吞吐被钉死在 1/d，队列无界增长，实测 p50 从 2ms 爆到 169ms，
	// 连心跳都排不上队，leader 反复因 lease 失效退位。这个 bug 已修。
	if wait > 0 {
		p.inflight <- struct{}{}
	}
	release := time.Now().Add(wait)

	p.mu.Lock()
	prev := p.prev
	mine := make(chan struct{})
	p.prev = mine
	p.mu.Unlock()

	go func() {
		// 先等前一个 RPC 进入底层管道 —— 保证 enqueue 顺序 = 提交顺序。
		if prev != nil {
			<-prev
		}
		if rem := time.Until(release); rem > 0 {
			time.Sleep(rem)
		}
		// 错误无法同步返回（本函数已经返回了）。底层管道出错时
		// pipelineDecode 会因为超时/失败而重启复制，不影响正确性。
		_, _ = p.inner.AppendEntries(args, resp)
		close(mine)
		if wait > 0 {
			<-p.inflight
		}
	}()
	return nil, nil
}

func (p *delayPipeline) Consumer() <-chan fraft.AppendFuture { return p.inner.Consumer() }
func (p *delayPipeline) Close() error                        { return p.inner.Close() }
