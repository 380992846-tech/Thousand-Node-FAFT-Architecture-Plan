// cmd/raftbench/shards.go
//
// shards 模式：S 个分片 × n 副本，按故障域放置，注入**整域故障**。
//
// ── 为什么这是 FAFT 最该被测的一组 ─────────────────────────────────
// 前面所有模式都是**单分片**，测的是 quorum 几何对单组性能的影响。
// 但 FAFT 的核心主张不是性能，是这一句：
//
//	决定可达性的不是 quorum 的大小，而是它的组织结构。
//
// 这句话只有在**多分片 + 整域故障**下才检验得了：一个故障域挂掉，
// 同时影响到很多个分片的副本。单分片基准里根本没有这个现象。
//
// ── 关于"千卡" ─────────────────────────────────────────────────────
// 这里跑的不是千卡集群，是**千分片元数据面**：1000 分片 × 3-5 副本
// = 3000-5000 个 Raft 实例，全在同一个进程里。
//
// 对 FAFT 来说后者才是相关的规模维度 —— GPU 完全不参与共识，
// 千卡集群里真正压到元数据层的是分片数、故障域数、以及故障的**相关性**。
// 所以这个模式量的是：
//
//	分片数（规模）× 故障域数（结构）× quorum 几何（决策）→ 整域故障后的可用分片比例
//
// ── 这不是"千节点集群实测" ─────────────────────────────────────────
// 传输是进程内 in-memory，所有节点共享一个 Go 调度器与一块 CPU。
// 它测的是**结构与算术**，不是吞吐。可用性在这里由「存活副本数 ≥ |Q1|」
// 决定，而存活副本数由「副本怎么摊到域上」决定 —— 两者都是结构量，
// 恰好是进程内模拟能忠实复现的部分。带宽/延迟效应请走 bench/burst 模式。
package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	fraft "github.com/distributed-kv/kvstore/third_party/flexiraft"
)

type shardsOpts struct {
	label    string
	n        int // 每分片副本数
	shards   int // 分片数
	domains  int // 故障域数
	q1, q2   int
	delay    time.Duration
	sigma    float64
	seed     int64
	payload  int
	mae      int
	kill     int           // 杀几个域
	steady   time.Duration // 杀之前先观察多久（量心跳开销）
	recover  time.Duration // 杀之后等多久再统计
	electT   time.Duration // 等所有分片选主的上限
	hbTO     time.Duration // HeartbeatTimeout（分片多时要放大，否则选举风暴）
	elecTO   time.Duration
	clockUs  int64
	gogc     int
	progress bool
	// maxInstances 覆盖 maxRaftInstances 上限（默认 0 = 用常量）。
	maxInstances int
	// pollEvery 恢复期轮询间隔（默认 100ms）。调小会显著增加
	// 「扫描全部实例」的开销：1000 分片 × 5 副本时每次扫描要读 5000 个原子状态。
	pollEvery time.Duration
	// settle 恢复统计的起始偏移（默认 2s）。用于剔除「原 leader 退位延迟」
	// 造成的假可用 —— 见第 4 步的长注释。
	settle time.Duration
}

type shardSet struct {
	sets     []*cluster
	domainOf [][]int // [shard][replica] = 域编号
	domains  int
	opt      shardsOpts
}

// maxRaftInstances 本机可承受的 Raft 实例总数上限（分片数 × 副本数）。
//
// 这个常量是**踩过坑之后加的**，依据是一条实测记录：
//
//	1000 分片 × 3 副本 = 3000 实例 → 单次 12-18 秒，连续多次稳定
//	1000 分片 × 5 副本 = 5000 实例 → 单次 40-60 秒，且在第 7 次连续运行时
//	                                   把整台机器打到失去响应
//
// 出事时物理内存还剩 22.8GB，所以**不是内存问题**：是大量 Raft 实例
// 在整域故障后同时进入选举风暴 —— 16 个逻辑核全被打满，上万个定时器
// 同时到期，操作系统层面失去调度余量。
//
// 结论：这不是"跑跑看"的参数，必须硬性拒绝。要看更大规模的结构性质，
// 降低分片数即可。
//
// ⚠️ **不要再说"比例对 400 与 1000 分片是同一个数"** —— 那句话被实测推翻了。
//
// 这里原来写着"本模式量的是比例，而比例对 400 与 1000 分片是同一个数（已实测对照）"。
// 查 results/ 才发现：只有 400 分片的文件，**1000 分片那一侧没有任何落盘证据** ——
// 也就是说那句话当时不可核对。补测（`-mode shards-scale`，同一结构
// n=3 / d=5 / |Q1|=|Q2|=2 / kill=1，注入延迟 0）之后：
//
//	S=400 （1200 实例）：100.00% / 100.00% / 99.25%   —— 稳，且等于算术界
//	S=1000（3000 实例）： 34.30% /  50.90% / 100.00%  —— 波动 3 倍，**不等于算术界**
//
// 原因不是 quorum 结构，而是**单机资源**：3000 个 Raft 实例共享 16 个逻辑核，
// CPU 饱和使选举 RPC 超时（日志里成片 "requestVote ... send timed out"），
// 候选者反复重来形成活锁，于是窗口末刻大量分片没有 leader。
// 上面那条"连续第 7 次运行把机器打到失去响应"，是同一现象的极端形态。
//
// 所以结论必须按规模说清楚：**算术界（存活数 ≥ |Q1|）是必要条件，不是充分条件**。
// 小规模（CPU 不饱和）上实测与界吻合；大规模上不吻合。
// 证据：results/shards-scale-400.json。
const maxRaftInstances = 3000

// buildShardSet 建 S 个**互相独立**的 Raft 组，但把副本映射到全局故障域。
//
// 每组有自己的 transport 网格与日志存储，互不可见；"域"在这里是一个
// 纯逻辑标签，只用于决定故障时杀谁。这正是我们要分离的变量：
// 共识本身不关心域，域的效应完全来自「同一时刻有多少副本一起消失」。
func buildShardSet(o shardsOpts) (*shardSet, error) {
	if o.domains < 1 {
		return nil, fmt.Errorf("domains 必须 >= 1")
	}
	if o.kill >= o.domains {
		return nil, fmt.Errorf("kill=%d 必须 < domains=%d（至少要留一个域活着）", o.kill, o.domains)
	}
	limit := o.maxInstances
	if limit <= 0 {
		limit = maxRaftInstances
	}
	if total := o.shards * o.n; total > limit {
		return nil, fmt.Errorf(
			"拒绝运行：%d 分片 × %d 副本 = %d 个 Raft 实例，超过本机上限 %d。\n"+
				"  这个上限是硬性的、有实测依据的：5000 实例在上位机连续运行时会把整台机器打到失去响应\n"+
				"  （不是内存问题 —— 出事时还剩 22.8GB；是选举风暴把 16 个核全打满）。\n"+
				"  要看更大规模，请降低分片数。\n"+
				"  ⚠️ 但**不要**以为「降低分片数会得到同一个比例」—— 实测 S=400（1200 实例）稳定等于算术界，\n"+
				"     而 S=1000（3000 实例）三轮是 34.3%%/50.9%%/100.0%%，波动 3 倍。\n"+
				"     比例本身与本机资源相关，见 results/shards-scale-400.json。\n"+
				"  确实要突破上限：显式传 -maxinstances。",
			o.shards, o.n, total, limit)
	}
	ss := &shardSet{domains: o.domains, opt: o}

	for s := 0; s < o.shards; s++ {
		bo := buildOpts{
			n: o.n, q1: o.q1, q2: o.q2,
			delay: o.delay, sigma: o.sigma,
			seed:    o.seed + int64(s)*104729,
			payload: o.payload,
			mae:     o.mae,
		}
		bo.hbTO = o.hbTO
		bo.elecTO = o.elecTO
		cl, err := buildCluster(bo)
		if err != nil {
			ss.shutdown()
			return nil, fmt.Errorf("第 %d 个分片建组失败: %w", s, err)
		}
		// 放置策略：副本 i 落在域 (s+i) % D —— 同一个分片的副本尽量摊开，
		// 不同分片的副本错开。D >= n 时每个分片占 n 个不同域；
		// D < n 时必然有副本同域（这正是我们要观测的脆弱情形）。
		dom := make([]int, o.n)
		for i := 0; i < o.n; i++ {
			dom[i] = (s + i) % o.domains
		}
		ss.sets = append(ss.sets, cl)
		ss.domainOf = append(ss.domainOf, dom)
	}
	return ss, nil
}

// shutdown 并发关闭所有分片。
//
// ⚠️ 必须并发。串行关闭 3000 个实例实测会卡住十分钟以上：
// 每个 Raft 的 Shutdown() 要等内部 goroutine 退完，而复制 goroutine
// 可能正卡在 `makeRPC` 的对端 channel 上 —— 对端已经关了、没人收，
// 只能等 InmemTransport 的超时（500ms）。串行就是 3000 × 500ms。
// 并发之后总耗时约等于单次超时。
func (ss *shardSet) shutdown() {
	var wg sync.WaitGroup
	for _, cl := range ss.sets {
		wg.Add(1)
		go func(c *cluster) {
			defer wg.Done()
			c.shutdown()
		}(cl)
	}
	wg.Wait()
}

// leadersReady 返回已选出 leader 的分片数。
//
// 刻意不用 cluster.Leader()：它内部走 pollState，等不到稳定 leader 会
// 直接 Failf（那是测试夹具的语义）。这里只需要一个瞬时读数。
func (ss *shardSet) leadersReady() int {
	ready := 0
	for _, cl := range ss.sets {
		for _, nd := range cl.nodes {
			if nd.raft != nil && nd.raft.State() == fraft.Leader {
				ready++
				break
			}
		}
	}
	return ready
}

// aliveDomainCount 统计某分片在给定存活域集合下还剩几个副本。
func (ss *shardSet) survivors(s int, dead map[int]bool) int {
	c := 0
	for _, d := range ss.domainOf[s] {
		if !dead[d] {
			c++
		}
	}
	return c
}

func runShards(o shardsOpts) (*result, error) {
	q1 := effectiveQuorum(o.n, o.q1)
	q2 := effectiveQuorum(o.n, o.q2)
	progress("[shards] %d 分片 × %d 副本（共 %d 个 Raft 实例），%d 个故障域，|Q1|=%d |Q2|=%d",
		o.shards, o.n, o.shards*o.n, o.domains, q1, q2)

	tBuild := time.Now()
	ss, err := buildShardSet(o)
	if err != nil {
		return nil, err
	}
	defer ss.shutdown()
	buildMs := float64(time.Since(tBuild).Microseconds()) / 1000.0
	progress("  建组耗时 %.0fms", buildMs)

	// ── 1. 等所有分片选出 leader ──
	tReady := time.Now()
	var readyMs float64 = -1
	deadline := time.Now().Add(o.electT)
	for {
		if ss.leadersReady() == o.shards {
			readyMs = float64(time.Since(tReady).Microseconds()) / 1000.0
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	readyCount := ss.leadersReady()
	progress("  %d/%d 个分片选出 leader（耗时 %.0fms）", readyCount, o.shards, readyMs)

	// ── 2. 稳态观察：心跳/RPC 开销 ──
	var beforeAE, beforePipe, beforeVote int64
	for _, cl := range ss.sets {
		beforeAE += cl.counters.appendEntries.Load()
		beforePipe += cl.counters.pipelineAE.Load()
		beforeVote += cl.counters.requestVote.Load()
	}
	time.Sleep(o.steady)
	var afterAE, afterPipe, afterVote int64
	for _, cl := range ss.sets {
		afterAE += cl.counters.appendEntries.Load()
		afterPipe += cl.counters.pipelineAE.Load()
		afterVote += cl.counters.requestVote.Load()
	}
	steadySec := o.steady.Seconds()
	hbPerSec := float64((afterAE-beforeAE)+(afterPipe-beforePipe)) / steadySec
	votePerSec := float64(afterVote-beforeVote) / steadySec
	progress("  稳态开销：心跳/复制 RPC %.0f 次/秒，RequestVote %.0f 次/秒", hbPerSec, votePerSec)

	// ── 3. 注入整域故障 ──
	// 选最小编号的 kill 个域，保证可复现（不随机）。
	dead := make(map[int]bool, o.kill)
	for d := 0; d < o.kill; d++ {
		dead[d] = true
	}
	killedNodes := 0
	tKill := time.Now()
	// 同样必须并发：见 shardSet.shutdown 的注释。
	{
		var vwg sync.WaitGroup
		var mu sync.Mutex
		for s, cl := range ss.sets {
			for i, nd := range cl.nodes {
				if !dead[ss.domainOf[s][i]] {
					continue
				}
				mu.Lock()
				killedNodes++
				mu.Unlock()
				vwg.Add(1)
				go func(nd *node) {
					defer vwg.Done()
					_ = nd.raft.Shutdown().Error()
					nd.raw.Close()
				}(nd)
			}
		}
		vwg.Wait()
	}
	killMs := float64(time.Since(tKill).Microseconds()) / 1000.0
	progress("  杀掉域 %v：共 %d 个副本下线（耗时 %.0fms）", domainList(dead), killedNodes, killMs)

	// ── 4. 等待并统计可用性 ──
	//
	// ⚠️ 两个坑，都必须在这里挡住，否则可用性数字会系统性偏高：
	//
	// 【坑一：单次快照】不能只在末尾取一次。那会把"此刻正在选举中"
	// 误判成"恢复不了"。所以要轮询。
	//
	// 【坑二：退位延迟污染】原 leader 被杀之后，**没被杀到的 shard 的
	// leader 还会在 Leader 状态停留一个 LeaderLeaseTimeout**（本机 100ms）
	// —— 它要等续租检查失败才会退位。第一次采样正好抓到这个窗口，
	// 于是"曾可用"被严重高估。
	//
	//   实测验证（算术可复算）：n=3/D=5 杀 1 域时 60% 的分片只剩 2 副本，
	//   它们不可能选出 leader（|Q1|=3）；但其中 2/3 的原 leader 还活着，
	//   于是 60% × 2/3 = 40% 被误记成"曾可用"，加上真正能选的 40%，
	//   正好是实测的 80.5%。n=5/D=5 杀 1 域（存活 4/5）同理：
	//   原 leader 存活概率 4/5 = 80%，实测 77.8%。
	//
	// 修法：把开头的 settle 窗口排除在统计之外；同时把首个采样的
	// leader 占比单独记下来（ShardTransientPeak），把这个瞬态如实报出来 ——
	// 它本身是个真实的、短暂的服务窗口，但**不是恢复**。
	settle := o.settle
	if settle <= 0 {
		settle = 2 * time.Second
	}

	avail := make([]bool, o.shards)
	everAvail := make([]bool, o.shards)
	alive := make([]int, o.shards)
	for s := 0; s < o.shards; s++ {
		alive[s] = ss.survivors(s, dead)
	}

	var voteBefore int64
	for _, cl := range ss.sets {
		voteBefore += cl.counters.requestVote.Load()
	}

	samples, counted := 0, 0
	leaderFracSum := 0.0
	transientPeak := -1.0
	pollEvery := o.pollEvery
	if pollEvery <= 0 {
		pollEvery = 100 * time.Millisecond
	}
	tStart := time.Now()
	recoverDeadline := tStart.Add(o.recover)
	for {
		withLeader := 0
		for s, cl := range ss.sets {
			got := false
			for i, nd := range cl.nodes {
				if dead[ss.domainOf[s][i]] {
					continue
				}
				if nd.raft.State() == fraft.Leader {
					got = true
					break
				}
			}
			if got {
				withLeader++
				if time.Since(tStart) >= settle {
					everAvail[s] = true
				}
			}
			avail[s] = got // 最后一次采样时仍在 leader 状态
		}
		samples++
		frac := float64(withLeader) / float64(o.shards)
		if transientPeak < 0 {
			transientPeak = frac
		}
		// 只有过了 settle 窗口的采样才计入"恢复"统计。
		if time.Since(tStart) >= settle {
			counted++
			leaderFracSum += frac
		}
		if time.Now().After(recoverDeadline) {
			break
		}
		time.Sleep(pollEvery)
	}

	var voteAfter int64
	for _, cl := range ss.sets {
		voteAfter += cl.counters.requestVote.Load()
	}
	voteDuring := float64(voteAfter-voteBefore) / o.recover.Seconds()

	availCount, everCount := 0, 0
	for s := 0; s < o.shards; s++ {
		if avail[s] {
			availCount++
		}
		if everAvail[s] {
			everCount++
		}
	}
	leaderFrac := 0.0
	if counted > 0 {
		leaderFrac = leaderFracSum / float64(counted)
	}

	// 算术上界：存活数 ≥ |Q1| 的分片比例。
	predicted := 0
	histogram := map[int]int{}
	for s := 0; s < o.shards; s++ {
		histogram[alive[s]]++
		if alive[s] >= q1 {
			predicted++
		}
	}

	availRate := float64(availCount) / float64(o.shards)
	everRate := float64(everCount) / float64(o.shards)
	predRate := float64(predicted) / float64(o.shards)
	progress("  窗口末仍可用：%d/%d = %.2f%%", availCount, o.shards, availRate*100)
	progress("  退位瞬态峰值 %.1f%%（首个采样；剔除 settle=%v 后统计恢复）", transientPeak*100, settle)
	progress("  剔除瞬态后曾选出：%d/%d = %.2f%%（算术上界 %.2f%%）", everCount, o.shards, everRate*100, predRate*100)
	progress("  leader 占有率均值 %.2f%%；恢复期 RequestVote %.0f 次/秒（%d/%d 次采样计入）",
		leaderFrac*100, voteDuring, counted, samples)

	// ── 5. 组装结果 ──
	res := &result{
		Mode:             "shards",
		N:                o.n,
		Q1:               q1,
		Q2:               q2,
		Majority:         majority(o.n),
		PayloadBytes:     o.payload,
		InjectedDelayUs:  o.delay.Microseconds(),
		ClockFloorUs:     o.clockUs,
		GCPercent:        o.gogc,
		MaxAppendEntries: o.mae,

		ShardCount:       o.shards,
		ShardDomains:     o.domains,
		ShardKilled:      o.kill,
		ShardKilledNodes: killedNodes,
		ShardAliveHist:   histToString(histogram),
		ShardAvailable:   availCount,
		ShardAvailRate:   availRate,
		ShardEverAvail:   everCount,
		ShardEverRate:    everRate,
		ShardTransient:   transientPeak,
		ShardLeaderFrac:  leaderFrac,
		ShardVoteRecover: voteDuring,
		ShardPredicted:   predicted,
		ShardPredRate:    predRate,
		ShardBuildMs:     buildMs,
		ShardReadyMs:     readyMs,
		ShardReadyCount:  readyCount,
		ShardHeartbeat:   hbPerSec,
		ShardVoteRPS:     votePerSec,
	}
	if o.label != "" {
		res.Label = o.label
	} else {
		res.Label = fmt.Sprintf("raftbench-shards-s%d-n%d-d%d-q1%d-q2%d-kill%d",
			o.shards, o.n, o.domains, q1, q2, o.kill)
	}

	res.Notes = []string{
		"负载形状：**多分片 + 整域故障**。这是唯一能检验「决定可达性的是 quorum 的组织结构而不是大小」的形状。",
		"规模是**千分片元数据面**，不是千卡集群：GPU 不参与共识，真正压到元数据层的是分片数、故障域数与故障的相关性。",
		"传输是进程内 in-memory、所有节点共享一个 Go 调度器与一块 CPU —— 所以本模式测的是**结构与算术**，不是吞吐。",
		fmt.Sprintf("放置：副本 i 落在域 (shard+i) %% %d。分片数 %d × 副本 %d = %d 个 Raft 实例。",
			o.domains, o.shards, o.n, o.shards*o.n),
		fmt.Sprintf("注入：杀掉 %d 个故障域（%s），共 %d 个副本下线。",
			o.kill, strings.Join(domainList(dead), " "), killedNodes),
		fmt.Sprintf("|Q1|=%d |Q2|=%d n=%d：选举需要 %d 个存活副本，提交需要 %d 个。",
			q1, q2, o.n, q1, q2),
		"可用性判据是「该分片还有没有唯一 leader」；算术上界是「存活副本数 ≥ |Q1|」。" +
			"实测低于上界的部分，来自选举限制（候选人日志必须足够新）以及" +
			"「|Q1| 贴近存活数时选主接近需要全票、抖动会让每轮重来」。",
		fmt.Sprintf("窗口末仍可用 %.2f%%、窗口内曾选出 %.2f%%、leader 占有率均值 %.2f%%。"+
			"三个数必须一起读：第一个是「此刻有没有 leader」，第二个是"+
			"「有没有恢复过」，第三个是「稳态可用时间的占比」。"+
			"恢复期 RequestVote %.0f 次/秒 —— 显著大于 0 就说明在这三个数之间摇摆，"+
			"而不是干脆地不可用。", availRate*100, everRate*100, leaderFrac*100, voteDuring),
	}
	if availCount < predicted {
		res.Notes = append(res.Notes,
			fmt.Sprintf("实测窗口末 %d 个可用 < 算术上界 %d 个；窗口内曾选出 %d 个。"+
				"差额说明「存活数够」不等于「能稳定持有 leader」。",
				availCount, predicted, everCount))
	}
	return res, nil
}

func domainList(dead map[int]bool) []string {
	out := make([]string, 0, len(dead))
	for d := range dead {
		out = append(out, fmt.Sprintf("d%d", d))
	}
	sort.Strings(out)
	return out
}

func histToString(h map[int]int) string {
	keys := make([]int, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%d:%d", k, h[k]))
	}
	return strings.Join(parts, " ")
}

