// cmd/raftbench/orca.go
//
// Orca 的**端到端**实现与实测（PVLDB vol 19, 2026，DOI 10.14778/3836663.3836709）。
//
// ── 为什么必须补这一块 ────────────────────────────────────────────────
// 此前 Orca 只是 pkg/faft 里的一行**解析对照**（planner_orca.go），
// 从没在真实 Raft 上跑过。而查代码时发现一件必须先说清楚的事：
//
//	OrcaPlanner{q2 = k+1, q1 = n-q2+1}
//	FlexiRaftPlanner{DataQuorum = k+1} 也用 q1 = n-q2+1
//
// **两者是同一个几何。** 也就是说对照表里的 `orca` 行与 `flexiraft-dq{k+1}`
// 行是同一套机制，Orca 唯一的独立内容是"把已判定故障的副本从 quorum 计算中
// **排除**"（动态 quorum）—— 而那一半当时没实现（planner_orca.go 的 Notes 里写着）。
// 所以"Orca 端到端"这件事，实质就是把排除机制落到 fork 上并测出来。
//
// ── 这一半为什么危险（也正是本实验要测的）──────────────────────────
// 排除集合把 quorum 缩小了，于是 |Q1|+|Q2|>N 这个交集保证可能被破坏。
// 两种做法必须**分开测**，而不是替 Orca 选一个：
//
//	full-set：只有 |Q2| 在非排除成员上算，|Q1| 仍在全部成员上算
//	          ⇒ 交集保证仍成立；代价是故障多了就选不出主
//	live-set：|Q1| 与 |Q2| 都在非排除成员上算
//	          ⇒ 存活节点少也能选主/提交，但**被误判为故障的节点可能自行选举**，
//	            它的 Q1 与在位 leader 的 Q2 未必相交
//
// 判据不靠论证，靠实测：每个臂都过同一套三层安全判据 + "同一任期两个 leader"检查。
//
// ── 实验形状 ──────────────────────────────────────────────────────────
// 每个臂：建 n 副本 → 预热 → 施压 → **杀掉 f 个副本（先杀 leader）**
// → 继续施压 → 看两件事：
//
//	① 杀完之后还能不能提交（服务能力）
//	② 三层安全性有没有被证伪（丢写 / 回滚 / 被杀节点可恢复）+ 重复 leader
//
// 这正是 Orca 摘要那句"比 Raft 与 FlexiRaft 容忍更多故障且开销可忽略"的直接检验：
// 它到底能容忍更多故障，还是只是把风险挪到了安全性上。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fraft "github.com/distributed-kv/kvstore/third_party/flexiraft"
)

type orcaOpts struct {
	buildOpts
	label string

	arms   []orcaArm
	kill   int
	// keepLeader：只杀 follower，保留 leader。
	//
	// 这个开关把"缩小 |Q2| 到底有没有用"这件事分成了两个场景：
	//   杀 leader：需要**重新选举** ⇒ 决定因素变成 |Q1|，而 FPaxos 要求
	//             |Q1| 随 |Q2| 变小而变大 ⇒ 缩小 |Q2| 换不来可用性
	//   留 leader：不需要选举 ⇒ 只要存活副本数 ≥ |Q2| 就能继续提交 ⇒
	//             缩小 |Q2| 的收益才显现出来
	// 两个场景的结论完全不同，只报其中一个都是误导。
	keepLeader bool
	killAt time.Duration
	after  time.Duration
	settle time.Duration
	conc   int
	timeout time.Duration

	electTimeout time.Duration
	clockFloor   time.Duration
	gogc         int
}

// orcaArm 一个待测的 quorum 配置。
type orcaArm struct {
	Name string `json:"name"`
	Q2   int    `json:"q2_configured"`
	Q1   int    `json:"q1_configured"`
	Mode fraft.ExclusionMode `json:"-"`
	// Exclude 是否在故障注入后启用排除集合（"动态"部分）。
	Exclude bool   `json:"exclude"`
	Desc    string `json:"desc"`
}

// defaultOrcaArms 默认对照集：n=5、k=1（q2=2）与多数两档。
//
// 分界点很清楚：杀 3 个之后
//
//	majority    (3,3) 不存在多数 ⇒ 提交停止
//	orca-static (4,2) 静态下仍可提交（2 个存活 ≥ q2），但选举要 4 票
//	orca-full   (4,2) leader 活着就能继续提交；选举仍要 4 票（安全）
//	orca-live   (4,2) 选举门槛也缩小 ⇒ 可能连安全性一起缩小
func defaultOrcaArms() []orcaArm {
	return []orcaArm{
		{Name: "majority", Desc: "Raft / Multi-Paxos：|Q1|=|Q2|=⌊n/2⌋+1"},
		{Name: "orca-static", Q2: 2, Q1: 4, Mode: fraft.ExclusionOff,
			Desc: "Orca 静态几何（k=1 ⇒ |Q2|=2、|Q1|=4）；**与 FlexiRaft dq=2 同一几何**"},
		{Name: "orca-full", Q2: 2, Q1: 4, Mode: fraft.ExclusionFullSet, Exclude: true,
			Desc: "排除故障副本；|Q1| 仍在全部成员上算 ⇒ 保持 |Q1|+|Q2|>n"},
		{Name: "orca-live", Q2: 2, Q1: 4, Mode: fraft.ExclusionLiveSet, Exclude: true,
			Desc: "排除故障副本；|Q1| 也在非排除成员上算 ⇒ 门槛更小，**安全性待证伪**"},
	}
}

// orcaRun 一个臂一次运行的结果。
type orcaRun struct {
	Arm  string `json:"arm"`
	Desc string `json:"desc"`

	Q1Configured int    `json:"q1_configured"`
	Q2Configured int    `json:"q2_configured"`
	Exclusion    string `json:"exclusion_mode"`
	Excluded     int    `json:"excluded_voters"`

	AckedBefore    int `json:"acked_before_kill"`
	AckedAfterKill int `json:"acked_after_kill"`
	// ServedAfterKill：注入故障后是否还成功提交过 —— 服务能力的直接判据。
	ServedAfterKill bool `json:"served_after_fault"`

	Killed []string `json:"killed_nodes"`
	// KillLeader：第一个被杀的是不是当时的 leader。
	// 它决定"要不要重新选举"，从而决定这个场景在测 |Q1| 还是 |Q2| ——
	// 两个场景的结论可能相反，所以必须逐条记录。
	KillLeader bool     `json:"killed_leader"`
	NewLeader  string   `json:"new_leader_after_fault"`
	// ElectMs 从注入故障到再次看到 leader 的耗时（-1 = 没选出来）。
	ElectMs float64 `json:"re_election_ms"`

	// 三层安全判据（与 fail 模式同口径）。
	//
	//	① LostWrites      每条确认过的写必须存在于**任何**节点的日志里
	//	                  （含被杀节点；只在存活节点里找不到 ≠ 丢失，那是"不可达"）
	//	② NewLeaderMissing 注入前已确认的写必须都在**当前 leader** 的日志里
	//	③ VictimMissing   每个被杀节点必须含它**自己确认过**的全部写
	//
	// ③ 之所以能精确算，是因为施压时按节点记录了确认集合（ackedByNode）。
	LostWrites       int `json:"lost_acked_writes"`
	NewLeaderMissing int `json:"new_leader_missing"`
	VictimMissing    int `json:"victim_missing"`
	// UnreachableAcked：确认过的写里，**存活节点一个都没有**的条数。
	// 它不是"丢失"（③ 会验证被杀节点的日志里还在），而是"数据暂时只有死节点有" ——
	// 这正是 |Q2| 变小的真实风险面，所以单独报，不混进安全性判据。
	UnreachableAcked int `json:"acked_only_on_killed_nodes"`
	// RollbackApplicable：没有新 leader 时 ② 不适用（记为 false）。
	RollbackApplicable bool `json:"rollback_applicable"`
	SafetyOK           bool `json:"safety_ok"`

	DuplicateLeaders []string `json:"duplicate_leaders"`
	// Consistent：**只比存活节点**的 (count,lastIndex,hash) 是否一致。
	Consistent bool `json:"consistent_survivors"`
	// ConsistentAllNodes：含被杀节点的口径。**杀掉节点后必然为 false**，
	// 因为被杀节点的状态机停在它死的那一刻 —— 记下来是为了避免误读。
	ConsistentAllNodes bool   `json:"consistent_all_nodes"`
	ConsistencyMsg     string `json:"consistency_msg"`

	Err string `json:"error,omitempty"`
}

type orcaReport struct {
	Mode   string `json:"mode"`
	N      int    `json:"n"`
	Kill   int    `json:"kill_count"`
	KillAt string `json:"kill_after"`
	After  string `json:"stress_after_kill"`
	Delay  string `json:"injected_delay"`
	Conc   int    `json:"concurrency"`
	Label  string `json:"label"`

	Runs    []orcaRun `json:"runs"`
	Verdict []string  `json:"verdict"`
	Notes   []string  `json:"notes"`
}

func runOrcaMode(o orcaOpts) (*orcaReport, error) {
	if len(o.arms) == 0 {
		o.arms = defaultOrcaArms()
	}
	if o.kill <= 0 {
		o.kill = 3
	}
	if o.killAt <= 0 {
		o.killAt = time.Second
	}
	if o.after <= 0 {
		o.after = 3 * time.Second
	}
	if o.conc <= 0 {
		o.conc = 16
	}
	if o.timeout <= 0 {
		o.timeout = 10 * time.Second
	}
	if o.settle <= 0 {
		o.settle = 20 * time.Second
	}

	rep := &orcaReport{
		Mode: "orca", N: o.n, Kill: o.kill,
		KillAt: o.killAt.String(), After: o.after.String(),
		Delay: o.delay.String(), Conc: o.conc, Label: o.label,
		Notes: orcaNotes(),
	}

	for _, arm := range o.arms {
		run := runOrcaArm(o, arm)
		rep.Runs = append(rep.Runs, run)
		fmt.Printf("[orca] %-12s |Q2c|=%-2d |Q1c|=%-2d %-8s  杀前 %-6d 杀后 %-6d "+
			"服务=%-5v 安全=%-5v 一致=%v\n",
			arm.Name, run.Q2Configured, run.Q1Configured, run.Exclusion,
			run.AckedBefore, run.AckedAfterKill, run.ServedAfterKill, run.SafetyOK, run.Consistent)
		if run.Err != "" {
			fmt.Printf("         ⚠️ %s\n", run.Err)
		}
	}

	rep.Verdict = orcaVerdicts(rep)
	return rep, nil
}

// runOrcaArm 跑一个臂：建集群 → 预热 → 施压 → 注入故障 → 继续施压 → 判安全性。
func runOrcaArm(o orcaOpts, arm orcaArm) orcaRun {
	run := orcaRun{
		Arm: arm.Name, Desc: arm.Desc,
		Q1Configured: arm.Q1, Q2Configured: arm.Q2,
		Exclusion: arm.Mode.String(), ElectMs: -1,
	}

	bo := o.buildOpts
	bo.q1, bo.q2 = arm.Q1, arm.Q2
	bo.exclusionMode = arm.Mode
	cl, err := buildCluster(bo)
	if err != nil {
		run.Err = fmt.Sprintf("建集群失败: %v", err)
		return run
	}
	defer cl.shutdown()

	if _, err := cl.waitLeader(o.electTimeout); err != nil {
		run.Err = fmt.Sprintf("等 leader 失败: %v", err)
		return run
	}
	if err := warmupRun(cl, 200, 4, o.payload, o.timeout); err != nil {
		run.Err = fmt.Sprintf("预热失败: %v", err)
		return run
	}

	// ── 施压 ──
	//
	// 按**节点**记录确认集合：这样"被杀节点是否含有它自己确认过的写"
	// 这一层才能精确算，而不是靠推断（fail 模式只杀一个节点，用不上）。
	var ackMu sync.Mutex
	acked := make(map[uint64]bool)
	ackedByNode := make(map[fraft.ServerID]map[uint64]bool)
	var nextSeq atomic.Uint64
	nextSeq.Store(uint64(1) << 40)
	var killSentinel atomic.Int64 // >0 表示故障已注入
	var afterKillAcked atomic.Int64

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < o.conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				cur := cl.leader()
				if cur == nil {
					time.Sleep(time.Millisecond)
					continue
				}
				s := nextSeq.Add(1)
				if err := cur.raft.Apply(makeCmd(o.payload, s), o.timeout).Error(); err != nil {
					continue
				}
				ackMu.Lock()
				acked[s] = true
				if ackedByNode[cur.id] == nil {
					ackedByNode[cur.id] = make(map[uint64]bool)
				}
				ackedByNode[cur.id][s] = true
				ackMu.Unlock()
				if killSentinel.Load() > 0 {
					afterKillAcked.Add(1)
				}
			}
		}()
	}

	time.Sleep(o.killAt)

	// ── 注入故障：先杀 leader，再杀其他副本 ──
	victim := cl.leader()
	if victim == nil {
		close(stop)
		wg.Wait()
		run.Err = "施压期间没有 leader，故障注入无法进行"
		return run
	}
	order := make([]*node, 0, len(cl.nodes))
	if o.keepLeader {
		// 先杀 follower；leader 排在最后，只有"杀不完"时才会轮到它。
		for _, nd := range cl.nodes {
			if nd != victim {
				order = append(order, nd)
			}
		}
		order = append(order, victim)
	} else {
		order = append(order, victim)
		for _, nd := range cl.nodes {
			if nd != victim {
				order = append(order, nd)
			}
		}
	}
	killN := o.kill
	// 至少留一个节点活着：否则测的是"全灭"，不是可用性。
	if killN >= len(order) {
		killN = len(order) - 1
	}

	// 故障时刻的快照（必须在注入前取）。
	ackMu.Lock()
	beforeSet := make(map[uint64]bool, len(acked))
	for s := range acked {
		beforeSet[s] = true
	}
	run.AckedBefore = len(beforeSet)
	ackMu.Unlock()

	run.KillLeader = killN > 0 && order[0] == victim

	var killed []*node
	for i := 0; i < killN; i++ {
		nd := order[i]
		_ = nd.raft.Shutdown().Error()
		nd.raw.Close()
		killed = append(killed, nd)
		run.Killed = append(run.Killed, string(nd.id))
	}

	// ⚠️ 计数起点必须取在**杀完之后**，不是杀之前。
	//
	// 第一版把 sentinel 设在批量杀之前，于是"杀之后新增的确认数"里混进了
	// **in-flight 写**（杀之前发出、杀的过程中才返回），让 majority 这种
	// 明明已经无法提交的臂也报出"杀后 5 条" —— 结论直接反了。
	// 这正是评审在 fail 模式第 4 步指出的同一类污染：计数窗口的起点必须与被
	// 测量的现象对齐，否则窗口里混进的是别的东西。见 BUGS.md BUG-42。
	killSentinel.Store(time.Now().UnixNano())

	// ── 启用排除集合（这一步就是"动态 quorum"）──
	if arm.Exclude {
		ids := make([]fraft.ServerID, 0, len(killed))
		for _, nd := range killed {
			ids = append(ids, nd.id)
		}
		// 真实系统里这一步由故障检测器完成；本模式自己就是检测器（它知道自己杀了谁）。
		// **误判**（把活着的节点当成死的）是 live-set 模式的主要风险来源，
		// 本模式不制造误判，所以测到的是"检测完全正确时的上界"。
		for _, nd := range cl.nodes {
			if !isKilled(nd, killed) {
				nd.raft.SetExcludedVoters(ids)
			}
		}
		run.Excluded = len(ids)
	}

	// ── 观察期：等重新选主 + 看还能不能提交 ──
	electStart := time.Now()
	deadline := electStart.Add(o.electTimeout)
	var newLeader *node
	for time.Now().Before(deadline) {
		for _, nd := range cl.nodes {
			if !isKilled(nd, killed) && nd.raft.State() == fraft.Leader {
				newLeader = nd
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if newLeader != nil {
		run.NewLeader = string(newLeader.id)
		run.ElectMs = float64(time.Since(electStart).Microseconds()) / 1000.0
	}

	time.Sleep(o.after)
	close(stop)
	wg.Wait()

	run.AckedAfterKill = int(afterKillAcked.Load())
	run.ServedAfterKill = run.AckedAfterKill > 0

	// ── 一致性 + 重复 leader ──
	//
	// ⚠️ checkConsistency 遍历**全部**节点，包括已经被杀的那些。
	// 被杀节点的状态机停在它死的那一刻，与存活 leader 必然不同 ——
	// 所以整轮结果一定 consistent=false，那一列没有信息量。
	// 因此这里另算**只比存活节点**的一致性：那才是"副本是否一致"的问题。
	// 两条都记进结果，避免只看一个数就下结论。
	counts, lastIdx, hashes, consistentAllNodes, msg := checkConsistency(cl, o.settle)
	run.ConsistencyMsg = msg
	run.ConsistentAllNodes = consistentAllNodes

	survivorOK := true
	first := -1
	for i, nd := range cl.nodes {
		if isKilled(nd, killed) {
			continue
		}
		if first < 0 {
			first = i
			continue
		}
		if counts[i] != counts[first] || lastIdx[i] != lastIdx[first] || hashes[i] != hashes[first] {
			survivorOK = false
		}
	}
	run.Consistent = survivorOK
	run.DuplicateLeaders = duplicateLeaders(cl, killed)

	// ── 三层安全性 ──
	run.LostWrites, run.NewLeaderMissing, run.VictimMissing, run.UnreachableAcked, run.RollbackApplicable =
		orcaSafetyLayers(cl, acked, beforeSet, ackedByNode, killed)
	run.SafetyOK = run.LostWrites == 0 &&
		(!run.RollbackApplicable || run.NewLeaderMissing == 0) &&
		run.VictimMissing == 0 && len(run.DuplicateLeaders) == 0
	return run
}

func isKilled(nd *node, killed []*node) bool {
	for _, k := range killed {
		if k == nd {
			return true
		}
	}
	return false
}

// duplicateLeaders 找出"同一任期里有多个节点认为自己是 leader"的情况。
//
// 这是**安全性**的直接判据，不是性能指标：FPaxos 的 |Q1|+|Q2|>N 保证的是
// 同一个任期不可能选出两个 leader。排除机制一旦把交集保证破坏掉，
// 这里就会报出来 —— 这是本实验最关键的一列。
//
// 为什么看"同一任期"而不是"同时处于 Leader 状态"：旧 leader 在被新 leader
// 通知之前会继续保持 Leader 状态，那是**正常瞬态**，不是分裂。
// （与 pkg/cluster 的 DuplicateLeaders 同口径。）
func duplicateLeaders(cl *cluster, killed []*node) []string {
	byTerm := map[string][]string{}
	for _, nd := range cl.nodes {
		if isKilled(nd, killed) {
			continue
		}
		if nd.raft.State() != fraft.Leader {
			continue
		}
		// 公开 API 没有 Term()；Stats() 是官方暴露途径。
		term := nd.raft.Stats()["term"]
		byTerm[term] = append(byTerm[term], string(nd.id))
	}
	var out []string
	for term, ids := range byTerm {
		if len(ids) > 1 {
			out = append(out, fmt.Sprintf("term=%s: %s", term, strings.Join(ids, ",")))
		}
	}
	sort.Strings(out)
	return out
}

// orcaSafetyLayers 复算三层安全性。
//
// 刻意不复用 fail 模式的实现：fail 模式只杀一个节点，本模式杀多个，
// "谁该有哪条写"的期望不同，混用会把两种语义搅在一起
// （fail 模式第一版就是这么错的：拿全时段集合去查每个节点，必然报"丢数据"）。
func orcaSafetyLayers(cl *cluster, acked, beforeSet map[uint64]bool,
	ackedByNode map[fraft.ServerID]map[uint64]bool, killed []*node) (lost, rollback, victimMiss, unreachable int, rollbackOK bool) {

	// ① 每条确认过的写至少存在于**任何一个**节点的日志里（含被杀节点）。
	//
	// ⚠️ 第一版这里只查**存活**节点，于是把 |Q2|=2、且那 2 个副本恰好都被杀
	// 的情况报成了"丢写"。那不是丢失 —— 写还在被杀节点的日志里（③ 会验证），
	// 只是暂时**不可达**。判据口径搞错会把"不可用"报成"丢数据"，
	// 与 fail 模式第一版犯的是同一个错（那次是把全时段集合去查每个节点）。
	// ① 的正确口径是"全节点"：真丢了 = 任何节点的日志里都没有。
	allSeqs := make([]map[uint64]uint64, 0, len(cl.nodes))
	for _, nd := range cl.nodes {
		allSeqs = append(allSeqs, collectAckedSeqs(nd))
	}
	for s := range acked {
		found := false
		for _, m := range allSeqs {
			if _, ok := m[s]; ok {
				found = true
				break
			}
		}
		if !found {
			lost++
		}
	}

	// ①b 另算一个"存活节点里是否可达"：它不是安全性判据（不是丢失），
	//     但**可用性**上很重要 —— |Q2|=1/2 时数据可能只寄存在被杀节点上。
	for s := range acked {
		ok := false
		for i, nd := range cl.nodes {
			if isKilled(nd, killed) {
				continue
			}
			if _, has := allSeqs[i][s]; has {
				ok = true
				break
			}
		}
		if !ok {
			unreachable++
		}
	}


	// ② 注入前已确认的写必须都在当前 leader 的日志里 —— 没 leader 时不适用。
	if nl := cl.leader(); nl != nil {
		rollbackOK = true
		nlSeqs := collectAckedSeqs(nl)
		for s := range beforeSet {
			if _, ok := nlSeqs[s]; !ok {
				rollback++
			}
		}
	}

	// ③ 每个被杀节点必须含**它自己确认过**的全部写。
	//    精确算得出来，是因为施压时按节点记录了确认集合。
	for _, nd := range killed {
		own := ackedByNode[nd.id]
		if len(own) == 0 {
			continue
		}
		seqs := collectAckedSeqs(nd)
		miss := 0
		for s := range own {
			if _, ok := seqs[s]; !ok {
				miss++
			}
		}
		if miss > 0 {
			victimMiss++
		}
	}
	return lost, rollback, victimMiss, unreachable, rollbackOK
}

func orcaVerdicts(r *orcaReport) []string {
	var served, unsafe, noElect []string
	for _, run := range r.Runs {
		if run.Err != "" {
			continue
		}
		if run.ServedAfterKill {
			served = append(served, run.Arm)
		}
		if run.NewLeader == "" {
			noElect = append(noElect, run.Arm)
		}
		if !run.SafetyOK {
			unsafe = append(unsafe, fmt.Sprintf("%s（丢写=%d 回滚=%d 被杀节点缺=%d 重复leader=%d）",
				run.Arm, run.LostWrites, run.NewLeaderMissing, run.VictimMissing,
				len(run.DuplicateLeaders)))
		}
	}
	out := []string{
		fmt.Sprintf("杀 %d/%d 个副本后**仍能提交**的臂：%s", r.Kill, r.N,
			joinOrNone(served)),
		fmt.Sprintf("杀完之后**选不出新 leader**的臂：%s", joinOrNone(noElect)),
	}
	if len(unsafe) == 0 {
		out = append(out, "本轮没有任何臂被安全性判据证伪（丢写/回滚/被杀节点可恢复/重复 leader 全过）。"+
			"⚠️ 这不等于「排除机制安全」—— 本轮没有制造**误判**（把活节点当死节点），"+
			"而误判正是 live-set 模式的风险来源。")
	} else {
		out = append(out, "⚠️ 安全性被判据证伪的臂："+strings.Join(unsafe, "；"))
	}
	return out
}

func joinOrNone(xs []string) string {
	if len(xs) == 0 {
		return "（无）"
	}
	return strings.Join(xs, ", ")
}

func orcaNotes() []string {
	return []string{
		"Orca 的静态几何与 FlexiRaft 的参数化**完全相同**（q2=k+1、q1=n-q2+1），" +
			"所以 orca-static 与 flexiraft-dq2 必然逐位一致 —— 这不是巧合，是同一套机制；" +
			"对照表里两者本就不该算作两个独立 baseline。",
		"Orca 唯一独立的内容是**把已判定故障的副本从 quorum 计算中排除**。" +
			"本模式对两种做法（full-set / live-set）分别实测，不替原文选一种。",
		"⚠️ 未获得 Orca 论文全文：上述结构性描述来自检索到的摘要，" +
			"k 的定义与排除集合的维护方式可能与原文有差异，论文中须披露并做敏感性分析。",
		"故障检测器在本模式里就是模式本身（它知道自己杀了谁）⇒ 测到的是" +
			"**检测完全正确**时的上界。真实系统的误判与检测延迟没有建模，" +
			"而误判正是 live-set 模式的主要风险来源。",
		"三层判据与 fail 模式同口径；③（被杀节点可恢复）在本模式里是精确的，" +
			"因为施压时按节点记录了确认集合。",
	}
}

func writeOrcaReport(path string, rep *orcaReport) error {
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("\n→ 已写出 %s\n", path)
	return nil
}
