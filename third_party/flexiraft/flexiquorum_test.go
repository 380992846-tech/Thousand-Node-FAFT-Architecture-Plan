// third_party/flexiraft/flexiquorum_test.go
//
// 本 fork 相对上游 hashicorp/raft 只改了 5 处，全部围绕"让 quorum 大小可配置"。
// 这个文件是那 5 处改动的回归测试 —— fork 若没有测试，任何一次上游同步
// 都可能把改动悄悄覆盖掉，而症状是**静默的数据不一致**，不是编译错误。
//
// 覆盖三件事：
//  1. 提交门槛的算术（纯函数，确定性最强）
//  2. 未设/越界时**必须**回退到多数 —— 这是"基线与上游逐行等价"的保证
//  3. |Q1| 在选举路径上真的生效（行为测试，用杀 leader 来验证）
package raft

import (
	"testing"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

func fiveVoters() Configuration {
	return Configuration{Servers: []Server{
		{ID: "a", Suffrage: Voter},
		{ID: "b", Suffrage: Voter},
		{ID: "c", Suffrage: Voter},
		{ID: "d", Suffrage: Voter},
		{ID: "e", Suffrage: Voter},
	}}
}

// matchAll 按 a=10, b=9, c=8, d=7, e=6 依次上报复制进度。
// 顺序与数值都固定，便于手算期望值 —— 这是本测试能当"参照实现"用的前提。
func matchAll(c *commitment) {
	c.match("a", 10)
	c.match("b", 9)
	c.match("c", 8)
	c.match("d", 7)
	c.match("e", 6)
}

func newTestCommitment(q int, startIndex uint64) *commitment {
	return newCommitment(make(chan struct{}, 1), fiveVoters(), startIndex, q)
}

// TestCommitmentDataQuorumArithmetic 是本次改动最核心的断言。
//
// matchIndexes 排序后为 [6,7,8,9,10]。要"至少 q 个副本达到索引 I"，
// I 就是第 q 大的值，即下标 len-q：
//
//	q=1 -> 10 (只需 leader 自己)
//	q=2 ->  9
//	q=3 ->  8 (多数)
//	q=4 ->  7
//	q=5 ->  6 (全票)
func TestCommitmentDataQuorumArithmetic(t *testing.T) {
	cases := []struct {
		q    int
		want uint64
	}{
		{5, 6},
		{4, 7},
		{3, 8},
		{2, 9},
		{1, 10},
	}
	for _, tc := range cases {
		c := newTestCommitment(tc.q, 1)
		matchAll(c)
		if got := c.getCommitIndex(); got != tc.want {
			t.Errorf("dataQuorumSize=%d: commitIndex = %d, 期望 %d", tc.q, got, tc.want)
		}
	}
}

// TestCommitmentExclusionArithmetic 守 Orca 动态 quorum 的提交算术。
//
// 场景：5 个投票成员，matchIndexes 排序后 [6,7,8,9,10]（a=10…e=6）。
// 排除若干成员后，quorum 只在**剩下的**成员里数：
//
//	q=2，排除 {e}        -> 剩 [7,8,9,10]，第 2 大 = 9
//	q=2，排除 {d,e}      -> 剩 [8,9,10]，  第 2 大 = 9
//	q=2，排除 {a}        -> 剩 [6,7,8,9]， 第 2 大 = 8   ← leader 自己也被排除时的行为
//	q=3，排除 {d,e}      -> 剩 [8,9,10]，  第 3 大 = 8
//
// 关键点：排除集合**只**减少参与计数的成员数，不改变"第 q 大"这个取法；
// 一旦排除到剩下不足 q 个成员，就必须回退到"剩下的多数"（否则会取越界下标）。
func TestCommitmentExclusionArithmetic(t *testing.T) {
	cases := []struct {
		name     string
		q        int
		excluded []ServerID
		want     uint64
	}{
		{"无排除 q=3（多数）", 3, nil, 8},
		{"排除 e，q=2", 2, []ServerID{"e"}, 9},
		{"排除 d,e，q=2", 2, []ServerID{"d", "e"}, 9},
		{"排除 a（leader 自己），q=2", 2, []ServerID{"a"}, 8},
		{"排除 d,e，q=3", 3, []ServerID{"d", "e"}, 8},
		// 剩下 2 个成员却要 q=3：越界 → 回退到"剩下的多数" = 2 → 取第 2 大 = 5？
		// 不：剩下 [9,10]（排除 c,d,e），多数 = 2 → 第 2 大 = 9。
		{"剩下不足 q，回退到剩下多数", 3, []ServerID{"c", "d", "e"}, 9},
		// 排除非成员：必须被忽略，否则等于凭空缩小 quorum（静默破坏 |Q1|+|Q2|>N）。
		{"排除非成员被忽略", 3, []ServerID{"zzz"}, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCommitment(tc.q, 1)
			c.setExcluded(tc.excluded)
			matchAll(c)
			if got := c.getCommitIndex(); got != tc.want {
				t.Errorf("q=%d 排除=%v：commitIndex = %d，期望 %d",
					tc.q, tc.excluded, got, tc.want)
			}
		})
	}
}

// TestCommitmentExclusionCanBeCleared 排除集合必须能撤销（故障恢复后）。
func TestCommitmentExclusionCanBeCleared(t *testing.T) {
	c := newTestCommitment(2, 1)
	c.setExcluded([]ServerID{"d", "e"})
	matchAll(c)
	if got := c.getCommitIndex(); got != 9 {
		t.Fatalf("排除后 commitIndex = %d，期望 9", got)
	}
	if n := c.excludedCount(); n != 2 {
		t.Fatalf("excludedCount = %d，期望 2", n)
	}
	// 清空后回到"全 5 个成员里数第 2 大" = 9（数值相同但参与集合不同）。
	c.setExcluded(nil)
	if n := c.excludedCount(); n != 0 {
		t.Fatalf("清空后 excludedCount = %d，期望 0", n)
	}
	c2 := newTestCommitment(2, 1)
	matchAll(c2)
	if got := c.getCommitIndex(); got != c2.getCommitIndex() {
		t.Errorf("清空排除后应与无排除一致：%d vs %d", got, c2.getCommitIndex())
	}
}

// TestCommitmentExclusionNeverLowersBelowLiveQuorum 是安全性方向的检查：
// 排除集合绝不能把门槛降到"剩下的存活数"以下 —— 那意味着单副本即可提交。
func TestCommitmentExclusionNeverLowersBelowLiveQuorum(t *testing.T) {
	// 只让 b 上报进度（b=9），其余保持 0；排除 c,d,e 后剩下 {a,b}。
	c := newTestCommitment(1, 1) // q=1：只要 1 个成员达到即提交
	c.setExcluded([]ServerID{"c", "d", "e"})
	c.match("b", 9)
	// q=1 且在非排除成员里数第 1 大 → 9。这是**配置本身**允许的
	// （|Q2|=1），不是排除机制引入的 —— 测试把它记录下来，
	// 避免以后有人误以为"排除"能突破 |Q1|+|Q2|>N 的约束。
	if got := c.getCommitIndex(); got != 9 {
		t.Fatalf("commitIndex = %d，期望 9（q=1 的语义）", got)
	}
}
// TestCommitmentFallsBackToMajorityEven 是偶数副本数的回退测试 —— **补写的**。
//
// 原来那条只用 5 个成员，而回退公式 (m-1)/2+1 与正确的 m/2+1
// **只在奇数 m 上相等**：
//
//	m=4: 正确 4/2+1 = 3（多数）；写错得 2（少数）
//	m=2: 正确 2/2+1 = 2（多数）；写错得 1（单副本即可提交）
//
// 也就是说"回退到多数、与上游逐行等价"这句话在偶数副本数下曾经是**假的**，
// 而本项目所有实测的 n 都是奇数（3/5/7/9），所以一直没露头 ——
// 直到给排除集合写测试、用到"剩下 2 个成员"才被抓到。见 BUGS.md BUG-41。
func TestCommitmentFallsBackToMajorityEven(t *testing.T) {
	// 4 个成员：a=10, b=9, c=8, d=7 → 多数 = 3 → 第 3 大 = 8
	cfg4 := Configuration{Servers: []Server{
		{ID: "a", Suffrage: Voter},
		{ID: "b", Suffrage: Voter},
		{ID: "c", Suffrage: Voter},
		{ID: "d", Suffrage: Voter},
	}}
	c := newCommitment(make(chan struct{}, 1), cfg4, 1, 0)
	c.match("a", 10)
	c.match("b", 9)
	c.match("c", 8)
	c.match("d", 7)
	if got := c.getCommitIndex(); got != 8 {
		t.Errorf("4 成员回退：commitIndex = %d，期望 8（多数=3，第 3 大）", got)
	}
	// 与上游写法对照：matched[(len-1)/2] = matched[1] = 8 ✅
	if upstream := []uint64{7, 8, 9, 10}[(4-1)/2]; upstream != 8 {
		t.Fatalf("上游取法给出 %d，期望 8", upstream)
	}

	// 2 个成员：多数 = 2 → 必须两个都达到才提交
	cfg2 := Configuration{Servers: []Server{
		{ID: "a", Suffrage: Voter},
		{ID: "b", Suffrage: Voter},
	}}
	c2 := newCommitment(make(chan struct{}, 1), cfg2, 1, 0)
	c2.match("a", 10) // 只有 leader 自己确认
	if got := c2.getCommitIndex(); got != 0 {
		t.Errorf("2 成员回退：只 1 个成员确认时 commitIndex = %d，期望 0（多数=2）", got)
	}
	c2.match("b", 10)
	if got := c2.getCommitIndex(); got != 10 {
		t.Errorf("2 成员回退：两个都确认后 commitIndex = %d，期望 10", got)
	}
}

// TestCommitmentFallsBackToMajority 保证「基线与上游逐行等价」这句话成立。
//
// 0 / 负数 / 超过副本数 都必须回退到 ⌊m/2⌋+1 = 3，也就是上游的取法
// matched[(len(matched)-1)/2]。如果这里错了，raftbench 的 majority 基线
// 就不再是上游行为，整组对照实验的结论会全部作废。
func TestCommitmentFallsBackToMajority(t *testing.T) {
	const wantMajority = 8 // q=3 -> matched[len-3] = 8

	for _, q := range []int{0, -1, 6, 99} {
		c := newTestCommitment(q, 1)
		matchAll(c)
		if got := c.getCommitIndex(); got != wantMajority {
			t.Errorf("dataQuorumSize=%d 应回退到多数（%d），实际得到 %d",
				q, wantMajority, got)
		}
	}

	// 与上游写法逐字对照：matched[(len(matched)-1)/2]。
	// 上游对 [6,7,8,9,10] 取下标 (5-1)/2 = 2 -> 8，与上面一致。
	matched := []uint64{6, 7, 8, 9, 10}
	if upstream := matched[(len(matched)-1)/2]; upstream != wantMajority {
		t.Fatalf("上游取法给出 %d，本测试的期望值 %d 写错了", upstream, wantMajority)
	}
}

// TestCommitmentRespectsStartIndex 验证 Raft 的提交规则没有被破坏：
// 新 leader 必须先把自己任期的第一条日志复制到 quorum，才允许标记任何东西
// 已提交（否则会把上一个任期的日志按"多数即提交"错误地提交掉）。
func TestCommitmentRespectsStartIndex(t *testing.T) {
	// startIndex=11：最大的 matchIndex 只有 10，谁都够不着，不应提交。
	c := newTestCommitment(1, 11)
	matchAll(c)
	if got := c.getCommitIndex(); got != 0 {
		t.Errorf("quorumMatchIndex=10 < startIndex=11 时不应提交任何东西，实际 commitIndex=%d", got)
	}

	// startIndex=10：刚好够，可以提交。
	c = newTestCommitment(1, 10)
	matchAll(c)
	if got := c.getCommitIndex(); got != 10 {
		t.Errorf("startIndex=10 时应提交到 10，实际 %d", got)
	}
}

// TestCommitmentIsMonotonic 确认提交索引只增不减 —— recalculate 在每次
// match() 后都会跑，若实现里漏了单调性判断，落后的 follower 回包会把
// 已提交的索引拖回去。
func TestCommitmentIsMonotonic(t *testing.T) {
	c := newTestCommitment(3, 1)
	matchAll(c)
	first := c.getCommitIndex()
	if first != 8 {
		t.Fatalf("前置条件不成立：commitIndex=%d，期望 8", first)
	}
	// 让一个 follower 大幅落后，再触发 recalculate。
	c.match("a", 11) // 只推进一个副本，第 3 大的值仍是 8
	if got := c.getCommitIndex(); got < first {
		t.Errorf("commitIndex 从 %d 回退到 %d", first, got)
	}
	// 全部推进到更高位置后应当前进。
	c.match("b", 20)
	c.match("c", 20)
	if got := c.getCommitIndex(); got <= first {
		t.Errorf("三个副本都推进后 commitIndex 应前进，仍是 %d", got)
	}
}

// TestElectionQuorumSizeIsEnforced 是 |Q1| 改动的行为测试。
//
// n=3，杀掉 leader 后只剩 2 个副本：
//   - |Q1| 未设（回退到多数 2）：2 >= 2，应当能选出新 leader
//   - |Q1|=3：2 < 3，**必然**选不出来
//
// 这条性质在结果上就是"阶跃"而不是"概率变低"，所以测试可以断言确切结果，
// 不需要统计。
//
// ⚠️ 负例里**不能**用 cluster.Leader() 轮询：它内部走 pollState，
// 在 longstopTimeout（5s）内等不到稳定的 Leader 就会调 Failf 把测试判失败
// —— 也就是说"没有 leader"这件事本身会把 c.Leader() 变成致命操作。
// 所以这里直接读各节点自己的 State()。
func TestElectionQuorumSizeIsEnforced(t *testing.T) {
	// 直接看各节点的 State()，绕开 cluster.Leader() 的稳定性等待。
	// 返回 nil 表示"当前没有唯一 leader"。
	leaderOf := func(c *cluster) *Raft {
		var found *Raft
		for _, r := range c.rafts {
			if r.State() == Leader {
				if found != nil {
					return nil // 出现双主，按"没有唯一 leader"处理
				}
				found = r
			}
		}
		return found
	}

	run := func(name string, electionQuorum int, wantNewLeader bool) {
		t.Run(name, func(t *testing.T) {
			conf := inmemConfig(t)
			conf.ElectionQuorumSize = electionQuorum
			conf.DataQuorumSize = 1 // 满足 |Q1|+|Q2| > n 的前提
			// 负例会持续选举失败并刷屏，关掉日志。
			conf.Logger = hclog.NewNullLogger()

			c := MakeCluster(3, t, conf)
			defer c.Close()

			// 初始选主用 c.Leader() 是安全的：此时必然有 leader。
			leader := c.Leader()
			if leader == nil {
				t.Fatal("初始没有选出 leader")
			}
			// 让这个节点彻底消失：既移出集群列表，也真的 Shutdown。
			c.RemoveServer(leader.localID)
			if err := leader.Shutdown().Error(); err != nil {
				t.Fatalf("关闭原 leader 失败: %v", err)
			}

			// 给足选举时间（ElectionTimeout = 50ms，这里等 1.5s）。
			deadline := time.Now().Add(1500 * time.Millisecond)
			var got *Raft
			for time.Now().Before(deadline) {
				if l := leaderOf(c); l != nil && l != leader {
					got = l
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			switch {
			case wantNewLeader && got == nil:
				t.Errorf("|Q1|=%d：存活 2 个副本，应当能选出新 leader，但没有", electionQuorum)
			case !wantNewLeader && got != nil:
				t.Errorf("|Q1|=%d：存活 2 个副本 < |Q1|，不应选出 leader，但选出了", electionQuorum)
			}
		})
	}

	run("election-quorum-majority", 0, true)
	run("election-quorum-all", 3, false)
}

// TestDataQuorumOneStillCommits 确认 |Q2|=1 在真实提交路径上确实工作：
// leader 不等待任何 follower 就能推进 commitIndex，日志照常追加。
//
// 注意这里**不**断言 follower 也有这些日志 —— |Q2|=1 的语义恰恰是
// "返回成功时可能只有 leader 有"。那部分由 cmd/raftbench -mode lag 量测。
func TestDataQuorumOneStillCommits(t *testing.T) {
	conf := inmemConfig(t)
	conf.ElectionQuorumSize = 3
	conf.DataQuorumSize = 1

	c := MakeCluster(3, t, conf)
	defer c.Close()

	leader := c.Leader()
	if leader == nil {
		t.Fatal("没有选出 leader")
	}

	const n = 50
	for i := 0; i < n; i++ {
		if err := leader.Apply([]byte{byte(i)}, time.Second).Error(); err != nil {
			t.Fatalf("第 %d 条 Apply 失败: %v", i, err)
		}
	}

	// leader 的 FSM 最终应应用到 n 条命令。
	deadline := time.Now().Add(2 * time.Second)
	for {
		applied := leader.AppliedIndex()
		if int(applied) >= n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("|Q2|=1 下 leader 只应用到索引 %d，期望 >= %d", applied, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
