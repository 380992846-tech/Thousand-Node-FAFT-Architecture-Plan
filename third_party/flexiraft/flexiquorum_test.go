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
