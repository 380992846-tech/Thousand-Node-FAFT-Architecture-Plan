// cmd/raftbench/main_test.go
//
// 这些测试守的是"实验本身是否可信"，不是"程序能不能跑"：
//   - quorum 解析与回退是否与上游一致（错了整组对照就作废）
//   - Flexible Paxos 约束是否真的被拦住（漏了会静默丢数据）
//   - sweep 生成的配置是否每一项都合法
package main

import (
	"testing"
	"time"
)

func TestEffectiveQuorum(t *testing.T) {
	cases := []struct {
		n, q, want int
		note       string
	}{
		{5, 0, 3, "0 表示未设 -> 多数"},
		{5, -1, 3, "负数 -> 多数"},
		{5, 6, 3, "超过副本数 -> 多数"},
		{5, 3, 3, "恰好多数"},
		{5, 5, 5, "全票"},
		{5, 1, 1, "最小"},
		{4, 0, 3, "偶数 n 的多数是 n/2+1"},
		{1, 0, 1, "单节点"},
		{1, 1, 1, "单节点显式设 1"},
	}
	for _, c := range cases {
		if got := effectiveQuorum(c.n, c.q); got != c.want {
			t.Errorf("effectiveQuorum(n=%d, q=%d) = %d，期望 %d（%s）",
				c.n, c.q, got, c.want, c.note)
		}
	}
}

// TestSweepConfigsSatisfyFPaxos 是这一组里最重要的一条：
// sweep 会一次跑十几组配置，只要有**任何一组**违反 |Q1|+|Q2| > n，
// 那组数字就是"共识可能丢数据"的产物，整个结果集都要作废。
func TestSweepConfigsSatisfyFPaxos(t *testing.T) {
	for _, n := range []int{3, 4, 5, 6, 7, 9, 11} {
		cfgs := sweepConfigs(n)
		if len(cfgs) == 0 {
			t.Fatalf("n=%d：sweep 没有生成任何配置", n)
		}
		seen := map[[2]int]bool{}
		for _, c := range cfgs {
			q1, q2 := c[0], c[1]
			if q1+q2 <= n {
				t.Errorf("n=%d：配置 (|Q1|=%d, |Q2|=%d) 违反 FPaxos 约束：%d+%d = %d <= %d",
					n, q1, q2, q1, q2, q1+q2, n)
			}
			if q1 < 1 || q1 > n || q2 < 1 || q2 > n {
				t.Errorf("n=%d：配置 (%d, %d) 超出 [1, n] 范围", n, q1, q2)
			}
			if seen[c] {
				t.Errorf("n=%d：配置 (%d, %d) 重复", n, q1, q2)
			}
			seen[c] = true
		}
		// 至少要有"多数"这一档，否则没有基线可比。
		maj := majority(n)
		if !seen[[2]int{maj, maj}] {
			t.Errorf("n=%d：sweep 缺少多数基线 (%d, %d)", n, maj, maj)
		}
		// 以及 |Q2|=1 这一极值档。
		foundExtreme := false
		for _, c := range cfgs {
			if c[1] == 1 {
				foundExtreme = true
			}
		}
		if !foundExtreme {
			t.Errorf("n=%d：sweep 缺少 |Q2|=1 的极值档", n)
		}
	}
}

// TestBuildClusterRejectsUnsafeQuorum 确认安全性条件在**建集群之前**被拦住。
//
// 这条约束违反时的症状是静默的数据不一致（已提交的日志可能被回滚），
// 不会报错、不会 panic，所以入口校验是唯一的防线。
func TestBuildClusterRejectsUnsafeQuorum(t *testing.T) {
	unsafe := []struct{ n, q1, q2 int }{
		{5, 2, 2}, // 2+2 = 4 <= 5
		{5, 3, 2}, // 3+2 = 5 <= 5
		{3, 1, 1}, // 1+1 = 2 <= 3
		{3, 2, 1}, // 2+1 = 3 <= 3
	}
	for _, c := range unsafe {
		_, err := buildCluster(buildOpts{n: c.n, q1: c.q1, q2: c.q2, sigma: 0.6, payload: 64})
		if err == nil {
			t.Errorf("n=%d q1=%d q2=%d：不安全组合（|Q1|+|Q2| = %d <= n）应当被拒绝，但没有",
				c.n, c.q1, c.q2, c.q1+c.q2)
			continue
		}
		if !contains(err.Error(), "Flexible Paxos") {
			t.Errorf("n=%d q1=%d q2=%d：错误信息没有点明原因是 Flexible Paxos 约束：%v",
				c.n, c.q1, c.q2, err)
		}
	}
}

// TestBuildClusterAcceptsBoundaryQuorum 确认边界组合（|Q1|+|Q2| = n+1）
// 不被误杀 —— 那是合法的最紧配置，也正是 sweep 的极值档。
func TestBuildClusterAcceptsBoundaryQuorum(t *testing.T) {
	cl, err := buildCluster(buildOpts{n: 3, q1: 3, q2: 1, sigma: 0.6, seed: 1, payload: 64})
	if err != nil {
		t.Fatalf("|Q1|=3 |Q2|=1 n=3（3+1 = 4 > 3）应当被接受，却报错: %v", err)
	}
	defer cl.shutdown()
	if cl.q1 != 3 || cl.q2 != 1 {
		t.Errorf("生效的 quorum 是 (q1=%d, q2=%d)，期望 (3, 1)", cl.q1, cl.q2)
	}
	if _, err := cl.waitLeader(5 * time.Second); err != nil {
		t.Fatalf("边界配置下选主失败: %v", err)
	}
}

// TestMakeCmdIsUniquePerCall 守住开发中真实踩过的坑：
// 复用 payload 缓冲区会让 InmemStore/InmemTransport 里"已提交"的日志
// 被后续迭代改写，症状是各节点条数一致但状态指纹不一致。
// 所以 makeCmd 必须每次新分配。
func TestMakeCmdIsUniquePerCall(t *testing.T) {
	a := makeCmd(16, 1)
	b := makeCmd(16, 2)
	if &a[0] == &b[0] {
		t.Fatal("makeCmd 两次调用返回了同一块底层数组 —— 必须每次新分配")
	}
	if a[0] == b[0] && a[7] == b[7] {
		t.Fatal("不同序号生成了相同的前 8 字节")
	}
	// 序号必须写在前 8 字节（大端），FSM 靠它算状态指纹。
	if got := a[7]; got != 1 {
		t.Errorf("seq=1 时最后一个字节应为 1，实际 %d", got)
	}
	if got := b[7]; got != 2 {
		t.Errorf("seq=2 时最后一个字节应为 2，实际 %d", got)
	}
}

// TestFailAckAccounting 守的是评审指出的那个坑：
//
//	"如果新 leader 一直没选出来，acked 集合会一直涨到超时，
//	 FailAckedAfter 就会被后面超时那段时间污染。"
//
// 三段必须分得清，而且账目要闭合：
//
//	before + during(切换中) + after(切换后) == total
//
// 无新 leader 时 after 必须是 -1（不适用），不能是一个被污染的数。
func TestFailAckAccounting(t *testing.T) {
	cases := []struct {
		name                    string
		total, before, switchAt int
		newLeader               bool
		wantDuring, wantAfter   int
	}{
		{
			name:   "选出新 leader：三段各就各位",
			total:  1000, before: 800, switchAt: 850, newLeader: true,
			wantDuring: 50, wantAfter: 150,
		},
		{
			name:   "切换点就等于杀点：没有切换中的确认",
			total:  500, before: 500, switchAt: 500, newLeader: true,
			wantDuring: 0, wantAfter: 0,
		},
		{
			// 这条是关键：没有新 leader 时，杀之后的确认数**全部**属于
			// "切换中"（濒死 leader 的 in-flight + 选举期间的零星确认），
			// after 必须是不适用，而不是把这个数当成"切换后仍能服务"。
			name:   "没选出新 leader：after 不适用",
			total:  900, before: 900, switchAt: -1, newLeader: false,
			wantDuring: 0, wantAfter: -1,
		},
		{
			name:   "没选出新 leader 且杀后仍有确认",
			total:  900, before: 850, switchAt: -1, newLeader: false,
			wantDuring: 50, wantAfter: -1,
		},
		{
			name:   "声称有新 leader 但切换点缺失（防御）",
			total:  900, before: 850, switchAt: -1, newLeader: true,
			wantDuring: 50, wantAfter: -1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			during, after, ok := failAckAccounting(c.total, c.before, c.switchAt, c.newLeader)
			if during != c.wantDuring || after != c.wantAfter {
				t.Fatalf("during=%d after=%d，期望 %d / %d", during, after, c.wantDuring, c.wantAfter)
			}
			if !ok {
				t.Fatalf("账目不闭合：before=%d during=%d after=%d total=%d",
					c.before, during, after, c.total)
			}
			// 账目恒等式：after 不适用（-1）时只核对前两段。
			sum := c.before + during
			if after >= 0 {
				sum += after
			}
			if sum != c.total {
				t.Fatalf("before+during+after = %d ≠ total %d", sum, c.total)
			}
		})
	}
}

// TestFailAckAccountingNeverAttributesTimeoutToSwitch 把评审那句话直接钉成断言：
// **没有新 leader 时，"杀之后"的确认数一个都不能算进 after。**
func TestFailAckAccountingNeverAttributesTimeoutToSwitch(t *testing.T) {
	// 模拟选举超时期间累积了 300 条确认（濒死 leader 的 in-flight + 零星确认）
	const total, before = 1300, 1000
	during, after, ok := failAckAccounting(total, before, -1, false)
	if !ok {
		t.Fatal("账目应闭合")
	}
	if after != -1 {
		t.Fatalf("无新 leader 时 after 必须是 -1（不适用），实际 %d —— "+
			"这个数会被读成「切换后仍能服务」", after)
	}
	if during != 300 {
		t.Fatalf("这 300 条应全部归入「切换中」，实际 during=%d", during)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
