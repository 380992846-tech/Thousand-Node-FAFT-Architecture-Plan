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

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
