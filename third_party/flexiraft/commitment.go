// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"sort"
	"sync"
)

// Commitment is used to advance the leader's commit index. The leader and
// replication goroutines report in newly written entries with match(), and
// this notifies on commitCh when the commit index has advanced.
type commitment struct {
	// protects matchIndexes and commitIndex
	sync.Mutex
	// notified when commitIndex increases
	commitCh chan struct{}
	// voter ID to log index: the server stores up through this log entry
	matchIndexes map[ServerID]uint64
	// a quorum stores up through this log entry. monotonically increases.
	commitIndex uint64
	// the first index of this leader's term: this needs to be replicated to a
	// majority of the cluster before this leader may mark anything committed
	// (per Raft's commitment rule)
	startIndex uint64

	// 【本 fork 新增】data-commit quorum 大小。
	//
	// 0 表示用多数（与上游 hashicorp/raft 行为一致）。
	// 设为 k+1 即 FlexiRaft 的 data-commit quorum。
	dataQuorumSize int
}

// newCommitment returns a commitment struct that notifies the provided
// channel when log entries have been committed. A new commitment struct is
// created each time this server becomes leader for a particular term.
// 'configuration' is the servers in the cluster.
// 'startIndex' is the first index created in this term (see
// its description above).
// 'dataQuorumSize' is the 【本 fork 新增】FlexiRaft data-commit quorum |Q2|.
// 传 0 表示用多数（与上游一致）。
func newCommitment(commitCh chan struct{}, configuration Configuration, startIndex uint64, dataQuorumSize int) *commitment {
	matchIndexes := make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			matchIndexes[server.ID] = 0
		}
	}
	return &commitment{
		commitCh:       commitCh,
		matchIndexes:   matchIndexes,
		commitIndex:    0,
		startIndex:     startIndex,
		dataQuorumSize: dataQuorumSize,
	}
}

// Called when a new cluster membership configuration is created: it will be
// used to determine commitment from now on. 'configuration' is the servers in
// the cluster.
func (c *commitment) setConfiguration(configuration Configuration) {
	c.Lock()
	defer c.Unlock()
	oldMatchIndexes := c.matchIndexes
	c.matchIndexes = make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			c.matchIndexes[server.ID] = oldMatchIndexes[server.ID] // defaults to 0
		}
	}
	c.recalculate()
}

// Called by leader after commitCh is notified
func (c *commitment) getCommitIndex() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.commitIndex
}

// Match is called once a server completes writing entries to disk: either the
// leader has written the new entry or a follower has replied to an
// AppendEntries RPC. The given server's disk agrees with this server's log up
// through the given index.
func (c *commitment) match(server ServerID, matchIndex uint64) {
	c.Lock()
	defer c.Unlock()
	if prev, hasVote := c.matchIndexes[server]; hasVote && matchIndex > prev {
		c.matchIndexes[server] = matchIndex
		c.recalculate()
	}
}

// Internal helper to calculate new commitIndex from matchIndexes.
// Must be called with lock held.
//
// ─────────────────────────────────────────────────────────────────────
// 【本 fork 的改动】FlexiRaft 的 data-commit quorum
//
// 上游 hashicorp/raft 把 commit 门槛硬编码为"多数"：
//
//	quorumMatchIndex := matched[(len(matched)-1)/2]
//
// 即取第 ⌈m/2⌉ 大的 matchIndex（m = 投票成员数）。
//
// FlexiRaft (Yadav & Rahut, CIDR 2023) 把它换成**可配置的 data-commit
// quorum**：只需 q2 个副本确认即可提交。
//
//	quorumMatchIndex := matched[len(matched)-q2]
//
// 关于写法差异：matched 升序排序。要"至少 q2 个副本达到索引 I"，
// I 应是第 q2 大的值，即下标 len(matched)-q2（0-based）。
// 校验与上游等价：m=5、q=3 时 len-3 = 2，而上游 (5-1)/2 = 2 —— 一致。
//
// 安全性：必须满足 |Q1| + |Q2| > N（Howard et al., OPODIS 2016 §4.2）。
// q2 变小必须由 Q1 变大补偿。本 fork **不校验**该约束 ——
// 由调用方（NewRaft 的配置）负责，因为 fork 内部无法得知 Q1 的取值。
// ─────────────────────────────────────────────────────────────────────
func (c *commitment) recalculate() {
	if len(c.matchIndexes) == 0 {
		return
	}

	matched := make([]uint64, 0, len(c.matchIndexes))
	for _, idx := range c.matchIndexes {
		matched = append(matched, idx)
	}
	sort.Sort(uint64Slice(matched))

	// 【改动】用可配置的 data quorum；<=0 或越界时回退到多数（与上游一致）。
	q := c.dataQuorumSize
	if q <= 0 || q > len(matched) {
		q = (len(matched)-1)/2 + 1
	}
	quorumMatchIndex := matched[len(matched)-q]

	if quorumMatchIndex > c.commitIndex && quorumMatchIndex >= c.startIndex {
		c.commitIndex = quorumMatchIndex
		asyncNotifyCh(c.commitCh)
	}
}

