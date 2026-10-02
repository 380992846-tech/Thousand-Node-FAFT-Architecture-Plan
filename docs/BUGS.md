# 缺陷清单与修复记录（BUGS.md）

本文档记录对原始代码库的缺陷审计结果。所有条目均给出**源码位置**、**为什么错**、
**怎么发现的**、以及**修复方式**。

审计方法：静态阅读 + 单元测试 + 基准测试 + 端到端集群运行。
凡标注「实测」的条目都有可复现的命令或数字支撑。

---

## 摘要

| 分类 | 数量 | 影响 |
|---|---|---|
| 数据竞争 / 并发错误 | 5 | 随机 panic、静默丢数据 |
| 逻辑错误（功能不工作） | 10 | 数据面缺失、路由不转发、成员变更失效 |
| 性能缺陷（数量级） | 8 | 单请求 O(n log n)、每读一次 fsync |
| 客户端正确性 | 2 | 栈溢出崩溃、静默返回错误数据 |
| 配置与部署错误 | 4 | 端口越界、指标全局注册冲突 |
| **分析方法与统计缺陷（第八节）** | **13** | **方向写反、重复计数、恒等式漏路径、结果不可复现、脚本漏包、聚合数无法下钻、进程静默卡死、PowerShell 陷阱、无证据推断、**"已实测"其实没数据**、判据自相矛盾** |
| **合计** | **42** | |

**编号说明（重要，别在答辩时被问倒）**：

- 带编号的条目是 `BUG-1` … `BUG-40`，但 **`BUG-18` 全文不存在**（编号断档），
  所以**编号条目实际只有 39 条**；
- 另有 **3 条未编号的「新发现」**（读路径 `Barrier`、ephemeral 端口、
  `start-cluster.sh`），它们是修复过程中被自己的测试/基准打脸才发现的；
- 39 + 3 = **42**。

> 本表曾错误地写作 27 条、且分项（5/8/6/4/4）与正文分节对不上
> （正文第二节 10 条而非 8、第三节 8 条而非 6，且正文有独立的"客户端正确性"一节、
> 并无"资源泄漏/死代码"一节）。已按正文实际数量修正。
>
> 第八节的 13 条（BUG-28 … BUG-40）不是运行时代码缺陷，而是**分析层**或**工具层**缺陷：
> 有人把下界算成了"低于真实可达值"的数，有人让汇总重复计数，有人写的恒等式
> 漏掉一条路径导致测试偶发失败，有人让解析结果无法逐位复现，有人让验证脚本
> 漏掉 5 个包却照样打印"通过"。
> 它们没有让任何进程崩溃，只会让**论文里的数字看起来更好**，或者让人查不清哪个数对 ——
> 这正是最需要写进清单的一类。

其中最严重的三条：

1. **BUG-16**：路由层从不转发请求。整个系统的读写路径实际不存在 ——
   而且它**不是崩溃、不是报错，是假装成功**，所以只测"HTTP 200"的用例全都会通过。
2. **BUG-1**：`FSM` 的读写用了两把不相干的锁 —— 并发读写 Go map，未定义行为。
   ✅ 这条**已由 race detector 独立验证**（2026-10）：21 个包 `go test -race` 全部通过，
   并做了正向对照确认检测器真的生效。见第六节与 [`MEASUREMENT.md`](MEASUREMENT.md) §4.3。
3. **BUG-21**：客户端在出错时无限递归，Leader 持续不可用时栈溢出崩溃。
   而 Leader 不可用是分布式系统的**常态**，不是异常。

---

## 一、数据竞争与并发错误

### BUG-1 · FSM 读写使用了错误的锁

**位置**：`pkg/raft/node.go`（原 `KVStore.Get`）

```go
// 原始代码
func (ks *KVStore) Get(key string) (string, bool) {
    ks.mu.RLock()          // ← 锁的是 KVStore.mu
    defer ks.mu.RUnlock()
    val, ok := ks.fsm.store[key]   // ← 读的是 FSM.store
    return val, ok
}
```

`KVStore.mu` 与 `FSM.mu` 是两把**互不相干**的锁。`FSM.Apply`
（写入方）持有 `FSM.mu`，而 `Get` 持有 `KVStore.mu` —— 二者之间没有任何
同步关系。并发读写 Go 的 map 是未定义行为，实践中表现为
`fatal error: concurrent map read and map write` 随机崩溃，
或读到撕裂的值。

**发现方式**：静态阅读。补 `TestFSMConcurrentReadWrite` 作为回归测试
（4 写 8 读并发，验证键数与计数完全正确）。

**修复**：`KVStore.Get` 委托给 `FSM.Get`，由 `FSM` 自己持有 `RWMutex`。
所有对 `store` 的访问都必须经过 `FSM` 的方法。

---

### BUG-2 · 死字段 `KVStore.store` 从未被写入

**位置**：`pkg/raft/node.go`

`KVStore` 里有一个 `store map[string]string` 字段，在 `NewKVStore` 里
初始化，之后**再也没有任何写入**。所有写入都进了 `fsm.store`。
任何基于 `ks.store` 的读取都会稳定返回空 —— 一个看起来能用、
实际永远返回零值的接口。

**修复**：删除该字段。数据只有一处真相（`FSM.store`）。

---

### BUG-8 · `MetricsCollector.shardID` 无锁读写且从未被赋值

**位置**：`pkg/metrics/metrics.go`

```go
// 原始代码
func NewMetricsCollector(nodeID string) *MetricsCollector {
    return &MetricsCollector{nodeID: nodeID}   // ← shardID 保持零值
}
func (m *MetricsCollector) SetShardID(shardID int) { m.shardID = shardID }
```

`SetShardID` **在整个代码库中从未被调用**（grep 零命中）。后果：

- 所有分片的指标都落在 `shard="0"` 标签上，1000 节点集群的监控完全无法归因；
- 即便被调用，`m.shardID` 也是无锁读写 —— 与 `RecordLatency` 并发时构成数据竞争。

**修复**：`shardID` 改为构造期传入并只读；另预计算 `shardLabel`
避免每次观测都做 `strconv.Itoa`。

---

### BUG-24 · 缺少 singleflight，冷启动时惊群

**位置**：`pkg/client/client.go`（原 `getRoute`）

路由缓存未命中时，**每个并发请求都会各自查一次元数据**。
进程刚启动（或缓存刚失效）时，N 个并发请求会产生 N 次元数据查询，
而它们本可以共享同一次结果。

**修复**：按分片做 singleflight —— 同一分片只允许一个 goroutine
去查元数据，其余等待其完成。

---

### BUG-25 · 固定 TTL 且无抖动，客户端集体失效

**位置**：`pkg/client/client.go`

原实现 TTL 固定 10 秒。所有客户端在同一时刻启动，因此会在
同一时刻集体失效，形成周期性惊群。

**修复**：TTL 叠加 `[0, TTLJitter)` 的随机抖动，打散失效时刻。

---

## 二、逻辑错误（功能不工作）

### BUG-3 · `Bootstrap()` 的条件判断反了，且不幂等

**位置**：`pkg/raft/node.go`

```go
// 原始代码
func (ks *KVStore) Bootstrap() error {
    if ks.raft.State() != raft.Follower && ks.raft.State() != raft.Candidate {
        return nil   // ← 已经是 Follower/Candidate 才继续？
    }
    ...
}
```

逻辑完全颠倒：节点刚启动时**总是** Follower，代码会继续往下走 ——
这一点碰巧是对的；但这句判断的真实意图（"只在未初始化时引导"）
完全没有被表达。更严重的是**不幂等**：进程重启后再次调用
`BootstrapCluster` 会返回 `ErrCantBootstrap`，导致节点无法启动。

**修复**：

- 用 `raft.HasExistingState(logStore, stable, snapshotStore)` 判断是否已有持久化状态；
- 已有状态时返回 `BootstrapResult{AlreadyInitialized: true}` 而非错误；
- 显式处理 `ErrCantBootstrap`。

**回归测试**：`TestBootstrapIdempotent`。

> **副作用发现**：修复过程中发现 `HasExistingState` 的第三个参数是
> `SnapshotStore` 而非 `NetworkTransport` —— 原设计里根本没有保存
> snapshot store 的引用，因此这个判断此前**无法实现**。

---

### BUG-4 · `Join()` 语义错误：在本地节点上调用 `AddVoter`

**位置**：`pkg/raft/node.go`（原 `Join`）

```go
// 原始代码
func (ks *KVStore) Join(leaderAddr string) error {
    ...
    future := ks.raft.AddVoter(raft.ServerID(ks.nodeID), raft.ServerAddress(ks.bindAddr), 0, 0)
    return future.Error()
}
```

两个错误：

1. **在错误的节点上执行**。`AddVoter` 是** Leader 的职责** —— 它是一次
   日志提案。在成员自己身上调用 `AddVoter`，只有当这个成员恰好是 Leader
   时才可能生效。新节点永远不是 Leader，所以这个调用永远不会成功。
2. **`leaderAddr` 参数被完全忽略**。函数签名收了这个参数，函数体里
   从未使用。
3. `AddVoter` 的第四个参数（timeout）传 `0`，会立即超时。

**修复**：

- `AddVoter` / `AddNonvoter` / `RemoveServer` 一律先检查 `State() == Leader`，
  非 Leader 返回 `ErrNotLeader`；
- 新增 `pkg/nodeapi` 的 `POST /join` 端点，由 Leader 处理入组；
- `cmd/kvstore-node` 的 `--join` 参数指向 **Leader 的数据面 HTTP 地址**。

**回归测试**：`TestNonLeaderRejectsWriteAndMembership`。

---

### BUG-5 · gob 编码：可被任意类型欺骗，且非确定性

**位置**：`pkg/raft/node.go`

原实现用 `encoding/gob` 编码 `Command`。问题：

1. **类型欺骗**。gob 流携带类型描述，解码方只做类型转换，不校验
   "这确实是一条 KV 命令"。任何合法的 gob 流都能被解码进来（可能产生
   零值命令）。
2. **非确定性**。gob 不保证同一输入产生同一字节序列，这对
   （快照校验、跨节点字节级比对）是不利的。
3. **体积与开销**。每条命令都带字段名与类型信息，而 struct 上的
   `json` tag 在 gob 路径下完全不生效（写了白写）。

**修复**：手写确定性二进制编码：

```
[1B op][4B CRC32C][4B klen][4B vlen][key][value]
```

- 解码器只接受本格式，校验长度前缀不超过 64 MiB；
- CRC 让被截断/损坏的日志条目在 `Apply` 阶段就被拒绝，
  而不是污染状态机。

**回归测试**：`TestDecodeRejectsGarbage`、`TestDecodeRejectsCorruptChecksum`、
`TestEncodeDeterministic`（200 次编码逐字节比对）。

---

### BUG-6 · 快照失败路径重复 `Close()`

**位置**：`pkg/raft/node.go`

```go
// 原始代码
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
    enc := gob.NewEncoder(sink)
    if err := enc.Encode(s.store); err != nil {
        sink.Cancel()
        return err
    }
    return sink.Close()
}
```

失败路径只调用了 `Cancel()`，**成功路径只调用 `Close()` 而不 `Cancel()`** ——
这在 hashicorp/raft 的契约下是对的，但原代码在另一处（未展示的变体）
同时调用了两者。修复为：失败只 `Cancel`，成功只 `Close`，且各自仅一次。

---

### BUG-10 · `GetShardForKey` 每次调用都排序，且不是二分查找

**位置**：`pkg/metadata/cluster.go`

```go
// 原始代码
func (ct *ClusterTopology) GetShardForKey(key string) *ShardInfo {
    ct.mu.RLock()
    defer ct.mu.RUnlock()

    // 按 StartKey 排序后二分查找     ← 注释这么说
    keys := make([]int, 0, len(ct.shards))
    for id := range ct.shards { keys = append(keys, id) }
    sort.Ints(keys)                    // ← 每次都排一遍

    for _, id := range keys {          // ← 而且其实是线性扫描
        shard := ct.shards[id]
        if key >= shard.StartKey && ... { return shard }
    }
    return nil
}
```

注释写着"二分查找"，实现是：

- **每次调用**都分配一个 1000 元素的切片；
- 对它做一次 `sort.Ints`（O(n log n)）；
- 然后**线性扫描**（O(n)），二分查找根本不存在。

在 1000 分片、每个请求一次路由查询的场景下，这是纯浪费。
更糟的是它发生在**读锁**下，所有并发请求串行争抢同一把锁。

**修复**：维护按 `StartKey` 升序的 `ordered []*ShardInfo` 索引，
在拓扑变更时重建（`reindexLocked`），查找用 `sort.Search` 真二分，O(log n)。

**性能佐证**：`BenchmarkLookup` 覆盖 16 / 200 / 1000 分片三档。

---

### BUG-11 · 排序依据错误：按分片 ID 而非 key 区间

**位置**：`pkg/metadata/cluster.go`

即使忽略 BUG-10 的性能问题，`sort.Ints(keys)` 排的是**分片 ID**，
而查找依据是 **key 区间**。二者顺序不一致时（ID 0 拥有最大的 key 区间，
ID 2 拥有最小的），函数会返回**错误的分片** —— 请求被路由到不拥有该 key 的节点。

**修复**：索引只按 `StartKey` 排序，`ID` 仅作为同 `StartKey` 时的稳定次序。

**回归测试**：`TestLookupCorrectWhenIDsDisagreeWithKeyOrder` 定向构造
"ID 顺序与 key 顺序完全相反"的拓扑。

---

### BUG-12 · watch 不处理 DELETE 事件，被删分片永久残留

**位置**：`pkg/metadata/cluster.go`（原 `watchTopology`）

```go
// 原始代码
for _, ev := range resp.Events {
    var shard ShardInfo
    if err := json.Unmarshal(ev.Kv.Value, &shard); err != nil {
        continue        // ← DELETE 事件的 Value 为空，unmarshal 失败，直接跳过
    }
    ct.shards[shard.ID] = &shard
    ...
}
```

DELETE 事件的 `Kv.Value` 是空的，`json.Unmarshal` 报错后 `continue`，
于是**分片合并/下线之后，它在缓存里永远存在**。所有落在那段 key 上的
请求都会被路由到一个已经不存在的分片。

**修复**：显式判断 `ev.Type == clientv3.EventTypeDelete`，从 `shards`
删除并广播 `delete` 事件，然后重建索引。

**回归测试**：`TestDeleteShardTakesEffect`。

---

### BUG-13 · watch 不更新 `nodeShardMap`

**位置**：`pkg/metadata/cluster.go`

`loadTopology` 会填充 `nodeShardMap`（nodeID → shardID），但
`watchTopology` 只更新 `shards`，从不更新 `nodeShardMap`。
运行一段时间后，节点归属查询会给出过期结果。

**修复**：把 `nodeShardMap` 的重建并入 `reindexLocked`，
任何拓扑变更都触发它。

---

### BUG-14 · 持锁期间做阻塞 etcd 写入

**位置**：`pkg/metadata/cluster.go`

```go
// 原始代码
func (ct *ClusterTopology) UpdateLeader(shardID int, leader string) error {
    ct.mu.Lock()
    defer ct.mu.Unlock()       // ← 全程持写锁
    ...
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    _, err = ct.etcdClient.Put(ctx, ..., string(data))   // ← 网络往返在锁内
    return err
}
```

把一次最长 3 秒的网络往返放在**全局写锁**内。锁竞争直接等于网络延迟，
在高并发下整个元数据层会被一个慢 etcd 拖死。

**修复**：网络调用移出锁外，只在最后更新内存状态时短暂持锁。

---

### BUG-15 · `RegisterShard` 创建了未使用的 etcd Session

**位置**：`pkg/metadata/cluster.go`

```go
// 原始代码
s, err := concurrency.NewSession(ct.etcdClient)   // 建了个租约会话
if err != nil { return err }
defer s.Close()
_, err = ct.etcdClient.Put(ctx, key, string(data))  // ← 但根本没用它
return err
```

`concurrency.Session` 被创建后完全没有被使用 —— 纯死代码，
而且注释声称"使用 etcd 事务防止并发写冲突"，实际是裸 `Put`，
没有任何 CAS 保护，多个写入方会互相覆盖。

**修复**：删除死代码；`UpdateLeader` 改用 `clientv3.Txn` 做版本检查。

---

## 三、性能缺陷

### BUG-16 · 路由层从不转发请求（最严重）

**位置**：`pkg/router/router.go`

```go
// 原始代码
func (r *Router) HandleRequest(w http.ResponseWriter, req *http.Request) {
    vars := mux.Vars(req)
    key := vars["key"]
    client, err := r.Route(key)
    if err != nil { http.Error(w, err.Error(), http.StatusNotFound); return }

    // 转发到Leader
    // 实际实现中，这里需要转发请求          ← 就是没实现
    w.WriteHeader(http.StatusOK)
    w.Write([]byte(fmt.Sprintf("Routed to shard %d leader %s", client.shardID, client.leaderAddr)))
}
```

**整个项目的读写路径实际不存在**。客户端 PUT 一个 key，会收到
HTTP 200 和一句 `"Routed to shard 0 leader 127.0.0.1:8000"` ——
看起来成功，实际什么都没发生。`initShardClients()` 同样是空函数体。

**修复**：实现真正的反向代理：

- 读取并限制请求体（8 MiB）；
- 按 Leader → 其余成员的顺序尝试，最多 `MaxAttempts` 次；
- 后端返回 503 时读取 `X-Raft-Leader` 提示并切换目标；
- 透传响应头与状态码。

---

### BUG-17 · 每次 Leader 变更都新建 `http.Client`

**位置**：`pkg/router/router.go`

```go
// 原始代码
if !ok || client.leaderAddr != shard.Leader {
    client = r.createShardClient(shard)   // ← 每次都 new 一个 http.Client
    ...
}
func (r *Router) createShardClient(shard *metadata.ShardInfo) *ShardClient {
    return &ShardClient{
        ...
        httpClient: &http.Client{Timeout: 5 * time.Second},  // ← 新的连接池
    }
}
```

`http.Client` 自带**独立的连接池**。每次 Leader 切换就换一个新池，
意味着所有 TCP 连接被丢弃重建。在 Leader 频繁切换时，旧连接进入
TIME_WAIT 堆积，最终耗尽临时端口。

此外，`client.leaderAddr` 在读时**没有持锁**，而写时有锁 —— 数据竞争。

**修复**：

- 每个分片复用一个 `shardClient`，其 `http.Client` 共享同一个 `Transport`；
- Leader 地址用 `atomic.Pointer[string]` 存储，无锁读写；
- `Transport` 全局共享一份，配置 `MaxIdleConnsPerHost=64` 保证长连接复用。

---

### BUG-19 · 无 key 校验

**位置**：`pkg/router/router.go`

空 key、超长 key 会一路传到后端。空 key 在 `mux` 下会匹配到
`{key}` 为空的分支，后端行为未定义。

**修复**：路由层校验 key 非空且不超过 `MaxKeyLen`（默认 1024）。

---

### BUG-20 · 多副本重试不存在

**位置**：`pkg/router/router.go`

Leader 切换的瞬间，旧 Leader 会返回错误，而路由层没有重试 ——
客户端收到 502 且不重试。

**修复**：实现后端候选列表重试（见 BUG-16 的修复）。
统计计数器 `retries` / `failures` 暴露在 `/stats`。

---

### BUG-23 · 路由缓存以单个 key 为键

**位置**：`pkg/client/client.go`

```go
// 原始代码
routeCache map[string]*RouteInfo
...
c.routeCache[key] = route
```

以**单个 key** 为键缓存路由。在 10 亿量级的 key 空间下：

- 缓存永远命不中（每个 key 只访问一次就再也不来）；
- 内存无限增长（每条缓存 10 秒 TTL，但 key 数量本身是无限的）。

正确做法是以**分片区间**为键 —— 区间数量等于分片数（几百到几千），
与 key 基数无关。

**修复**：`byShard map[int]*RouteInfo`，并加 `shardCacheMax` 容量保护。
`Stats()` 里新增 `CacheHits` / `CacheMisses` 便于观测命中率。

---

### BUG-26 · 路由缓存不随拓扑变更失效

**位置**：`pkg/client/client.go`

原实现只能等 TTL 过期。分片迁移/合并后，客户端会持续把请求
打到错误的节点长达 10 秒。

**修复**：订阅 `OnShardChange`，拓扑一变就删除对应分片的缓存条目。

---

### 新发现 · 读路径每请求一次 `Barrier`（实测 6.67ms）

**位置**：`pkg/nodeapi/server.go`（本仓库新增代码，被自己的基准打脸）

线性化读的最简实现是每次读都调 `raft.Barrier()`。实测：

```
BenchmarkGetRaw                 578 ns/op
BenchmarkBarrier          6,672,332 ns/op     ← 慢 11500 倍
BenchmarkSet              5,447,163 ns/op
BenchmarkBarrierVsCommitTimeout/commit=1ms   5,721,181 ns/op
BenchmarkBarrierVsCommitTimeout/commit=5ms   6,456,209 ns/op
BenchmarkBarrierVsCommitTimeout/commit=20ms  5,872,965 ns/op
```

`CommitTimeout` 从 1ms 调到 20ms，数字纹丝不动 —— 说明瓶颈不在
提交超时，而在 **fsync**：`Barrier` 会向日志追加一条空条目并等待落盘。

用本项目自己的成本模型复算：`b=1, t_fsync=5.4ms → 185 ops/s`，
与实测 `BenchmarkSet` 的 ~184 ops/s 一致。

**后果**：端到端实测读延迟 `p50 = 39.91ms`，全部是攒出来的。

**修复**：实现 Raft §6.4 的 readIndex 语义（`ReadBarrier`）。
Leader 只要在本任期提交过一条日志，其 commit index 即为当时全集群
最大值，可直接在该索引上服务读，**无需任何额外网络或磁盘往返**。
换主后任期编号变化 → 缓存自动失效。

**实测效果**（2 分片 × 3 副本，32 并发，读占比 0.9，5s）：

| 指标 | 修复前 | 修复后 |
|---|---|---|
| 吞吐 | 396 ops/s | **2,014 ops/s** |
| get p50 | 39.13 ms | **4.09 ms** |
| get p99 | 63 ms | 33.40 ms |
| put p50 | 39.91 ms | 99.24 ms（读变快后写排队更明显） |

读延迟约降 **10 倍**。写路径仍受单批 fsync 限制，见 `ROADMAP.md`。

---

### 新发现 · ephemeral 端口被 advertise 永久遮住

**位置**：`pkg/raft/node.go`

```go
// 修复前的写法
addr, _ := net.ResolveTCPAddr("tcp", ks.cfg.BindAddr)   // "127.0.0.1:0"
transport, _ := raft.NewTCPTransport(ks.cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
//                                                    ^^^^ 当 advertise 传进去了
```

`hraft.TCPStreamLayer.Addr()` 的实现是：

```go
func (t *TCPStreamLayer) Addr() net.Addr {
    if t.advertise != nil { return t.advertise }   // ← 优先返回 advertise
    return t.listener.Addr()
}
```

`net.ResolveTCPAddr("tcp", "127.0.0.1:0")` 返回的不是 nil，
于是 **`LocalAddr()` 永远返回 `"127.0.0.1:0"`，真实监听端口被永久遮住**。
三个用 `:0` 的节点都对外声称自己是 `127.0.0.1:0`，raft 以

```
found duplicate address in configuration: 127.0.0.1:0
```

拒绝成员加入。

**发现方式**：`TestThreeNodeReplication` 挂死/失败。生产环境用固定端口
时症状完全隐藏 —— 这正是它值得记录的**理由**。

**修复**：仅当配置了非零端口时才传 advertise，否则传 `nil` 让
listener 地址暴露出来；新增 `AdvertiseAddr()` 并在
`Bootstrap` / `AddVoter` / `AddNonvoter` 中一律使用真实地址。

**回归测试**：`TestAdvertiseAddrResolvesEphemeralPort`。

---

## 四、客户端正确性

### BUG-21 · 出错时无限递归（会栈溢出崩溃）

**位置**：`pkg/client/client.go`

```go
// 原始代码 —— Get / Set / Delete 三处同样写法
resp, err := c.httpClient.Do(req)
if err != nil {
    c.invalidateRoute(key)
    return c.Get(ctx, key)   // ← 递归，无上限、无退避
}
```

如果 Leader 持续不可用（或 key 根本无归属），每次重试都失败，
递归永不收敛。结果**不是返回错误，而是栈溢出崩溃** ——
一个本应优雅降级为错误返回的路径，让整个客户端进程挂掉。

**修复**：改为有界循环（`MaxAttempts` 默认 4），配合指数退避
（25ms 起，上限 400ms，响应 `ctx.Done()`）。

---

### BUG-22 · URL 拼接不做转义

**位置**：`pkg/client/client.go`

```go
// 原始代码
url := fmt.Sprintf("http://%s/api/v1/key/%s", route.LeaderAddr, key)
```

key 中含 `/` 时会被后端当成路径分隔符：
`key = "a/b"` 变成 `/api/v1/key/a/b`，`mux` 的 `{key}` 只匹配到 `a` ——
**请求被路由到另一个 key**，静默返回错误数据。
含 `?`、`#`、`%` 的 key 会截断或破坏 URL。

**修复**：`url.PathEscape(key)`；路由层用 `{key...}` 捕获并正确解码。

---

## 五、配置与部署

### BUG-7 · metrics 端口默认固定 `:9100`，千节点下必然冲突

**位置**：`cmd/kvstore-node/main.go`

```go
metricsEP = flag.String("metrics-addr", ":9100", "Prometheus metrics 监听地址")
```

所有节点默认绑定同一端口。1000 节点场景下只有第一个能绑上，
其余**静默失败**（`http.ListenAndServe` 的错误只被 `log.Printf` 记录）。

**修复**：默认改为空（不启用），由调用方显式指定；
`ListenAndServe` 返回错误而非吞掉。

---

### BUG-9 · `promauto` 全局注册：同进程第二个节点即 panic

**位置**：`pkg/metrics/metrics.go`

```go
// 原始代码
var operationsTotal = promauto.NewCounterVec(...)   // ← 全局默认 registry
```

`promauto` 注册到 `prometheus.DefaultRegisterer`。同进程内创建第二个
`MetricsCollector` 时，`MustRegister` 因重复注册而 **panic**。

这直接封死了"单进程多分片"实验路径 —— 而这正是做参数扫描
（分片数 × 副本数）所必需的。

**修复**：每个 `Collector` 持有独立的 `prometheus.Registry`，
通过 `Registry()` 暴露给 `/metrics`。

---

### BUG-27 · 脚本端口分配越界

**位置**：`scripts/start-cluster.sh`

```bash
port=$((DATA_BASE_PORT + shard * 10 + node))    # 8000 + shard*10 + node
```

- 每增加一个分片吃掉 **10** 个端口，但实际只用 5 个（浪费一半）；
- `shard=200` 时算出 `8000 + 2000 + 4 = 10004`，**超出合法端口范围**。

**修复**：`pkg/cluster` 改为线性分配 `BasePort + shard*replicas + i`，
并在启动时通过 `net.Listen` 真实校验端口可用性。

---

### 新发现 · `scripts/start-cluster.sh` 无法复现任何实验

**位置**：`scripts/start-cluster.sh`

脚本为 200×5 = 1000 个节点各起一个 OS 进程，`mkdir` 1000 个目录，
绑定 1000 个端口，再用 `sleep 5` 赌集群就绪。

- 没有任何就绪探测 —— `sleep 5` 之后集群可能还没选主完成；
- 没有失败处理 —— 某个节点起不来，实验照跑，数据照出；
- 没有清理逻辑 —— 中断后残留 1000 个进程；
- 无法归因 —— 测量结果里混着 OS 调度、端口竞争、页面缓存抖动。

**修复**：新增 `pkg/cluster` + `cmd/kvbench`，在**单进程**内起
任意规模的分片集群，等待所有分片选出 Leader，输出带完整环境元数据
的结果文件。同一条命令 + 同一个种子 => 同一份结果。

---

## 六、验证状态与真实缺口

### 已验证

```
go build -p 1 ./...     通过
go vet   -p 1 ./...     通过
go test  -p 1 ./...     通过  (pkg/faft 4.6s, pkg/raft 10.1s, pkg/metadata 0.6s)
go test -p 1 -list '.*' ./...   → 191 个 Test + 11 个 Benchmark
powershell -File build.ps1 race → 21 个包全部通过（含 third_party/flexiraft）
```

> ⚠️ **不要把 191 读成"191 个逐位复现的测试"**。
> `-list` 只数顶层 `Test*` 函数名，不含子测试，也不含任何"确定性"含义。
> 早期文档里出现过"113 个同种子逐位复现的测试"的说法 —— **那个数字不存在**：
> 当时 `-list` 实际给出 173 个 Test，其中与确定性有关的只有
> `TestWorkloadDeterministic`、`TestEncodeDeterministic` 两个，
> 外加一个负向对照。"同种子 ⇒ 同结果"是 `cmd/kvbench` 的**工作负载**
> 性质，不是测试性质；而性能测量**不是**逐位可复现的
> （同一配置 5 轮实测 72k–184k ops/s，见 `MEASUREMENT.md`）。
>
> 修掉 BUG-32 之后需要补一句：**解析结果**（可用性、消息数、下界）现在是
> 逐位可复现的；**测量结果**仍然不是。两者口径必须分开说。

测试覆盖：

- 命令编解码往返 / 确定性 / CRC 篡改检测
- 快照往返 / bad magic / 截断快照不 panic
- 并发读写 FSM（BUG-1 回归，**并已由 race detector 独立验证**）
- 二分查找对全键空间与暴力查找逐一对照（16 分片 × 20000 随机 key）
- 删除分片真正生效 + 事件广播（BUG-12 回归）
- 三副本复制一致性（200 键 × 3 副本逐键比对）
- Leader 硬停后重新选主（实测 ~2.95s）
- 幂等 Bootstrap（BUG-3 回归）
- 非 Leader 拒绝写与成员变更（BUG-4 回归）
- ephemeral 端口解析（新缺陷回归）
- 下界的**穷举守门**：对"放置 × 成员子集 × quorum 大小"三者一起穷举求真实最优，
  断言 `下界 ≤ 真实最优`；以及上界的逐点校验（BUG-28 回归）

### 未验证（诚实记录）

1. ~~**`-race` 未执行**~~ → ✅ **2026-10 已补上**。
   装 MinGW-w64（`winget install BrechtSanders.WinLibs.POSIX.UCRT`）后
   `build.ps1 race` 覆盖全部 21 个包通过，**BUG-1 的修复从此有 race detector
   的独立验证**。并且做了**正向对照**（故意制造数据竞争，确认检测器真会报
   `WARNING: DATA RACE`）—— 没有这一步，"race 通过"无法与"检测器没生效"区分。
   见 [`MEASUREMENT.md`](MEASUREMENT.md) §4.3 与 BUG-34。

2. **etcd 集成路径未跑通**。`pkg/metadata` 的单元测试全部基于
   `LocalTopology`（进程内）。`ClusterTopology` 的 watcher、事务写入、
   `Ping` 路径只有静态审查，**没有真实 etcd 端到端验证**。
   `docs/etcd-cluster.md` 描述的部署方式未经本轮验证。

3. **`scripts/*.sh` 未在 Windows 上执行**。这些脚本是 bash，
   本机 PowerShell 环境未验证其可运行性。

4. **1000 节点规模未实测**。`pkg/model` 给出的是**解析预测**，
   端到端实测停在 2 分片 × 3 副本 = 6 副本。
   大规模结论必须有实测支撑才可写入论文（见 `DESIGN.md` 的实验计划）。

5. ~~**写路径未优化**~~ → ✅ 已实现批处理，实测 `put p50` 99.24ms → 17.72ms。

6. **算力天花板**：联合求解 n=21 单次 70s，按趋势 **n=1000 的联合求解不可行**。
   论文里 n=1000 的数字必须标明是解析外推（见 BUG-31）。

---

## 七、尚未修复的已知问题

按优先级排列，供后续迭代：

| 优先级 | 问题 | 位置 | 说明 |
|---|---|---|---|
| 高 | 写路径无批处理 | `pkg/raft` | 实测 put p50 99ms，受 fsync 主导 |
| 高 | 无分片分裂/合并 | — | `SplitKeySpace` 只在启动时静态切分 |
| 中 | 无 Leader 均衡 | — | 分片 Leader 分布靠随机选举 |
| 中 | 无热点检测 | — | TiKV 的 `balance-hot-region` 无对应实现 |
| 中 | 无 read-index 批量合并 | `pkg/nodeapi` | 当前每个读 goroutine 一次原子读 |
| 低 | `stats` 计数器未接入 Prometheus | `pkg/router` | 只有 JSON 端点 |
| 低 | 无优雅的成员下线流程 | `pkg/nodeapi` | 只有 `AddVoter`，无 drain |

---

## 八、分析方法与统计缺陷（本轮新增）

这一节的缺陷**不在运行时**，而在"从数字得出什么结论"这一步。
它们的共同特征：不会崩溃、不会报错、测试全绿，只会让结论朝**有利于自己**的方向偏。

### BUG-28 · 下界的松弛写反了方向，上界被真实值击穿

**位置**：`pkg/faft/lowerbound.go`（`AvailabilityUpperBound` 的松弛 ④）

**为什么错**：求下界要用可用性的**上界**。初版把松弛 ④ 写成
"quorum 成员尽可能摊开、每个域一个成员"，注释里的理由是
"区域事件整域打掉，摊开时每命中一个域只损失一个成员"。

**这个理由是反的。** 区域事件按域打掉副本时：

- 成员**堆在一个域**里 ⇒ 只需"这个域不被命中"；
- 成员**摊开到 m 个域** ⇒ 需要"这 m 个域**全部**不被命中"。

对 `need = ⌊q/2⌋+1` 这种"多数存活"判据，后者严格更难。
正确的做法是对成员数的**所有分拆**取可用性最大值 —— 堆叠与摊开都在候选集里，
谁高取谁。

**怎么发现的**：守门测试 `TestUpperBoundIsSound` 对随机拓扑 × 随机放置 ×
每个 quorum 大小逐点验证 `真实可用性 ≤ 上界`，第一次运行就报：

```
上界不成立：D=4 n=7 pBase=1.57e-04 q=6.74e-02 k=3  放置=[0 0 0 2 3 2 0] 大小=2
  真实可用性 = 0.945006039770
  上界       = 0.932572398809
  真实值超出上界 1.243e-02
```

`D=4, K=3` 打掉 3 个域时，两个成员堆在同一个域里的真实可用性是
`(1/4)·a²`，而"摊开"版本给出 `0`。

**后果（如果没被抓住）**：报出去的会是"我们的解**低于**理论最优"——
一个自相矛盾的结论。不会有人相信，但会浪费一轮审稿。

**修复**：`exactEventMax` 枚举 q 的全部整数分拆，对每个分拆用背包 DP
算出"命中 j 个已占用域、成员数之和为 s"的组合数，按
`survivalProbability(q-s, need, pMin)` 加权求和后取最大值；
`D > 12`（拓扑用轮转采样、与组合枚举不再等价）时改用与命中集合无关的解析界
`1 - H_min/D`，其中 `H_min = ⌈(K·q - D·tol)/need⌉` 由
`Σ_i m_i = K·q`、`m_i ≤ q` 推出。

**回归测试**：`TestUpperBoundIsSound`（90 个随机拓扑 × 2 组 k × 6 个放置 × 全部 q，
含 `D > 12` 与 `k = D` 分支）、
`TestUpperBoundMonotoneInEventSeverity`、
`TestLowerBoundSoundAgainstBruteForce`（对**放置 × 成员子集 × quorum 大小**
三者一起穷举求真实最优，断言 `下界 ≤ 真实最优`）。

> 顺带纠正另一个错误断言：原来还有个 `TestUpperBoundMonotoneInQ`，
> 声称"quorum 越大可用性越高"。**这也是错的**：`q=1` 只需 1 个存活，
> `q=2` 需要 2 个都存活，真实可用性本身就会下降。
> 已换成"事件越严重（K 越大）上界越低"——这条可以证明（φ 单调不增 +
> 对 K+1 子集逐点占优取期望），并用 `TestUpperBoundTracksNonMonotoneInQ`
> 把"非单调"这件事钉住，防止有人再把它当 bug 去"修"。

### BUG-29 · 两个 baseline 同名，汇总统计重复计数

**位置**：`pkg/faft/planner.go`（`FlexiRaftPlanner.Name()`）

**为什么错**：`DefaultBaselines()` 里有两个 `FlexiRaftPlanner`
（默认 `max(2, n/4)` 与钉死 `DataQuorum: 2`），但 `Name()` 对两者都返回
`"flexiraft"`。结果表出现两列同名，按名字聚合的统计把每个参数点数了两遍。

**怎么发现的**：下界对照表的汇总里出现 `未达可达目标 = 72`，
而"下界可行的参数点"只有 36 个 —— 一个方法不可能在 72 个点上未达标。
数字**超过了它的定义域**，这是发现重复计数最快的信号。

**修复**：钉死 `DataQuorum` 的实例返回 `flexiraft-dq2`；
`TestDefaultBaselinesComplete`（函数名本来就写着"命名唯一"，此前只对
`majority` 断言过唯一性）补上全集合唯一性断言。

**教训**：`36 个点` 出现 `72` 这种"指标超过定义域"的数，和早先
"可用性 80.5% 但只有 40% 的分片满足条件"属于同一类错误
（见 `MEASUREMENT.md` 的算术交叉校验一节）—— 汇总数字必须能用手算复核。

### BUG-30 · 批处理分类账恒等式漏了"超时单条"路径 ⇒ 测试偶发失败

**位置**：`pkg/raft/batch_test.go`（`TestBatchAmortization`）、`pkg/raft/batch.go`

**为什么错**：测试里的恒等式写的是

```
Enqueued + Direct == FSMCommands + Released
```

但 `Direct` 把**两条性质相反**的路径合在了一起：

| 路径 | 含义 | 是否计入 `Enqueued` |
|---|---|---|
| 退化路径 | 批循环没跑（当选 Leader 后头几十毫秒）/ 已停止 / 批处理关闭 | **否** |
| 超时路径 | 入了队，但等满 `BatchMaxWait` 后被摘出来单条提案 | **是** |

超时路径的命令**既**入过队、**又**走了单条提案，所以把它加在 `Enqueued` 那一侧
就重复计数了。`BatchMaxWait=2ms` 时这条路径频繁触发。

**怎么发现的**：连续 5 次跑同一条命令，**2 次通过 3 次失败**，失败信息是

```
命令去向对不上：入队 640 + 单条 40 != FSM 应用 640 + 释放 0
```

关键判据是 `Enqueued + Direct = 680 > 总数 640`：**计数超过了它的定义域**。
这看起来像"偶发竞态"，实际是恒等式本身不成立 —— 而且它失败与否取决于机器负载，
所以在安静的时候会一直"通过"，直到某天在别人机器上炸掉。

**修复**：`BatchStats` 把两条路径拆成 `DirectTimeout` 与 `DirectDegraded`
（`Direct` 保留为两者之和，兼容既有调用方），并把正确的分类账写进注释：

```
Enqueued + DirectDegraded == FSMCommands + Released
Flushed  + DirectTimeout  == Enqueued - Released
```

**验证**：修前 5 次运行 2 次通过；修后连续 8 次全通过。

**教训**：一个"偶发失败"的测试，先怀疑**断言本身写错了**，再怀疑代码有竞态。
判据还是那条：**计数不能超过它的定义域**。

### BUG-31 · 成员选择是 O(n³)：单次求解在 n=21 时要 561 秒

**位置**：`pkg/faft/solve_failure.go`（`SolveWithFailures` 的成员选择循环）

**为什么错**：`pickMaxAvailability` 每次调用都**从空集重新贪心**，
复杂度 O(k·n·cost(可用性))。而 `SolveWithFailures` 要对每个 `|Q2|=k` 求一次、
再对每个 `|Q1|` 求一次，合计 **O(n³·cost)** 次可用性计算。

**怎么发现的**：`faftbench lowerbound` 一条命令跑了 **12 分钟**才结束。
按进程 CPU 占用（437 s 仍在自旋）判断不是死锁而是算不完，
逐段计时后定位到 n=21 的一次联合求解：

| n | 单次联合求解耗时 |
|---|---|
| 5 | 78 ms |
| 9 | 274 ms |
| 11 | 973 ms |
| **21** | **561 193 ms（9.4 分钟）** |

**修复**：改成**嵌套贪心**（`availabilityLadder`）—— 贪心准则不依赖 k，
所以逐 k 贪心的结果恰好是嵌套贪心序列的前 k 项。一次 O(n²) 算出全部大小的
成员集合与可用性，**严格等价**（用两份结果 JSON 逐字段对照：离散字段差异 0）。
n=21 单次求解 561 s → **70 s**，整条命令 741 s → **52 s**（快 14×）。

**回归测试**：`ladder_test.go` 的
`TestAvailabilityLadderMatchesPerKGreedy`（随机拓扑 × 每个 k，逐位比对成员集合与可用性）
与 `TestSolveWithFailuresUnaffectedByLadder`（新旧两条路径必须给出同一个 `FailurePlan`）。

**仍然存在的缺口（写进论文的局限）**：n=21 单次联合求解 70 s，
按这个增长趋势 n=1000 的**联合求解不可行** —— 论文里 n=1000 的数字来自
`AnalyzeScale` 的解析外推，不是联合求解器跑出来的（见 `MEASUREMENT.md` §7）。

### BUG-32 · 可用性计算遍历 map ⇒ 解析结果无法逐位复现

**位置**：`pkg/faft/solve_failure.go`（`availabilityForKilledSet` 的域卷积）

**为什么错**：卷积循环写成 `for d, c := range perDomain`，而 Go 的 map
遍历顺序是**随机的**。浮点加法不满足结合律，于是**同一个成员集合两次调用**
会给出最后几位不同的结果：

```
0.99977499514985479   vs   0.99977499514985491
```

**后果**：这不改变任何结论（差 1e-16），但它让"同种子 ⇒ 同结果"这句话
在**解析结果**上都做不到。论文里的数字是要被人拿去复算的，复算对不上
却找不到原因，比数字本身错更糟。它是 `WALKTHROUGH.md` §3.3
"测量结果不是逐位可复现"的一个额外来源 —— 而且这一处**是可以修的**。

**修复**：按域下标 0..D-1 顺序卷积（同时省掉一次 map 哈希）。
`TestMemberAvailabilityIsBitReproducible` 连打 200 次比对逐位相等。

> 顺带说明口径：修完之后，**解析结果（`pkg/faft`）是逐位可复现的**；
> **测量结果（吞吐、延迟分位）仍然不是**，那是运行环境决定的，见 `MEASUREMENT.md` §4.1。

### BUG-33 · 自引入：贪心阶梯复用了 trial 底层数组，评估的是"陈旧前缀"

**位置**：`pkg/faft/solve_failure.go`（`buildAvailabilityLadder`，BUG-31 的修复代码）

**为什么错**：内层循环原本写成

```go
trial = append(trial[:len(l.order)], cand)   // ❌
```

`trial` 的底层数组里装的是**上一轮最后一个候选**，不是已选中的成员。
于是每一轮都在评估"错误集合"的可用性：贪心的**顺序看起来正常**，
可用性却系统性偏低（实测同一集合 0.99847 vs 正确值 0.99943）。

**怎么发现的**：写这次优化时同时写了等价性测试，
第一次运行就报 `k=2：成员集合不同`，以及
`TestAblationJointNeverWorse` 报"A4 输给 A2"。
如果没有那条等价性测试，这个 bug 会安静地改变**所有**联合求解结果 ——
而且方向是让方法**变差**，所以不会有人怀疑"数字变好了"。

**修复**：显式重建前缀 `trial = append(trial[:0], l.order...)` 后再 append 候选。

**它没有进入任何结果**：这条缺陷在提交前就被测试拦下，
`results/` 与文档里的数字都来自修复后的版本（已用同一次运行的 JSON 对照确认）。

**教训**：这与早先 `cmd/raftbench` 的 payload buffer 复用是**同一类**缺陷
（复用底层数组 ⇒ 静默改写内容），症状也一样 —— **计数/集合看起来对，值不对**。
优化一定要配一条"新旧实现必须给出同一个结果"的等价性测试。

### BUG-34 · 验证脚本的包清单漏了 5 个包，"全部通过"是虚的

**位置**：`build.ps1`（`$allPkgs`）

**为什么错**：包清单是**显式列出**的 15 个，注释理由是
"避免 `./...` 在某些 Go 版本上的解析差异"。那个理由早就站不住了
（Go 1.24 上 `./...` 一直正常），而代价很实在：仓库后来又加了 4 个包，
清单没跟着更新，于是这 5 个包**长期没被 `build` / `vet` / `test` / `race` 覆盖**：

| 漏掉的包 | 测试数 |
|---|---|
| `pkg/sim` | 17 |
| `cmd/faultagg` | 7 |
| `cmd/faultfit` | 6 |
| `cmd/raftbench` | 5 |
| `third_party/flexiraft` | 6 |
| **合计** | **41 / 191** |

**怎么发现的**：装完 gcc 跑 `build.ps1 race` 时数了输出里的包 —— 只有 15 行，
而 `go test ./...` 会报 21 个包。两个数字对不上，就是漏了。

**这件事比 `-race` 本身更重要**：脚本会照常打印"通过"，
所以"测试全过"这句话之前是**虚的**；而其中 `third_party/flexiraft` 正是
论文基线测量所驱动的代码（hashicorp/raft 的 FPaxos 改造版），
`pkg/sim` 是模拟器 —— 它们是**最需要**被并发检测覆盖的两个包。

**修复**：`$allPkgs = @('./...')`，以后新增包自动纳入。
重跑 `build.ps1 race`：**21 个包全部通过**。

**教训**：验证脚本自己也必须被验证。"通过了"与"检查了哪些东西"是两个问题，
后者要有可核对的数字（这里就是"15 vs 21"）。
同类教训见 BUG-29 / BUG-30：**用手算复核汇总数字**。

### BUG-35 · 只报一个 `errors=N`，塌陷时看不出是谁的错

**位置**：`pkg/bench/runner.go`（错误计数）

**为什么错**：压测结果里错误只有一个聚合数字 `errors` 与 `error_rate`。
但错误来自三处完全不同的地方，含义也完全不同：

| 来源 | 含义 |
|---|---|
| 客户端 500ms **拨号超时** | **压测端扛不住**，不是集群的问题 |
| HTTP 503「非 Leader」 | 路由/重试路径的问题 |
| 连接被拒 | 进程/端口的问题 |

混成一个数字，最坏的结果是把"压测客户端到极限了"读成"集群变慢了"。

**怎么发现的**：做 §5.10 的分片扩展时，单分片档位出现了一次
**305 ops/s / 43.9% 错误**的塌陷，而同参数此前此后各跑 5–7 次都是
1,800–6,200 ops/s / 0 错误。当时手上有 `errors=983`，**没有任何办法判断
是哪一类错误** —— 也就无法区分"集群塌了"与"客户端超时级联"。
（后续 7 次都没能复现，所以这条至今没有定性结论；能定性的是：
当时的输出不足以定性。）

**修复**：`Result` 新增 `error_classes`（错误文本 → 次数），
文本做归一化（`127.0.0.1:18001` → `127.0.0.1:PORT`，`after 500ms` → `after DURATION`），
并加 64 类上限防止 map 无限增长。分类键不归一化会因为端口不同而爆表，
反而失去"一眼看出是哪类错"的意义。

**同批加的两件事**（都属于"让结论可核对"）：

1. `scripts/run-kvbench-shards.ps1` 丢弃**错误率 > 1%** 的轮次并重跑，
   在表里报「轮次(重跑/丢弃)」。理由：那种轮次的吞吐被客户端超时压低了
   一个数量级，混进中位数会得出错误的扩展性结论。
2. 结果 JSON 增加 `leader_distribution` / `leader_max` / `leader_mean`。
   ⚠️ 但在单进程装置里它恒为 1/1（每个 Raft 成员有自己的 NodeID），
   这一点写进了 `MEASUREMENT.md` §5.10 —— **不能拿它当均衡性证据**。

**教训**：**聚合指标必须能下钻**。"错误 983 次"不是信息；
"客户端拨号超时 983 次"才是。这条与 BUG-28/29/30 是同一族：
数字本身没错，错在**它不足以支撑你要下的结论**。

### BUG-36 · 压测进程结果写完却不退出，脚本静默卡死 24 分钟

**位置**：`cmd/kvbench`（退出路径）

**为什么错**：实测出现过 `kvbench` **已经把结果 JSON 与摘要写完、进程却不退出**的情况。
脚本用 `& $Bin ...` 等子进程结束，于是**整组实验静默卡死**：
日志里连一行提示都没有，看起来像"机器慢"，实际是进程挂在清理阶段。

**怎么发现的**：一组 c=128 的扫描在一档上停了 **24 分钟**。
按进程状态判断：CPU 200s 且几乎不涨 ⇒ 阻塞而不是算不完；
再看该轮日志，**结果早就写完了**。

**修复**：`kvbench` 增加退出看门狗 —— 测量结束后起定时器，
超过 `-exit-grace`（默认 30s）仍未退出，就打印**全部 goroutine 栈**并以 **exit 9** 退出：

```go
func startExitWatchdog(grace time.Duration)   // 正常路径下它根本不会醒
```

**为什么必须打印 goroutine 栈**：这类挂起唯一的定位线索就是栈。
宁可刷屏也不能省 —— 下次复现时能直接指出卡在哪一步。

**教训**：压测驱动不能有"卡住但不报错"的失效模式。
**卡住必须响亮地失败，并留下定位信息** —— 否则一整夜的实验会静默作废。

### BUG-37 · PowerShell：`$ErrorActionPreference='Stop'` + `*>` ⇒ 子进程写一行 stderr 就终止整个脚本

**位置**：`scripts/run-kvbench-ceiling.ps1`、`scripts/run-kvbench-shards.ps1`

**为什么错**：脚本开头设了 `$ErrorActionPreference = 'Stop'`（本意是别忽略错误），
而 `& $Bin ... *> $log` 会把子进程的 stderr **合并成 ErrorRecord**。
两者相遇的结果是：**子进程只要往 stderr 写一行，整个脚本就被终止**。

**怎么发现的**：c=128 的一组扫描跑到 16x3 就整体退出，**表都没打出来**，
stderr 只有一句 `kvbench.exe : `（消息体是空的）。
剩下的 32x3 一档根本没跑，看输出却像"跑完了"。

**修复**：调用原生程序期间临时放开 EAP，拿到退出码后再收回来：

```powershell
$ErrorActionPreference = 'Continue'
& $Bin ... *> $log
$childExit = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
if ($childExit -ne 0) { ... }   # 退出码 9 = 看门狗触发，日志里有 goroutine 栈
```

**教训**：这是本项目踩到的第二个 PowerShell 陷阱（第一个是
`[int]3.5 = 4` 的银行家舍入，见 `ROADMAP.md`）。
共同点：**失败方式安静且方向可疑** —— 一个把 n=7 的多数 quorum 算错，
一个让"少跑了一档"看起来像"跑完了"。

### BUG-38 · 从"所有方法都达到下界"推断"下界太松"—— 一次没有证据的推断

**位置**：`docs/MEASUREMENT.md` §5.9（第一版结论）、`cmd/faftbench/lowerbound.go` 的说明文字

**为什么错**：下界对照表显示 36 个可行点里有 **24 个"所有可行方法都已达到下界"**。
当时据此写下："那说明**下界在这些点上不够紧**（松弛 ① 忽略 Q1/Q2 耦合是最主要的原因）。"

**这个推断没有证据。** "所有方法都达到下界"有两种完全不同的解释：

| 解释 | 含义 |
|---|---|
| ① 界太松 | 下界低于真实最优，所以随便什么方法都能达到 |
| ② **参数点不区分方法** | 下界**紧**，但约束把每个方法都顶到了同一个最优值 |

两者在"多少个点达到下界"这个数字上**完全一样**，光看那张表分不出来。

**怎么发现的**：主配置（9 域 × 5 副本）的放置空间是 **9^5 = 59,049**，穷举得动。
跑了 `faftbench exhaustive`（48 个点 × 59,049 个放置）：

```
下界 == 真实最优：36 / 36（平均差距 0.00 条消息）
贪心 == 穷举最优：46 / 46 个存在可行最优解的点
```

**下界是紧的** —— 解释 ② 才对。第一版把结论说反了。

**修复**：

1. `MEASUREMENT.md` §5.9 改为"**参数点不区分方法**"，并新增 §5.11 穷举复核一节；
2. `faftbench lowerbound` 的输出直接印上这句话（避免只看这一张表的人再误读）；
3. `faftbench exhaustive` 把"贪心劣于最优"与"无解点上兜底方案不同"**分开计数** ——
   第一版把后者也算成缺口，报出"2 个点贪心劣于最优"，而那 2 个点**本来就无解**。

### BUG-39 · 注释里引用了一个"已实测对照"，但那一侧的实测数据根本不存在

**位置**：`cmd/raftbench/shards.go`（`maxRaftInstances` 的说明）

**为什么错**：注释写着

> 本模式量的是**比例**（可用分片占比），
> 而比例对 400 分片与 1000 分片是同一个数（**已实测对照**）。

评审问"实测数据在哪"。查 `results/`：**只有 400 分片的 10 个文件，
1000 分片那一侧没有任何落盘证据**。所以那句话当时**不可核对** ——
它把一次临时运行说成了"已实测对照"，而证据链是断的。

**补测之后发现它不只是"没证据"，而是"错的"**（`-mode shards-scale`，
同一结构 n=3/d=5/|Q1|=|Q2|=2/杀 1 域/延迟 0，两侧写进同一份 JSON）：

| 分片数 | 实例数 | 三轮末刻可用比例 | 算术界 |
|---|---|---|---|
| 400 | 1200 | 100.0% / 100.0% / 97.5% | 100% |
| 1000 | 3000 | 71.6% / 90.5% / 96.0% | 100% |

跨规模最大差 0.2840 = **11.4 σ**（σ 按 S=400、p=0.5 保守取），**远超抽样噪声**。
更糟的是 1000 分片侧**另一次同参数运行是 34.3% / 50.9% / 100.0%** ——
同一命令跨 session 差 3 倍。

**原因**：不是 quorum 结构，而是单机资源。3000 个 Raft 实例共享 16 个逻辑核，
CPU 饱和使选举 RPC 超时（日志成片 `requestVote ... send timed out`，term 一秒涨上千），
候选者反复重来形成**活锁**。
**算术界「存活数 ≥ |Q1|」是必要条件，不是充分条件** —— 它没算"选举能不能收敛"。

**修复**：

1. 删掉那句结论，换成实测数据与正确的适用范围；
2. 新增 `-mode shards-scale`：把多个分片数放进**同一次调用**写进同一份 JSON，
   让"同一个数"这类说法**可以被直接复核**（而不是只能相信注释）；
3. `results/shards-scale-400.json` 落盘；
4. `MEASUREMENT.md` §5.8 的"10/10 精确等于算术界"补上规模限定（400 分片），
   新增 §5.8.1 记录这次推翻。

**教训**：**注释里引用"已实测"时，必须同时能指出落盘文件。**
指不出来就等于没测 —— 而这类话最危险的地方在于，它看起来像是有人验过了。

### BUG-40 · 判据自己骗人：σ 算成 0，于是"差 0.28"被判成"在噪声内"

**位置**：`cmd/raftbench/shards_scale.go`（跨规模裁决逻辑，BUG-39 的新代码）

**为什么错**：第一版用**各规模的中位数**做比较，并且拿"中位数的中位数"当 p 去算
二项标准差 σ = sqrt(p(1-p)/S)。当两个中位数是 0.509 与 1.0 时，
`medianF` 取到 **1.0**，于是 p(1-p)=0 ⇒ **σ=0** ⇒ 差值/σ 无法计算（保留 0）⇒
判据输出"**相当于 0.00 σ，在抽样噪声内**" —— 而真实差值是 **0.49**、
真实 σ 约 0.025（约 20σ）。

也就是说：**这个工具本该回答"400 与 1000 是不是同一个数"，它却给出了相反答案。**

**怎么发现的**：打印出来自相矛盾 —— 同一段文字里写着"最大差 0.4910"，
下一句说"在抽样噪声内"。

**修复**：

1. 差值取**所有轮次两两之间**的最大差，不用中位数之差
   （中位数会把"样本内 34%↔100%"这种波动整个抹平）；
2. σ 用**最小规模 + p=0.5**（方差最大，是上界）算，不会低估噪声；
3. 每个规模的 **min–max** 一并报出来 —— 波动本身就是结论的一部分。

**教训**：这是同一族错误在本项目的**第三次**（BUG-28 松弛方向、BUG-35 聚合错误数、
BUG-38 无证据推断）：**判据比数据更容易骗人**。
一条判据只要出现"自相矛盾的输出"，就必须当成缺陷处理 ——
这里的两句话就差一行，肉眼能看出来，但如果不逐字读就会被它带走。
**教训**：**"看起来像"不是证据。**
一个数字支持多种解释时，必须去做能区分它们的那次测量 —— 这里就是穷举。
同类错误在本项目里已反复出现（BUG-28 的松弛方向、BUG-29 的重复计数、
BUG-35 的聚合错误数），共同特征是：
**结论方向明确，但支撑它的数字并不唯一指向该结论。**





