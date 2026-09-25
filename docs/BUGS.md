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
| 逻辑错误（功能不工作） | 8 | 数据面缺失、路由不转发、成员变更失效 |
| 性能缺陷（数量级） | 6 | 单请求 O(n log n)、每读一次 fsync |
| 资源泄漏 / 死代码 | 4 | goroutine 与连接泄漏 |
| 配置与部署错误 | 4 | 端口越界、指标全局注册冲突 |
| **合计** | **27** | |

其中最严重的三条：

1. **BUG-16**：路由层从不转发请求。整个系统的读写路径实际不存在。
2. **BUG-1**：`FSM` 的读写用了两把不相干的锁 —— 并发读写 Go map，未定义行为。
3. **BUG-21**：客户端在出错时无限递归，Leader 持续不可用时栈溢出崩溃。

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
go test  -p 1 ./...     通过  (pkg/raft 11.6s, pkg/metadata 8.8s)
```

测试覆盖：

- 命令编解码往返 / 确定性 / CRC 篡改检测
- 快照往返 / bad magic / 截断快照不 panic
- 并发读写 FSM（BUG-1 回归）
- 二分查找对全键空间与暴力查找逐一对照（16 分片 × 20000 随机 key）
- 删除分片真正生效 + 事件广播（BUG-12 回归）
- 三副本复制一致性（200 键 × 3 副本逐键比对）
- Leader 硬停后重新选主（实测 ~2.95s）
- 幂等 Bootstrap（BUG-3 回归）
- 非 Leader 拒绝写与成员变更（BUG-4 回归）
- ephemeral 端口解析（新缺陷回归）

### 未验证（诚实记录）

1. **`-race` 未执行**。本机没有 C 编译器（无 gcc / clang / MSVC），
   而 `go test -race` 需要 cgo。**BUG-1 的修复没有被 race detector 独立验证**，
   目前的证据是逻辑推理 + 并发功能测试（`TestFSMConcurrentReadWrite`）。
   补齐方式：装 TDM-GCC 或 MSVC Build Tools 后运行
   `pwsh -File build.ps1 race`。

2. **etcd 集成路径未跑通**。`pkg/metadata` 的单元测试全部基于
   `LocalTopology`（进程内）。`ClusterTopology` 的 watcher、事务写入、
   `Ping` 路径只有静态审查，**没有真实 etcd 端到端验证**。
   `docs/etcd-cluster.md` 描述的部署方式未经本轮验证。

3. **`scripts/*.sh` 未在 Windows 上执行**。这些脚本是 bash，
   本机 PowerShell 环境未验证其可运行性。

4. **1000 节点规模未实测**。`pkg/model` 给出的是**解析预测**，
   端到端实测停在 2 分片 × 3 副本 = 6 副本。
   大规模结论必须有实测支撑才可写入论文（见 `DESIGN.md` 的实验计划）。

5. **写路径未优化**。实测 `put p50 = 99.24ms`，受单批 fsync 限制。
   修复方向明确（写批处理），但尚未实现。

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
