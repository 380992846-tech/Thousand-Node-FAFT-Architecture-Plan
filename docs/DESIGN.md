# FAFT：故障域感知的弹性 quorum 拓扑
## 设计方向与实验方案（可发表工作底稿）

> **FAFT = Failure-Aware Flexible-quorum Topology**
> 把「分片副本放在哪些故障域」与「每个分片用多大的 quorum」当作**同一个优化问题**来解。

本文档的目标是给出一个**能站得住的发表方向**，以及一套**能被审稿人重放的实验方案**。
文档中每一条定量论断都标注来源：`[已发表]` 表示有文献出处，`[本项目实测]` 表示
本仓库的可复现测量，`[推导]` 表示本文档自行推导且明确标注为推导。

---

## 0. 结论先行

**可以主张的贡献**（按可信度排序）：

| # | 贡献 | 支撑力度 |
|---|---|---|
| C1 | 把「quorum 几何」与「副本放置」形式化为**联合优化问题**，给出可行性多面体与代价函数 | 强 —— 文献里两者始终分开处理 |
| C2 | 面向**相关故障**的故障域感知 quorum 构造，量化"存活性 vs 成本"的权衡曲线 | 强 —— 工业界有实践（Spanner witness、OceanBase 仲裁副本），无学术形式化 |
| C3 | 在真实 Multi-Raft 分片存储上实现并测量 FAFT，覆盖分组数 1→N 的扫描 | 中 —— 依赖本仓库工程完成度 |
| C4 | 一个**校准过的离散事件模拟器**，把结论外推到 1000 节点 | 中 —— 必须有校准误差的诚实披露 |
| C5 | 规模化的测量方法学（开环发压、故障注入边界定义、WAN 带宽作为一等指标） | 中 —— 方法学贡献，需论证为何现有工作缺失 |

**不能主张的**（会被审稿人当场击落）：

- ❌ leader 出口带宽 ∝ 1/(n−1)。Mencius (OSDI'08) §7 原文已写出
  *"its throughput is in proportion to 1/(n-1)"*，Leopard 与 Carnot Bound 给出同形式界。
- ❌ 「线性化读需要 ≥1 个到 quorum 的往返」这条"定理"。两轮独立检索均
  **未找到任何一手来源**，属民间传说（folklore）。
- ❌ 经典下界在 n=1000 处的紧张性。见 §2.2：它们**全都不紧张**。
- ❌ 「erasure coding 能缩小共识 quorum」。DispersedLedger (NSDI'22) 明确：
  *"a BFT protocol on N=3f+1 nodes requires votes from at least 2f+1 nodes to make progress"* ——
  coding 改变的是**谁下载什么**，不是**谁必须同意**。

---

## 1. 问题定位：文献到底留了什么缝

### 1.1 已发表工作覆盖了什么

| 方向 | 代表工作 | 状态 |
|---|---|---|
| 单组共识协议 | Raft(ATC'14)、EPaxos(SOSP'13)、PigPaxos(SIGMOD'21)、SwiftPaxos(NSDI'24)、Rabia(SOSP'21)、Nezha(PVLDB'23) | 极度饱和 |
| 弹性 quorum | Flexible Paxos(OPODIS'16)、Fast FPaxos(ICDCN'21)、FlexiRaft(CIDR'23)、Orca(PVLDB'26) | 已充分探索 |
| 领导者租约 | Paxos Quorum Leases(SoCC'14)、LeaseGuard(SIGMOD'26)、Bodega(OSDI'26) | 本年度刚被补上形式化处理 |
| 非投票副本 | Spanner witness、CockroachDB non-voting、TiKV learner | 工业界成熟，学术未形式化 |
| 纠删码 + 共识 | CRaft(FAST'20)、Racos(SoCC'24)、Hyra(SIGMOD'26)、Nostor(OSDI'25) | 有真实谱系，缝很窄 |
| 硬件加速共识 | Waverunner(NSDI'23)、Nano-Consensus(SoCC'25)、P4KVS(PACMMOD'25) | 热门但门槛极高 |
| 分片 RSM 吞吐 | Mako(OSDI'25)：10 分片 3.66M TPC-C txn/s | 强基准，必须对比 |

### 1.2 确认为空白的部分

**核心空白：Multi-Raft 的放置 / 分裂合并 / 热点策略几乎没有学术发表。**

证据（可复现）：arXiv 全文检索

- `all:"multi-raft"` → 全库仅 **1** 篇，且该文只把 Multi-Raft 当 baseline；
- `all:"placement driver"` → **0** 篇；
- `all:"leader placement"` → **0** 篇；
- `all:"replica placement" AND all:raft` → **0** 篇；
- `all:"sharded consensus"` → 9 篇，全部是区块链分片 / BFT，无一涉及 Multi-Raft。

对比：**multi-BFT 的排序问题**已经有人在做 ——
Hydra (ICDE'26, pp.2587–2600)、Orthrus (ICDE'25, pp.2615–2627)、
Ladon (EuroSys'25, DOI 10.1145/3689031.3696102)。
所以"多组共识的排序"不是空白，"**多组 Raft 的放置与 quorum 几何**"才是。

最近的学术邻居是 **ReCraft (DSN'25, arXiv:2504.14802)**：
*"enables the sharding of Raft clusters with split and merge reconfigurations"*，
自带安全性与活性证明。但它是**重配置协议**论文，不是**放置优化**论文。

工业界的实际做法全部散落在 vendor 文档与 RFC 里：

- TiKV PD 只有三个原语：`AddReplica` / `RemoveReplica` / `TransferLeader`；
  `balance-leader-scheduler` 按"该 store 上 region 大小之和"打分；
  hotspot 用 rank-formula v1/v2；
- CockroachDB v26.3 起有 **multi-metric allocator**，对每 store 的
  CPU / 写带宽 / 磁盘建模，优先租约转移而非副本搬迁；
- **但没有任何一篇论文给出这些启发式的竞争比、最优性、或最优策略。**

### 1.3 因此本研究的位置

> 我们不提出新的共识协议 —— 单组协议的设计空间已经饱和（要打赢
> Nezha、SwiftPaxos、P4KVS、Nano-Consensus，门槛高到不现实）。
> 我们提出的是**跨分片的决策层**：在给定工作负载、故障域结构与网络代价下，
> 如何**同时**决定每个分片的 quorum 几何、副本落点与 Leader 位置。

这个位置的好处：所有 baseline 都现成（TiKV 启发式、FlexiRaft、Raft），
工作负载现成（YCSB / TPC-C），而且**没有人抢**。

---

## 2. 理论支撑：哪些界是紧的，哪些不是

这一节决定论文的理论叙事。**关键结论：经典下界在 n=1000、f≤5 时全都不紧**，
真正绑定的是工程量的伸缩律。

### 2.1 三个精确的经典结果（[已发表]）

**Lamport, _Lower Bounds on Consensus_ (MSR 2000 / Distributed Computing 19:104–125, 2006)**

设 n 为处理器数，m 为最小多数集合大小，非拜占庭、消息可丢失：

| 方案 | 消息数 | 消息延迟 |
|---|---|---|
| 2 个延迟下的最优 | `n(m−1)` | 2 |
| 消息数最少 | `m + n − 2` | `m` |
| 经典 synod（=Multi-Paxos Phase-2） | `2m + n − 3` | 3 |

**Dolev–Reischuk (JACM 32(1):191–204, 1985)** —— 注意正确形式：

- 认证模型：`Ω(n + t²)`
- 未认证模型：`Ω(nt)`

> ⚠️ **常见错误引用**：流传的 `Ω(min(f², n²))` **不是**这个定理。
> 因为 `t ≤ n` 恒成立，`min(t², n²) = t²`，在小 f 时会退化成 `Ω(1)`
> 而真实界是 `Ω(n)` —— 方向都反了。已核对 Crossref 记录
> (DOI 10.1145/2455.214112) 的原文摘要。

**Flexible Paxos (Howard, Malkhi, Spiegelman, OPODIS 2016, §4.2)** —— 核心约束：

```
|Q1| + |Q2| > N
```

推论：**两个阶段不能同时用少数派 quorum**。因为若 `|Q1| ≤ N/2` 且
`|Q2| ≤ N/2`，则 `|Q1|+|Q2| ≤ N`，违反约束。即 `min(|Q1|,|Q2|) ≤ ⌊N/2⌋`。

这条推论是本设计的**出发点**：自由度不在"把 quorum 变小"，
而在"**把哪个阶段变小**"。

### 2.2 在 n=1000, f=1..5 处求值：没有一个下界是紧的 [推导]

设 n=1000，标准多数 quorum 则 m=501：

| f | Dolev–Reischuk `Ω(n+f²)` | f² 占比 | Lamport 经典 synod | 容错需求量 |
|---|---|---|---|---|
| 1 | 1000+1 = 1001 | 0.10% | 1999 | 2 |
| 2 | 1000+4 = 1004 | 0.40% | 1999 | 3 |
| 3 | 1000+9 = 1009 | 0.89% | 1999 | 4 |
| 4 | 1000+16 = 1016 | 1.57% | 1999 | 5 |
| 5 | 1000+25 = 1025 | 2.44% | 1999 | 6 |

**发现 1**：n=1000、f≤5 时，Dolev–Reischuk 的二次项最多占界的 **2.44%**，
渐进界实际是 `Ω(n)` 而**不是** `Ω(f²)`。Dolev–Reischuk 只有在
`f = Θ(n)`（n=1000 时 f ≳ 32）才变成二次的。

**发现 2**：多数 quorum 是 501，而 f=5 只需要 6 个副本 —— **过度配置 83.5 倍**。
每个"千节点部署"都在为 499 个故障付钱，而实际只需容忍 1–5 个。

### 2.3 真正绑定的东西：Flexible Paxos 的可用性不对称 [本项目实测计算]

> 本节数字由 `cmd/faftbench cost` 与 `cmd/faftbench domain` 生成，可复现。
> 这是本项目**最重要的一组结果**，它直接决定了 FAFT 的命题是否成立。

在 `|Q1| + |Q2| > 1000` 下，把阶段二收窄会强制阶段一膨胀。
下面在**副本粒度**上计算可用性（单副本失效率 p=1e-4），
quorum 大小为 q 时 `P[可用] = Σ_{i=q}^{n} C(n,i)(1−p)^i p^{n−i}`：

| n | \|Q2\| | \|Q1\| | 写消息/op | 选举需在线副本 | 控制路径可用性 |
|---|---|---|---|---|---|
| 1000 | 501 | 500 | 1002 | 500 | ~1.000000000000 |
| 1000 | **2** | **999** | **4** | **999** | **0.995325232148** |
| 1000 | 250 | 751 | 500 | 751 | ~1.000000000000 |
| 101 | 51 | 51 | 102 | 51 | ~1.000000000000 |
| 101 | 2 | 100 | 4 | 100 | 0.999949832078 |
| 21 | 11 | 11 | 22 | 11 | ~1.000000000000 |
| 21 | 2 | 20 | 4 | 20 | 0.999997902658 |

**发现 3（核心命题）**：弹性 quorum 把写消息数从 1002 压到 4 ——
**167 倍的收益** —— 代价是选举需要 **999/1000** 个副本在线，
控制路径可用性从 ~1.0 崩到 **0.9953**，即约 **13 倍的可达性劣化**。

即：**消息收益与可用性代价是同一个决策的两面，而现有做法在两者之间二选一。**

**发现 4（FAFT 的突破点）**：决定控制路径可达性的**不是 quorum 的大小，
而是它的组织结构**。若 quorum 按故障域组织（域内失效相关、域间独立），
即使把单域失效率设成 **1e-3**（比单副本 1e-4 高一个数量级 —— 对 FAFT 是**不利**假设）：

| 域数 D | 每域副本 | \|Q2\|（域） | \|Q1\|（域） | 选举需在线域数 | 控制路径可用性 |
|---|---|---|---|---|---|
| 3 | 333 | 2 | 2 | 2 | 0.999997002000 |
| **5** | **200** | **2** | **4** | **4** | **0.999990019985** |
| 7 | 142 | 2 | 6 | 6 | 0.999979069895 |
| 15 | 66 | 2 | 14 | 14 | 0.999895905917 |

**发现 5**：在 5 个故障域下，域级组织用 `|Q2|=2 域`、`|Q1|=4 域`
就把控制路径可用性维持在 **0.99999**，而副本级组织在同样的 `|Q2|=2` 下
只有 **0.9953**。

> ⚠️ **比较口径的诚实说明**：两组数字的失效概率不同
> （域级用 p_d=1e-3，副本级用 p=1e-4）。域级数字是在**更悲观**的
> 假设下取得的，因此这个对比对 FAFT 是保守的。但论文中必须把两个模型
> 各自的假设写清楚，并补一组同口径的对照实验。

**这正是 FAFT 要解决的问题**：与其在"省消息"与"保可用性"之间二选一，
不如**同时**决定 quorum 几何与副本落点，使阶段一与阶段二各自
落在合适的故障域上。

### 2.4 收益上界的量级：必须诚实 [本项目实测计算]

`cmd/faftbench scale` 的输出（5 个故障域，跨域 RTT 2ms）：

| n | 基线写消息 | FAFT 写消息 | 省倍数 |
|---|---|---|---|
| 3 | 4 | 4 | 1.00 |
| 5 | 6 | 4 | 1.50 |
| 9 | 10 | 6 | 1.67 |
| 21 | 22 | 12 | 1.83 |
| 51 | 52 | 24 | 2.17 |
| 101 | 102 | 44 | 2.32 |

**发现 6（对论文叙事至关重要）**：在**受限**的可行域内
（同时满足数据与控制两条路径的域级容错目标），FAFT 的消息收益是
**1.5×–2.3×**，而**不是**文献标题里常见的 83× 或 167×。

那 83×/167× 只在**放弃控制路径容错约束**时出现 —— 也就是
`|Q1| = n − |Q2| + 1 ≈ n` 那个角落。

> **因此论文的正确叙事不是"我们省了 167 倍消息"，
> 而是"我们把 167 倍的收益从'牺牲可用性'改成了'重新组织 quorum" 。**
> 这个区别决定了论文是可信的还是被当场击落的。

### 2.5 成本模型的四个上限 [组合形式为推导]

| 上限 | 公式 | 来源 |
|---|---|---|
| Leader 出口带宽 | `C / ((n−1)·β)` | `[已发表]` Mencius OSDI'08 §7；Leopard；Carnot Bound |
| Leader 磁盘 | `b / t_fsync`（批大小 b 摊薄） | `[推导]` 批处理模型 |
| Leader CPU | `1 / ((n−1)·c_cpu)` | `[推导]` Mencius 给出定性形式 |
| 有效吞吐 | `min(三者)` | `[推导]` 四变量联合闭式未见发表 |

**本项目实测数据**（`pkg/raft` 基准，单节点，Windows，BoltDB）：

```
BenchmarkGetRaw                 578 ns/op      ← 状态机本身
BenchmarkSet              5,447,163 ns/op      ← 一次 Raft 往返
BenchmarkBarrier          6,672,332 ns/op
BenchmarkBarrierVsCommitTimeout: 1ms/5ms/20ms → 5.72/6.46/5.87 ms
```

**发现 5**：`CommitTimeout` 从 1ms 调到 20ms，Barrier 成本纹丝不动
（5.72 → 5.87ms）。**瓶颈是 fsync，不是提交超时。**

用本项目模型复算：`b=1, t_fsync≈5.4ms → 185 ops/s`，
与 `BenchmarkSet` 的 ~184 ops/s 一致。**模型与实测在一个数量级内吻合。**

**发现 6（对论文很重要）**：在本机硬件上，**n=5 时真正的绑定约束是磁盘**
（185 ops/s），而不是 leader 带宽（122,070 ops/s）——
**相差三个数量级**。任何"千节点受 leader 带宽限制"的论断都必须
明确声明 `b` 与 `t_fsync`，否则是在拿一个不紧的界讲故事。

### 2.5 强一致性的读：唯一无需时钟假设的路径 [已发表]

| 机制 | 额外 RTT | 时钟假设 | 陈旧度 |
|---|---|---|---|
| Raft leader 读（readIndex 心跳轮） | 每批 1 RTT | **无** | 无（线性化） |
| Leader lease 读 | 0 | 有界时钟漂移 | leader 上无 |
| Quorum leases (SoCC'14) | 持有者上 0 | 时钟**速率**相近 + guard time | 持有者上无 |
| LeaseGuard (SIGMOD'26) | 0 | **两层**：漂移率 ϵ（延迟提交）/ 有界不确定性（继承租约读） | 无 |
| TiKV safe-ts | 0 | **无**（逻辑时间戳 + apply index） | 复制滞后 |
| CockroachDB follower read | 0 | **强制**有界偏移（80% 阈值即崩溃） | ≥ ~4.2s |
| Spanner snapshot read | 0 | TrueTime 区间，ε = 1–7 ms | 受 ε 约束 |

**结论（可直接引用）**：**唯一零时钟假设的线性化读是 readIndex 心跳轮
（每批 1 RTT）。任何 0-RTT 机制要么把线性化降级为有界陈旧，要么用时钟假设换 0 RTT。**

本项目据此实现了 `ReadBarrier`（`pkg/raft`），实测读延迟从
**39.13ms → 4.09ms（约 10 倍）**，且不引入任何时钟假设。

---

## 3. FAFT 设计

### 3.1 记号

- 分片集合 `S`，`|S| = S`；分片 `s` 的副本集 `R_s`，`|R_s| = n`
- 故障域集合 `D`（zone / rack / host 层级）；`dom(v)` 为节点 v 所属最细故障域
- 分片 `s` 的工作负载：读比例 `ρ_s`、访问速率 `λ_s`、key 分布
- 域间通信代价矩阵 `W[d_i][d_j]`（RTT 或 元字节代价）
- 故障模型：域 `d` 整体失效的概率 `p_d`，**允许相关故障**

### 3.2 两个决策变量（这是核心的统一）

**决策 1 · Quorum 几何**：为每个分片选择 `(Q1_s, Q2_s)`
（阶段一/阶段二 quorum 的**大小**），满足：

```
|Q1_s| + |Q2_s| > n           (Flexible Paxos 安全性, OPODIS'16)
|Q2_s| ≥ f_data + 1           (数据路径容错目标)
|Q1_s| ≥ f_ctrl + 1           (控制路径容错目标 —— 本研究引入)
```

**决策 2 · 副本落点**：`place : R_s → D`，即每个副本落在哪个故障域。

**关键洞察：这两个决策是耦合的，而现有系统把它们分开处理。**

传统的"多数 quorum"把两者**都固定死**了：quorum 恒为 `⌊n/2⌋+1`，
落点只要求"分散到不同故障域"。于是出现 §2.3 的发现 4：
控制路径被 995/1000 的可用性要求绑死。

FAFT 的做法是：**因为 `Q1` 和 `Q2` 只需要彼此相交（而不需要各自内部相交），
我们让 `Q2`（频繁、廉价）落在低代价域内，`Q1`（罕见、昂贵）跨域展开。**

```
        域 A (低延迟)          域 B            域 C
      ┌──────────────┐      ┌────────┐      ┌────────┐
  Q2  │ v1 v2 v3     │      │        │      │        │   ← 阶段二：域内 3 副本
      └──────────────┘      └────────┘      └────────┘
  Q1  │ v1 v2        │      │ v4 v5  │      │ v6 v7  │   ← 阶段一：跨域展开
      └──────────────┘      └────────┘      └────────┘
```

`|Q1| + |Q2| > n` 仍然满足（例如 n=7, |Q2|=3, |Q1|=5, 3+5=8>7）。

### 3.3 故障域感知的存活性目标（C2 的形式化）

定义分片 `s` 的**存活性**为"在给定故障模型下仍能形成合法 quorum 的概率"。
允许相关故障时，故障不再是独立的伯努利变量，而应按**故障域**建模：

```
P[Q2_s 不可用] = Σ_{D' ⊆ dom(Q2_s), cost(D') > budget_data} P[域集合 D' 同时失效]
```

**这是与所有现有工作的关键差别**：TiKV / CockroachDB 的放置规则
（`location-labels`、`SURVIVE REGION FAILURE`）是**规则式**的
（"每个域放一个副本"），而不是**概率式**的。当故障域大小不均、
或故障相关性随域变化时，规则式放置不是最优的。

### 3.4 目标函数

```
minimize   Σ_s [ λ_s · ( ρ_s · L_read(s) + (1−ρ_s) · L_write(s) ) ]     (延迟)
         + Σ_s   B(s)                                                   (WAN 流量)
         + ω    · Σ_s ( 存储成本 )                                       (空间)
subject to
           P[可用性违反] ≤ ε        (每个分片独立约束)
           |Q1_s| + |Q2_s| > n
           副本数 = n（或自由变量，见 3.5）
```

其中 `L_read(s)` 在 readIndex 语义下约为"到 `Q2_s` 的一个往返"，
`L_write(s)` 约为"到 `Q2_s` 的一个往返 + fsync"。

**为什么这个目标是新的**：它把（quorum 大小）与（副本落点）
放进**同一个最小化**，且延迟项显式依赖 `W[dom(·)][dom(·)]`。
文献中：
- Flexible Paxos 系列只优化 quorum **大小**，不涉及落点；
- MDCC / WPaxos / DPaxos 用 RTT 矩阵做**放置**，但不联合 quorum 几何；
- CockroachDB 的 MMA 是**贪心启发式 + 模拟**，无最优性分析。

**本项目已经实现的部分**：`pkg/faft` 给出
`Solve()`（在 `|Q1|+|Q2|>N` 可行域内枚举 + 域覆盖贪心）、
`Evaluate()`、`BaselineMajority()`、以及
`AvailabilityAtReplicaLevel()` / `ComputeControlCost()` 两个可用性计算器。
§2.3 与 §2.4 的所有数字都由它产出。

**尚未实现的部分**：相关故障下的联合优化（当前是独立域失效假设）、
LP 松弛下界、以及在线的增量重放置。

### 3.5 见证副本 / 非投票副本的作用（C2 的一部分）

需要明确指出一个**语义区分**，否则会被审稿人抓住：

| 系统 | 角色名 | 存数据？ | 投票？ |
|---|---|---|---|
| **Spanner** | witness replica | ❌ | ✅ 参与提交与选主，但不能当选 leader |
| **OceanBase** | 仲裁副本 | ❌ | ⚠️ **部分** —— 参与选主与 Prepare，**不参与 Accept** |
| **TiKV** | learner | ✅ | ❌ |
| **CockroachDB** | non-voting replica | ✅ | ❌ |
| **YugabyteDB** | read replica | ✅ | ❌ |

**只有 Spanner 的 witness 是"投票但不存数据"的副本。**
OceanBase 的仲裁副本是一个**学术文献里没有描述过的设计点**（部分投票者）。

这给 FAFT 一个明确的设计自由度：**引入"统计特性不同"的副本类型**，
并把它纳入 §3.4 的联合优化。例如：
在某个故障域放一个 witness（不存数据、参与选举、不参与 Accept），
既消除了"该域整体失效导致选不出主"的风险，又不付存储代价。

### 3.6 与文献的边界（必须写进论文的 positioning 段）

| 我们的机制 | 最近的现有工作 | 我们的增量 |
|---|---|---|
| 联合优化 quorum 几何 + 落点 | Flexible Paxos / MDCC / MMA（各自只做一半） | 形式化联合问题 + 可行性多面体 |
| 故障域概率模型 | placement rules（规则式） | 概率式 + 相关故障 |
| 控制路径容错目标 `f_ctrl` | 无对应概念 | 新引入的约束维度 |
| 分级副本类型 | Spanner witness / OceanBase 仲裁 | 纳入统一优化，并做实测对比 |

---

## 4. 实验方案

### 4.1 研究问题

- **RQ1**：在给定故障模型下，FAFT 的联合优化相比"多数 quorum + 均匀放置"
  能降低多少延迟 / WAN 流量，代价是多少存储？
- **RQ2**：控制路径的可用性（选主容忍度）与数据路径可用性
  （复制容忍度）之间的权衡曲线长什么样？
- **RQ3**：分片数从 1 增长到 N 时，放置层决策的收益是否随规模**放大或衰减**？
- **RQ4**：相关故障（整域失效）下，FAFT 相比规则式放置的优势有多大？

### 4.2 Baselines（缺一不可）

| # | Baseline | 为什么必须有 |
|---|---|---|
| B1 | 标准 Multi-Raft（多数 quorum + 每域一副本） | 主基准 |
| B2 | **FlexiRaft (CIDR'23)** | 弹性 quorum 的直接对手；Meta 生产验证 |
| B3 | **Orca (PVLDB'26)** | 本年度弹性 quorum 最新工作 |
| B4 | TiKV PD 启发式（balance-leader + balance-region） | 工业界实际做法 |
| B5 | CockroachDB MMA 式贪心（自实现） | 多指标分配器 |
| B6 | 均匀放置 + 多数 quorum（消融下界） | 隔离"放置"与"几何"各自的贡献 |

> B2/B3 若拿不到代码，必须明确声明是**按论文描述重实现**，
> 并在论文中披露重实现与原作的性能差距（参考文献里
> "EPaxos Revisited" (NSDI'21) 的做法：*"more than 4× worse tail latency
> than previously reported"* —— 一个顶会对landmark评测方法的重击）。

### 4.3 工作负载

| 工作负载 | 用途 | 参数 |
|---|---|---|
| **YCSB A–F** | 通用读写混合 | 1KB 记录，10 字段 × 100B；zipfian 与 uniform **都要跑** |
| **TPC-C** | 与 Mako (OSDI'25) 对标 | 45/43/4/4/4 混合 |
| **热点倾斜扫描** | RQ3 | zipf θ ∈ {0.5, 0.9, 0.99, 1.1} |
| **写密集** | 压 fsync 路径 | 读比例 ρ ∈ {0, 0.1, 0.5, 0.9, 1.0} |

> ⚠️ **YCSB 陷阱**：`pingcap/go-ycsb` 的 workload 文件里
> 注释写 zipfian，参数却是 `requestdistribution=uniform`。
> 论文必须明确写出**实际使用的分布参数**，不能只写"YCSB workload A"。

### 4.4 指标（全部必须报告）

**吞吐与延迟**
- 吞吐（ops/s）、p50 / p90 / p99 / p99.9 / max 延迟
- **吞吐-延迟曲线**（固定并发扫速率），而非单点数字
- 分读 / 写分别报告

**可用性**
- 选主容忍度 `f_ctrl`、复制容忍度 `f_data`（RQ2 的核心）
- 故障注入下的**不可用时间窗口**，且必须**定义起止边界**

> ⚠️ **方法学缺口**：检索未找到任何论文定义"故障切换区间从哪算到哪"。
> uKharon (ATC'22, 53μs) 与 RRC (<1s) 的数字**不可比**。
> 我们必须在论文中明确定义边界（建议：从最后一个成功响应到第一个成功响应）。

**网络代价（一等指标）**
- WAN 字节数 / 操作、跨域流量占比
- 心跳总带宽
> 现状：FlexiRaft 明确声明跨域流量削减 *"beyond the scope of this paper"*；
> 六大厂商里只有 Spanner（~100ms 跨域强读）与 TiDB（~3ms 域内 / ~20ms 跨域）
> 公布了硬数字。**把带宽做成 headline 指标本身就是贡献。**

**恢复与重配置**
- 单副本替换代价（消息 ack 数）
- 全集群重配置代价
- 重配置期间的尾延迟劣化

### 4.5 实验矩阵

| 维度 | 取值 | 说明 |
|---|---|---|
| 分片数 S | 1, 4, 16, 64, 256 | RQ3 的横向轴 |
| 副本数 n | 3, 5, 7 | |
| 故障域数 | 1, 3, 5 | 单 AZ / 多 AZ / 跨域 |
| 故障模型 | 独立 / 整域相关 | RQ4 |
| 故障注入 | 无 / 单副本 / 整域 / leader 定向 | |
| 读比例 ρ | 0, 0.1, 0.5, 0.9, 1.0 | |
| 并发 | 1, 8, 32, 128, 512 | |

### 4.6 验证策略（诚实版）

**真实集群**：最多 **100 分片 × 5 副本 = 500 副本**，单机多进程
（`pkg/cluster` 已支持单进程内起任意规模）。

如实说明：单机多进程与真实多机的差异在于
（a）无真实网络分区，（b）共享 CPU / 页面缓存 / 磁盘，
（c）无真实跨域 RTT。因此：

**模拟器**：实现离散事件模拟器，用于外推到 1000 节点。必须做到：
1. **校准**：在 5 / 50 / 100 副本三档上，模拟器输出与实测吞吐/延迟的
   误差必须 **< 15%**，并在论文中画出校准曲线；
2. **披露**：明确给出模拟器与实测**开始偏离**的规模点；
3. **不主张**：模拟器结论只能作为**趋势预测**，不能作为绝对性能声明。

> 这条策略的动机来自文献教训：EPaxos Revisited (NSDI'21) 发现原评测方法
> *"does not trigger or measure the full impact of conflict behavior"*，
> 结果 EPaxos 的尾延迟比 Multi-Paxos **差 4 倍以上** —— 与原始论文的结论相反。

### 4.7 消融实验

| 消融 | 隔离的贡献 |
|---|---|
| 固定多数 quorum + FAFT 放置 | 放置的独立贡献 |
| FAFT 几何 + 均匀放置 | quorum 几何的独立贡献 |
| 去掉控制路径约束 `f_ctrl` | 新引入约束的价值 |
| 去掉相关故障建模（退回独立故障） | 故障模型的贡献 |
| 去掉 witness 类型 | 分级副本的价值 |
| 关掉路由缓存 / singleflight | 工程优化 vs 算法优化的区分 |

---

## 5. 已有工程基础（本仓库）

| 模块 | 状态 | 对应论文需求 |
|---|---|---|
| `pkg/raft` | ✅ 已修复 27 项缺陷；readIndex 读优化（读延迟 ↓10×） | 被测系统的正确基线 |
| `pkg/metadata` | ✅ O(log n) 二分查找；`LocalTopology` 支持离线实验 | 拓扑层 |
| `pkg/nodeapi` | ✅ 数据面 HTTP API（原缺失） | 可测量接口 |
| `pkg/router` | ✅ 真转发 + Leader 提示重试 | 路由层 |
| `pkg/client` | ✅ 有界重试 + singleflight + 区间缓存 | 客户端 |
| `pkg/cluster` | ✅ 单进程多分片集群，一条命令起 N 副本 | **实验可复现性** |
| `pkg/bench` | ✅ 对数直方图 + **开环/闭环双模式** | 避免 coordinated omission |
| `pkg/model` | ✅ 四上限成本模型 + Lamport 精确界 + FPaxos 约束 | 论文的解析部分 |
| `cmd/kvbench` | ✅ 一体化实验驱动，同种子 => 同结果 | 结果可重放 |
| **FAFT 求解器** | ❌ **未实现** | **论文的核心贡献 C1/C2** |
| **离散事件模拟器** | ❌ **未实现** | RQ3 的规模化外推 |
| **写路径批处理** | ❌ 未实现（实测 put p50 99ms） | 否则写性能不可比 |

---

## 6. 目标会议与时间线

**首选：NSDI / OSDI**。理由：
- 该问题的空白在**系统测量**方向（vendor 文档 vs 论文），NSDI 偏好此类；
- 需要真实实现 + 真实测量，正是 NSDI 的品味；
- 若理论部分做扎实，也可投 **PODC / DISC** 的短论文。

**备选：EuroSys / SIGMOD / PVLDB**。
- SIGMOD/PVLDB 更看重与数据库系统的整合（需接 TPC-C）。

**时间线（假设每周可投入 15–20 小时）**

| 阶段 | 内容 | 预估 |
|---|---|---|
| P0 | FAFT 求解器（贪心 + LP 下界） | 3–4 周 |
| P1 | 校准版离散事件模拟器 | 3–4 周 |
| P2 | 真实集群测量至 500 副本 | 3–4 周 |
| P3 | Baseline 重实现（FlexiRaft / Orca / PD 启发式） | 4–6 周 |
| P4 | 消融 + 故障注入矩阵 | 3–4 周 |
| P5 | 写作 + artifact 打包 | 4–6 周 |
| | **合计** | **约 5–7 个月** |

---

## 7. 风险与对策

| 风险 | 严重度 | 对策 |
|---|---|---|
| FAFT 收益在真实硬件上不显著 | **高** | 先做**离线**求解 + 模拟器算收益上界；若上界不够大，及时转向 |
| 单机多进程实验被质疑不代表真实部署 | **高** | 补一组真实多机实验（哪怕只有 5 机 × 3 域） |
| 审稿人认为这是"工程启发式"而非研究 | 中 | 必须给出**近似比**或**下界**，哪怕只在受限模型下 |
| FlexiRaft/Orca 无开源代码 | 中 | 重实现 + 披露差距；或对比其论文报告值并标注 |
| 写路径性能不达标 | 中 | 先做批处理；否则限制结论在读密集场景 |
| 1000 节点只有模拟结果 | 中 | 明确声明 + 校准曲线 + 偏离点披露 |

---

## 8. 立即可做的下一步

按性价比排序：

1. **实现 FAFT 求解器**（`pkg/faft`）：先做贪心，再做 LP 松弛求下界。
   输入 = 故障域拓扑 + 工作负载，输出 = `(Q1_s, Q2_s, placement_s)`。
2. **用现有 `pkg/model` 算收益上界**：在 n=1000, f=5 下，
   对比"多数 quorum"与"FAFT 几何"的消息数与选主容错度。
   **这一步不需要任何新基础设施，几小时内可出结果，且能直接决定项目是否值得继续。**
3. **写路径批处理**：否则写密集场景的数字没有可比性。
4. **补 `pkg/bench` 与 `pkg/model` 的单元测试**（目前 `[no test files]`）。
5. **架构决策记录**：把 §3 的设计选择写成 ADR，便于后续反复检视。

---

## 附录 A · 关键文献（按用途分组）

**必须引用（本设计的直接依据）**
- Lamport. *Lower Bounds on Consensus.* MSR 2000 / Distributed Computing 19:104–125, 2006.
- Howard, Malkhi, Spiegelman. *Flexible Paxos: Quorum Intersection Revisited.* OPODIS 2016.
- Mao, Junqueira, Marzullo. *Mencius: Building Efficient Replicated State Machines for WANs.* OSDI 2008.
- Ongaro. *Consensus: Bridging Theory and Practice.* Stanford PhD, 2014（§6.4 readIndex）.
- Yadav, Rahut. *FlexiRaft: Flexible Quorums with Raft.* CIDR 2023.

**主要 baseline**
- Dharmawan, Annigeri, Amiri. *Orca: Flexible Quorums Meet Dynamic Quorums.* PVLDB vol 19, 2026.
- Shen, Cui, Sen, Angel, Mu. *Mako.* OSDI 2025, pp. 129–152.
- Huang et al. *TiDB: a Raft-based HTAP database.* PVLDB 13(12):3072–3084, 2020.
- Taft et al. *CockroachDB: The Resilient Geo-Distributed SQL Database.* SIGMOD 2020.
- Xiong et al. *ReCraft: Self-Contained Split, Merge, and Membership Change of Raft.* DSN 2025.

**方法学（必须引用以证明评测严谨）**
- Schroeder, Wierman, Harchol-Balter. *Open versus closed: a cautionary tale.* NSDI 2006.
- Tollman, Park, Ousterhout. *EPaxos Revisited.* NSDI 2021.
- Turkkan, Rodrigues, Kosar, Charapko, Ailijiang, Demirbas. *How to Evaluate Distributed Coordination Systems? — A Survey and Analysis.* IEEE TPDS 37(1):198–212, 2026.
- Cooper et al. *Benchmarking Cloud Serving Systems with YCSB.* SoCC 2010.

**故障模型与可用性**
- Dwork, Lynch, Stockmeyer. *Consensus in the presence of partial synchrony.* JACM 35(2):288–323, 1988.
- Chandra, Toueg. *Unreliable Failure Detectors for Reliable Distributed Systems.* JACM 43(2):225–267, 1996.
- Malkhi, Reiter, Wool. *The Load and Availability of Byzantine Quorum Systems.* SIAM J. Comput. 29(6):1889–1906, 2000.

**副本类型语义（用于 §3.5 的对比表）**
- Google Cloud Spanner 官方文档（witness / read-only replica 语义）
- OceanBase 官方文档（仲裁副本：参与选主与 Prepare，不参与 Accept）
- TiKV PR #12972（witness 引入）、tikv/pd PR #11242（witness 移除，2026-09）
- CockroachDB 文档（non-voting replicas）

## 附录 B · 数据可信度分级

| 级别 | 含义 |
|---|---|
| A | 本仓库可复现的实测（有命令、有数字、有环境） |
| B | 一手文献原文引用（已核对 Crossref / USENIX / arXiv 原页） |
| C | 二手转述（仅作背景，不作论据） |
| D | **未验证** —— 不得写入论文 |

本文档中所有 `[已发表]` 标注均为 B 级及以上；
`[推导]` 为本项目自行推导，**论文中必须同样标注为推导**；
`[本项目实测]` 为 A 级，但需注意单机环境的局限（见 §4.6）。
