# 路线图与当前状态（ROADMAP.md）

本文件记录**已完成**与**未完成**的工作，以及每项的验证状态。
诚实标注验证缺口是本文档的主要目的 —— 未验证的东西不得进入论文。

---

## 一、已完成

### 1.1 缺陷修复（37 项：34 条带编号 + 3 条未编号的新发现，编号中 BUG-18 缺号）

详见 [`BUGS.md`](BUGS.md)。最严重的三条：

- **BUG-16** 路由层从不转发请求 —— 整个读写路径实际不存在
- **BUG-1** `FSM` 读写用了两把不相干的锁 —— 并发读写 map，未定义行为
- **BUG-21** 客户端出错时无限递归 —— Leader 持续不可用时栈溢出崩溃

另有两项**分析层**缺陷（BUG-28 / BUG-29，`BUGS.md` 第八节）：
下界的松弛方向写反（会报出"低于理论最优"的自相矛盾结论）、
两个 baseline 同名导致汇总重复计数。两者都不会崩溃，只会让论文数字变好看。

### 1.2 补齐缺失的数据面

原代码只有 `pkg/raft` 这个库，从未暴露过 HTTP 接口。
新增 `pkg/nodeapi`：

- `GET/PUT/DELETE /api/v1/key/{key}` —— 读写入口，非 Leader 返回 503 + `X-Raft-Leader` 提示
- `POST /join` —— 由 Leader 处理成员入组（修复 BUG-4 的错误语义）
- `GET /healthz`、`GET /raft/stats`、`/metrics`

### 1.3 性能优化

#### 1.3.1 读路径：readIndex 语义（读延迟降约 10 倍）

| 指标 | 优化前 | 优化后 |
|---|---|---|
| 吞吐 | 396 ops/s | **2,014 ops/s** |
| get p50 | 39.13 ms | **4.09 ms** |
| get p99 | 63 ms | 33.40 ms |

手段：实现 Raft §6.4 的 readIndex 语义（`ReadBarrier`），
避免在每个读请求上做一次会落盘的 `Barrier`。
**不引入任何时钟假设** —— 这是唯一零时钟假设的线性化读路径。

#### 1.3.2 写路径：批处理（put p50 降约 5.6 倍）

**问题定位**（用基准把瓶颈从"提交超时"里分离出来）：

```
BenchmarkGetRaw                 578 ns/op      ← 状态机本身极快
BenchmarkSet              5,447,163 ns/op
BenchmarkBarrier          6,672,332 ns/op
BenchmarkBarrierVsCommitTimeout: commit=1ms/5ms/20ms → 5.72/6.46/5.87 ms
```

`CommitTimeout` 从 1ms 调到 20ms 数字纹丝不动 ⇒ **瓶颈是 fsync，不是提交超时**。
用 `pkg/model` 复算：`b=1, t_fsync≈5.4ms → 185 ops/s`，与实测 ~184 ops/s 一致。

**手段**：把多条客户端命令装进**一条 Raft 日志条目**（`opBatch`），
每批只付一次 fsync。实测摊薄倍数 **9.9 命令/fsync**。

**关键设计决定 —— 孤条目快路径**：若单条命令也硬等攒批窗口，
轻载下每条写要多付一个窗口的延迟而摊薄为零。实测（batch=8, wait=2ms, 串行）：

| 配置 | 串行延迟 |
|---|---|
| 关闭批处理 | 8.09 ms/op |
| 开启批处理（无快路径） | **11.47 ms/op** ← 纯倒贴 |
| 开启批处理（有快路径） | 8.40 ms/op（与基线同量级） |

**端到端实测**（2 分片 × 3 副本，32 并发，读占比 0.1，10s）：

| 指标 | 批处理前 | 批处理后 |
|---|---|---|
| put p50 | 99.24 ms | **17.72 ms** |
| put p99 | 223.52 ms | **22.92 ms** |
| throughput | ~2,014 ops/s | 2,105 ops/s |

**诚实说明**：批处理的收益来自**并发摊薄**，不是无条件加速。
串行场景不应期待收益（实测同量级）。此外本机 fsync 波动较大
（同一配置不同轮次 4.6~8.1 ms/op），因此**绝对数字不可跨轮次比较**，
只能看同一轮内的相对关系。跨机器可比数据必须固定环境后重测。

### 1.4 实验基础设施

| 组件 | 作用 |
|---|---|
| `pkg/cluster` | 单进程内起任意规模分片集群，替代 1000 个 OS 进程的脚本 |
| `pkg/bench` | 对数直方图 + **开环/闭环双模式**发压（避免 coordinated omission） |
| `pkg/model` | 四上限成本模型 + Lamport 精确消息界 + FPaxos 约束 |
| `pkg/faft` | FAFT quorum 几何求解器 + 故障感知求解器 + 可用性计算器 |
| `cmd/kvbench` | 一体化实验驱动：同命令 + 同种子 => 同结果 |
| `cmd/faftbench` | FAFT 分析（`scale`/`cost`/`domain`/`curve`/`avail`/`solve`/`solve-uniform`/`solve-plan`/`regional`） |
| `third_party/flexiraft` | **hashicorp/raft v1.6.1 的 FPaxos 改造版**：可配置 \|Q1\|/\|Q2\|；未设时与上游逐行等价 |
| `cmd/raftbench` | **共识层微基准**：`bench`/`burst`/`avail`/`lag`/`fail`/`shards`/`sweep` 七种模式，实测 quorum 几何的收益与代价 |
| `cmd/faultagg` | **节点级 → 域级**：把监控导出的节点故障聚合成域级区间；判据显式参数化，带往返自检 |
| `cmd/faultfit` | **域级 → 模型参数**：把域级故障区间拟合成 `{P,Q,K}`，带合成序列自检 |
| `scripts/run-raftbench.ps1` | 一键复现全部 raftbench 实验，结果落 `results/raftbench-*.json` |

### 1.5 方向验证（关键产出）

用 `pkg/faft` 算出的结果**确认了研究方向成立**：

- 弹性 quorum 省 167× 消息的代价是控制路径可用性从 ~1.0 崩到 **0.9953**（约 13 倍劣化）
- 但**按故障域组织** quorum 后，用 `|Q2|=2 域 / |Q1|=4 域` 就能把可用性维持在 **0.99999**
- 决定可达性的是 quorum 的**组织结构**，不是它的大小

### 1.6 故障感知求解器（P0-2 完成）

`SolveWithFailures` 在满足两条路径可用性目标下最小化写消息数 `2|Q2|`，
其中"成员选择"按**边际可用性增益**贪心，而不是按域计数轮转。

**对照实验**（`faftbench regional`）：两组拓扑的逐域失效率完全相同
（1e-5 ~ 1e-2，相差 1000 倍），唯一区别是有无区域事件：

| \|Q2\| | 有区域事件 | | 无区域事件 | |
|---|---|---|---|---|
| | 按域计数 | 按可用性 | 按域计数 | 按可用性 |
| 2 | 0.998990 | **0.999380** | 0.999890 | **0.999980** |
| 3 | 0.999300 | **0.999400** | 0.99999989 | **0.9999999997** |
| 5 | 0.998999 | **0.999400** | 0.99999994 | **1.0000000000** |

两个场景下按可用性贪心都不低于按域计数贪心，最大提升 **+4.1e-4**。

**一条有论文价值的结论**：区域事件下可用性
`= 1 − P(事件命中集合足以打穿 quorum)`，而这个概率随
**quorum 需要存活的副本数**上升而上升。5 个域、事件打掉 3 个域时：

| 配置 | need | 全灭情形 | 可用性 |
|---|---|---|---|
| 3 副本 / 3 域 | 2 | 7/10 | `1 − 0.7q` |
| 4 副本 / 4 域 | 3 | **10/10** | `1 − q` |
| 5 副本 / 5 域 | 3 | 10/10 | `1 − q` |

> **即：相关故障下加大 quorum 会让系统更脆弱，而不是更健壮。**
> 这与"多副本更可靠"的直觉相反，但推导是直的 —— quorum 越大，
> 需要存活的副本越多，事件越容易把它打穿。
>
> 这个结论对论文有直接影响：它说明在相关故障模型下，
> **"用更大的 quorum 换可靠性"这条常规思路是错的**，
> 起决定作用的是 quorum 需要存活的副本数与事件影响域数的关系。

### 1.7 离散事件模拟器（P0-3 完成，但**校准未通过**）

`pkg/sim` 把结论外推到无法真实部署的规模。**定位必须说清楚**：

> 本模拟器建模的是**成本结构**（消息数 / fsync / 字节 / 可用性），
> **不是**性能预测。它不建模 CPU 调度、页面缓存、TCP 拥塞、锁竞争、HTTP 栈。
> 因此只能回答"趋势与量级"，不能回答"能跑多少 QPS"。

三层结果的分工：

| 层面 | 实现 | 可作什么声明 |
|---|---|---|
| 实测 | `pkg/cluster` + `cmd/kvbench` | 绝对性能 |
| 模拟 | `pkg/sim` | **仅趋势** |
| 解析 | `pkg/model` + `pkg/faft` | 成本界 |

**校准结果：未通过。** 按 3 倍容差判据，两个实测样本都在已测规模上超限，
`Calibrate` 报告 `Calibrated=false, DivergencePoint=2`。

这个结论本身就是有价值的产出 —— 它说明**模拟器在唯一能验证的规模上
就已经不可信**。据此从实测拟合出开销因子：

```
实测吞吐平均为模拟上限的 7.26%（范围 4.44%–10.07%，2 个样本）
```

**规模外推**（应用开销因子后，含区间）：

| 分片 | 节点 | 写上限 | 写估计 | 读上限 | 全集群可用性 |
|---|---|---|---|---|---|
| 1 | 3 | 23,704 | 1,720 | 10,000 | 0.998001 |
| 16 | 48 | 379,259 | 27,516 | 160,000 | 0.968491 |
| 334 | 1002 | 7,917,037 | 574,390 | 3,340,000 | **0.512562** |
| 1000 | 3000 | — | — | — | **0.135200** |

> ⚠️ **可用性列的口径**：单分片可用性恒为 0.998001（**与分片数无关**）；
> 表中是**全集群可用性** = 单分片可用性 ^ 分片数，随分片数指数衰减。
> 这两个量极易混淆 —— 本项目在测试与文档中各自踩过一次。

最后一行是本文档里最值得注意的数字：**单分片可用性 99.8% 看着不错，
但 1000 个分片同时可用的概率只有 13.5%**（在分片间独立假设下）。
这是大规模分片部署的真实痛点，也是论文里应当讨论的问题。

**校准过程中暴露的两处错误**（均已修，并各有回归测试）：

1. **用写路径模型预测读吞吐**。初版用 `b/t_fsync` 去预测读，
   但 readIndex 语义下的读**不落盘**。实测读密集吞吐 2014 ops/s，
   而该模型给出的上限只有 370 —— 实测是"上限"的 5.4 倍，
   证明对照对象选错。修法：读写分设上限，并给实测样本加 `ReadRatio`
   字段按读占比选择对照对象。
2. **把模拟上限当成紧的**。修正后写密集样本延迟仍是模拟的 3.2 倍、
   吞吐只有上限的 4.4%。若不做处理，1000 节点上的绝对数字会高出
   一到两个数量级。修法：从实测拟合开销因子并显式应用。

详见 [`DESIGN.md`](DESIGN.md) §2.3。

### 1.8 FlexiRaft 机制的真实测量（`cmd/raftbench` + `third_party/flexiraft`）

**为什么单独做这一层**：端到端基准（`cmd/kvbench`）里，`|Q1|`/`|Q2|` 的效应
会被 HTTP 栈、编解码、BoltDB fsync 全部淹掉。要回答"quorum 几何值多少吞吐"，
必须把共识层单独隔离出来测。

**做法**：把 `hashicorp/raft@v1.6.1` 复制一份到 `third_party/flexiraft`
（29 个非测试文件），只改 5 处让 quorum 大小可配置：

| 位置 | 改动 |
|---|---|
| `config.go` | 新增 `DataQuorumSize` / `ElectionQuorumSize`（**故意不进** `ReloadableConfig`） |
| `commitment.go` | `recalculate()` 用 `matched[len(matched)-q]`，`q` 可配置 |
| `raft.go` | `setupLeaderState` 把 data quorum 传给 `newCommitment` |
| `raft.go` | `quorumSize()` 返回 election quorum（选举 / `verifyLeader` / lease 三处都该用 `\|Q1\|`） |
| `raft.go` | 新增 `dataQuorumSize()` / `voterCount()` |

**基线为什么可信**：两个字段都为 0 时，上面的改动全部回退到上游行为
（`voters/2+1`），与上游**逐行等价**。所以 majority 基线与 FlexiRaft 实验组
跑的是同一个二进制、同一段提交逻辑，只差两个整数 ——
**实现差异这个混淆变量被彻底排除**。这比"重实现一个 FlexiRaft"强得多。

**测出来的东西**（全部落在 `results/raftbench-*.json`）：

| 维度 | 结果 |
|---|---|
| 收益 | 注入单程延迟 5ms 时，`\|Q2\|` 从 3 降到 1 使吞吐从 **41.2k 升到 166.7k ops/s（4.05×）**；极值比随 RTT **单调上升**：0ms 1.03 → 1ms 1.25 → 2ms 2.02 → 5ms 4.05。注入延迟为 0 时三组区间重叠（收益确实来自"不等副本"） |
| 选主可用性代价 | **阶跃**：存活副本数 < `\|Q1\|` 时 0/7 成功，≥ 时 7/7。`\|Q1\|=5` 意味着任何单点故障都让集群失去选主能力 |
| durability 代价 | `\|Q2\|` 就是「`Apply` 返回成功那一刻日志所在副本数」的**严格下界**，实测 3/2/1。`\|Q2\|=1` 时 100% 只在一个副本上 |
| 复制落后量 | `\|Q2\|`≥多数时只差千条量级；`\|Q2\|=1` 时落后 3.4 万–9.6 万条且**无界累积**。**但这个约束可调且零成本**：`MaxAppendEntries` 从上游默认 64 提到 256，落后量变成稳定有界（2.5k–4k 条），吞吐不变（见 MEASUREMENT §5.6） |

**过程中修掉的两个真问题**（都写进了代码注释，因为它们是可复现的陷阱）：

1. **payload 缓冲区复用**：`InmemStore` 与 `InmemTransport` 传的都是引用，
   复用缓冲区会静默改写"已提交"的日志。症状极隐蔽 —— 各节点 `applied`
   条数与 `lastIndex` 完全一致，只有状态指纹 `hash` 不一致。
2. **注入延迟的语义写反了**：早先写成「先等前一个 enqueue，再 `sleep(d)`」，
   于是延迟**累加**，每个 follower 吞吐被钉死在 `1/d`，实测 p50 从 2ms 爆到
   169ms、心跳排不上队、leader 反复因 lease 失效退位重新选举。
   正确语义是 `enqueue_i = max(t_i + d_i, enqueue_{i-1})`（TCP 模型）。

**本机限制**（必须随结果一起读）：Go 单调时钟地板约 **300µs**、跳变最大
**4.5ms**，`time.Sleep(10µs)` 最快也要 521µs 才返回。**低于 ~1ms 的延迟
读数会读成 0。** `raftbench` 启动时自测并写进结果
（`clock_floor_us` / `latency_usable` / `observed_p50_ms`），
不达标时显式标注并把主指标让给吞吐。完整依据见
[`MEASUREMENT.md`](MEASUREMENT.md)。

---

### 1.9 可证下界：写消息数离理论最优有多远（本轮新增）

审稿意见直接点出："消融只能说 A4 比分开优化更好，说不出离最优还差多少。"
没有下界，"我们的解是最优的"这句话没法证。

**产出**：`pkg/faft/lowerbound.go`（relaxed lower bound）+ `cmd/faftbench lowerbound`。

**为什么不是 LP**：可用性是放置与成员选择的**非线性**函数（逐域二项分布卷积 +
区域事件项），写成 LP 必须先保守线性化，而线性化之后的界**比直接对非线性目标
逐项放宽更松**。四条松弛逐条写进源码与论文。

**结果**（9 域 / 5 副本 / 目标 0.999 与 0.9999 / 48 个参数点）：
下界可行 36 / 不可行 12；**有区分力的点 12/36**；
`faft-joint` 在 **36/36** 可行点上达到下界；`majority`/`tikv-pd`/`orca`
中位 **3.00×**；n=3…21 扫描下界恒为 2 条消息而多数 quorum 为 `n+1`。

**守门测试**（缺了它们，下界只是一个"看起来合理"的数字）：

- `TestUpperBoundIsSound`：随机拓扑 × 随机放置 × 全部 q 逐点验证 `真实 ≤ 上界`；
- `TestLowerBoundSoundAgainstBruteForce`：**放置 × 成员子集 × quorum 大小**
  三者一起穷举，断言 `下界 ≤ 真实最优`；
- `TestUpperBoundMonotoneInEventSeverity`。

> **第一次运行就抓到一个方向性错误**（BUG-28）：松弛 ④ 初版写成"成员尽可能摊开"，
> 而区域事件整域打掉时堆叠更容易存活。方向反了会报出"我们的解**低于**理论最优"。
> 这条记在 `BUGS.md` 第八节。

**边界**：精确分拆只覆盖 `D ≤ 12`；`D > 12` 退回解析界 `1 - H_min/D`，明显更松
（`D=1000, k≤3, q≤20` 时退化为"忽略区域事件"）。**"n=1000 也最优"目前没有下界支撑**，
记作 `PAPER-DRAFT.md` 的 L9。详见 [`MEASUREMENT.md`](MEASUREMENT.md) §5.9。

**副产品一：求解器的规模天花板**。生成这张表时发现整条命令要跑 12 分钟 ——
成员选择是 O(n³)（对每个 k 都从空集重新贪心）。改成嵌套贪心后**严格等价**
（两份结果 JSON 离散字段差异 0），n=21 单次求解 561 s → **70 s**，
整条命令 741 s → 52 s。但 n=21 仍要 70 s，**n=1000 的联合求解不可行** ——
论文里 n=1000 的数字只能用 `AnalyzeScale` 的解析外推，两者必须分开表述。
详见 `BUGS.md` BUG-31。

**副产品二：解析结果现在逐位可复现**。可用性卷积原来遍历 Go map，
浮点加法不满足结合律 ⇒ 同一输入两次调用最后几位不同。改成按域下标顺序卷积后
`TestMemberAvailabilityIsBitReproducible` 连打 200 次逐位相等。
注意与"测量结果不可逐位复现"**分开口径** —— 见 `BUGS.md` BUG-32。

---

## 二、未完成（按优先级）

### P0 · 阻塞论文的问题

| # | 任务 | 状态 |
|---|---|---|
| 1 | ~~写路径批处理~~ | ✅ **已完成**（见 §1.3.2）。put p50 99.24ms → 17.72ms |
| 2 | ~~相关故障下的联合优化~~ | ✅ **已完成**（见 §1.6）。求解器已以 quorum 可用性为准则 |
| 3 | ~~离散事件模拟器~~ | ✅ **已完成**（见 §1.7）。**且校准未通过** —— 见该节的诚实结论 |
| 3.5 | ~~可证下界（"离最优多远"）~~ | ✅ **已完成**（见 §1.9）。`pkg/faft/lowerbound.go`，36/36 可行点达到下界；`D>12` 偏松记作 L9 |
| 4 | **Baseline 重实现** | 🟡 **部分完成**。FlexiRaft 的机制已落到 `third_party/flexiraft`（fork hashicorp/raft，quorum 可配置）；Orca / TiKV PD 的**解析对照**已在 `pkg/faft/planner_orca.go`。缺的是 Orca 的端到端实现 |
| 5 | 真实 etcd 端到端 | ❌ 未开始（见 V2） |

### P0.5 · 已在做的实测（本轮新增）

| # | 任务 | 状态 |
|---|---|---|
| 4a | ~~FlexiRaft 的真实测量~~ | ✅ **已完成**。见 §1.8 |
| 4b | Orca (PVLDB'26) 解析对照 | ✅ 已完成（`faftbench planners`）。**论文全文未取得**，参数语义按摘要与引用重述，已在 `Notes()` 与测试中披露 |
| 4c | TiKV PD 启发式对照 | ✅ 已完成，但**结论是"不可直接比"**：PD 是放置调度器，**没有 quorum 几何自由度**，其跨分片优化不在本框架的比较范围内。已在 `planner_orca.go` 的 `Notes()` 中披露 |
| 4d | ~~`go test -race`~~ | ✅ **已完成**：装了 MinGW-w64 gcc，21 个包全部通过 + 正向对照；并查出脚本漏包（BUG-34） |
| 4e | Orca 的端到端实现 | ❌ 未开始 |

### P1 · 论文需要

| # | 任务 | 说明 |
|---|---|---|
| 6 | 分片分裂 / 合并 | 目前 `SplitKeySpace` 只在启动时静态切分 |
| 7 | Leader 均衡调度 | Leader 分布依赖随机选举，无均衡 |
| 8 | 热点检测 | TiKV 的 `balance-hot-region` 无对应实现 |
| 9 | 真实多机实验 | 至少 5 机 × 3 域的跨域 RTT 数据 |
| 10 | ~~`pkg/bench` / `pkg/model` / `pkg/faft` 单元测试~~ | ✅ **已完成**。补测试过程中查出 6 处真实缺陷（见下） |

### P2 · 工程完善

| # | 任务 |
|---|---|
| 11 | read-index 批量合并（当前每个读 goroutine 一次原子读） |
| 12 | 路由层统计接入 Prometheus（当前只有 JSON 端点） |
| 13 | 优雅下线 / drain 流程 |
| 14 | `scripts/*.sh` 在 Windows 下无验证，需重写或标注 |
| 15 | 架构决策记录（ADR） |

### 补测试过程中查出的真实缺陷（6 处）

这些缺陷此前都不会被现有测试发现，因为那些包根本没有测试。

| # | 位置 | 缺陷 | 影响 |
|---|---|---|---|
| 1 | `pkg/bench/histogram.go` | `Percentile` 返回的桶上界**可能超过实测最大值** | 实测 p99.9=1.0034ms > max=1.0000ms，物理上不可能，破坏分位数单调性，会让论文表格自相矛盾 |
| 2 | `pkg/model/model.go` | `OversizingFactor` 用 `(n-1)/2` 当容错目标 | n=1000 时算出 501/500 = 1.00，毫无意义；正确应相对 f=5 得 **83.5**。这是论文动机的核心数字 |
| 3 | `pkg/bench/runner.go` | 预热阶段无操作数上界 | 零成本执行器上 300ms 跑出 658,514 次调用，白烧 CPU |
| 4 | `pkg/faft/failure.go` | `binomTail` 朴素求和在 `i≈np` 处**下溢** | `binomTail(10,1,0.1)` 返回 1e-10，正确值是 0.6513215599 |
| 5 | `pkg/faft/analysis.go` | `AvailabilityAtReplicaLevel` 参数方向反了 | 算的是 `P[最多 n-q 存活]` 而非 `P[至少 q 存活]` |
| 6 | `pkg/faft/analysis.go` | 同一段二项式数学存在**两份实现** | 修了 `failure.go` 的 `binomTail` 却漏了 `analysis.go` 的副本，症状是"看起来修好了但数字没变" |

> ⚠️ **缺陷 4 与 5 会互相抵消**：错误 4 让 `binomTail(n,q,p)` 静默返回
> `P[X ≤ n-q]`，而这恰好是错误 5 想要的表达式。两条错误一路相抵，
> 最终数字大致正确 —— 因此**长期未被任何人发现**。
> 修正后 n=1000、p=1e-4 的输出与修正前完全一致。
>
> 教训：**"数字看起来合理"绝不等于实现正确。** 唯一防线是
> 独立参照实现（`tailPMF`，用解析上尾视角）逐点交叉验证，
> 加上硬编码已知值的回归测试（`TestAvailabilityRegressionKnownValues`）。

### 修正过程中的一次误判（一并记录，因为它本身是个教训）

修这件事的过程中我中途得出了一个**错误结论**，值得写下警示后人：

- 我先测出"朴素求和在 i≈np 处下溢"（真实存在，见缺陷 4）。
- 随后观察到一个现象：修好 `binomTail` 的数值问题后，
  `binomTail(5,5,0.01)` 从 0.6513 变成 1e-10。
  我据此判断"`AvailabilityAtReplicaLevel` 传参方向反了"（缺陷 5，也真实存在）。
- 我随后**两次**修正该函数的公式，两次都引入了**新的**错误
  （先写成 `binomTail(n, q, p)`，再写成 `binomTail(n, n-q+1, p)`），
  每次都靠"独立参照实现对照"才发现。
- 最终正确形式是 `binomTail(n, q, 1-p)`，并收敛到唯一入口
  `survivalProbability()`。

**这里面的真实教训有两条**：
1. 数值 bug 的排查过程中，**每一步修改都必须用独立参照重新验证**，
   不能凭"上一步的现象"直接推断下一步的修法 —— 我第一次的推断方向对，
   但具体公式错了两次。
2. 当同一段数学存在多份实现时（缺陷 6），
   局部修改会造成"看起来改对了但结果没变"或"结果变了但方向又错了"的
   混乱观感。**先把实现收敛成一份，再改。**

最终状态的正确性由三层独立证据支撑：
(a) `tailPMF` 解析上尾视角的逐点交叉验证（7 组参数）；
(b) 5 组闭式/手算已知值的回归测试；
(c) 域粒度与副本粒度在等价设定下的逐点一致性（5 组容错度）。

---

## 三、验证缺口（**必须补，或必须在论文中声明**）

| # | 缺口 | 影响 | 补齐方式 |
|---|---|---|---|
| V1 | ~~**`go test -race` 未执行**~~ | ✅ **已补上（2026-10）**：`winget install BrechtSanders.WinLibs.POSIX.UCRT` 装 MinGW-w64 gcc 后，`build.ps1 race` 覆盖 **21 个包全部通过**（含 `third_party/flexiraft` 与 `pkg/sim`）。**并做了正向对照**：故意写一个数据竞争的测试，确认本机 `-race` 真会报 `WARNING: DATA RACE` —— 否则"通过"与"检测器没生效"无法区分 | 已完成；顺带查出脚本漏包（BUG-34） |
| V1b | **验证脚本漏了 5 个包** | `build.ps1` 的包清单是显式列出的 15 个，仓库加了包没更新 ⇒ `pkg/sim`/`cmd/faultagg`/`cmd/faultfit`/`cmd/raftbench`/`third_party/flexiraft` 共 41 个测试从未被 build/vet/test/race 覆盖，脚本却照常打印"通过" | ✅ 已改成 `./...`（BUG-34） |
| V1c | **求解器的算力天花板** | 联合求解 n=21 单次 70 s（原来 561 s），按趋势 **n=1000 不可行** ⇒ n=1000 的数字只能标注为解析外推 | 需要换算法（如按域计数搜索）或分批求解；当前必须在论文中声明 |
| V2 | **etcd 集成路径未跑通** | `pkg/metadata` 单元测试全部基于 `LocalTopology`；`ClusterTopology` 的 watcher / 事务 / `Ping` 只有静态审查 | 起一个 etcd，跑端到端 |
| V3 | **1000 节点未实测** | `pkg/model` 与 `pkg/faft` 的数字全是解析预测。端到端实测停在 2 分片 × 3 副本 = 6 副本 | 模拟器 + 校准，或真实大规模集群 |
| V4 | **单机多进程不代表真实部署** | 无真实网络分区、共享 CPU/页面缓存/磁盘、无真实跨域 RTT | 补一组真实多机实验 |
| V5 | `scripts/*.sh` 未在本机执行 | bash 脚本，Windows 环境未验证 | 重写为跨平台或明确标注仅 Linux |
| V6 | `pkg/faft` 的可用性对比口径不同 | 域级用 p_d=1e-3，副本级用 p=1e-4。虽对 FAFT 保守，仍需同口径对照 | 补同口径实验 |

---

## 四、本机环境说明

开发过程中踩到三个与本机环境相关、但**会影响任何人在 Windows 上复现**的坑，
已全部固化到脚本里：

1. **本机原本没有 Go**。`scripts/bootstrap-go.ps1` 下载便携工具链到
   `D:\dsh-work\go`（纯 ASCII 路径）。
2. **项目路径含非 ASCII 字符**。Go 向子编译器传路径时按 ANSI 代码页转换，
   中文目录名会变成乱码，子进程拿到不存在的路径后
   Go 运行时以 `0xc0000005` 崩溃。绕法：`D:\dsh-work\raft1000`
   是指向本仓库的目录联接，构建在联接里进行。
3. **`go build` 并发 spawn 编译器时会随机 `0xc0000005` 崩溃**。
   实测规律：单包成功、纯标准库包也崩、`-p 1` 全部成功。
   **所有 go 命令必须带 `-p 1`**，`build.ps1` 已内置。

用法：

```powershell
powershell -File scripts\bootstrap-go.ps1   # 一次性：装工具链 + 建联接
powershell -File build.ps1 tools            # 查看环境
powershell -File build.ps1 build            # go build -p 1 ./...
powershell -File build.ps1 vet
powershell -File build.ps1 test
powershell -File build.ps1 race             # 已可用：脚本自己找 gcc（winget 装的 MinGW-w64）
powershell -File build.ps1 bench
```

**注意 1（脚本编写）**：本仓库的 `.ps1` 脚本必须兼容 **Windows PowerShell 5.1**
（本机默认 shell，没有 PowerShell 7）。踩过的三个语法坑：
数组操作数不能在一条表达式里隐式拼接、函数参数名不能以 `args` 开头、
**注释里不能出现反引号**（它是行续接符，在注释中会吞掉下一行并导致
"Unexpected token '}'" 语法错）。详见 `build.ps1` 顶部注释。

**注意 2（编辑源文件）**：编辑本仓库的 UTF-8 源文件时**不要用 PowerShell 的
`Get-Content`/`Set-Content` 做替换** —— 它会把 UTF-8 当 GBK 读入再写回，
产生乱码并把多行合并（本次开发中实际发生过两次）。
请使用支持 UTF-8 的编辑器。

**注意 3（本机时钟粒度）**：本机 Go 单调时钟是**粗粒度跳变**的，
实测地板约 **300µs**、200ms 忙等内最大跳变 **4.5ms**、
`time.Sleep(10µs)` 最快也要 521µs 才返回。

后果：**任何低于约 1ms 的延迟测量都会读成 0**（`raftbench -dump` 的
`raw_latency_ns` 全是 0 就是这个原因，不是 bug）。

这不是本项目能修的，它来自运行环境（虚拟化时钟）。应对方式：

- `cmd/raftbench` 启动时自测地板并写进结果（`clock_floor_us`）；
- 用 `latency_usable` / `observed_p50_ms` 标记该轮延迟是否可信；
- 延迟不可信时，主指标换成**吞吐**（跨秒窗口，噪声可平均掉）
  或 `derived_avg_latency_ms`（Little 定律反推，只用聚合量）；
- 亚毫秒延迟的结论**一律不作**。

完整数据见 [`MEASUREMENT.md`](MEASUREMENT.md) §4.1。

**注意 4（PowerShell 数值舍入）**：`[int]($n / 2)` 在 PowerShell 里走
**银行家舍入**：`[int]3.5` 得 4，于是 n=7 的多数 quorum 会被算成 5（应为 4），
而 n=5 与 n=9 恰好正确 —— 是个只在奇数上暴露的坑。
正确写法是 `[int][math]::Floor($n / 2) + 1`。
（`scripts/run-raftbench.ps1` 已修并加了注释；本项目的 scale 实验第一次就是这么跑错的。）

---

## 五、复现命令

```powershell
# 1. 构建
powershell -File build.ps1 build

# 2. 全量测试（含三副本复制、leader 故障重选）
powershell -File build.ps1 test

# 3. 性能基准（分离状态机 / Raft 往返 / Barrier 成本）
go test -p 1 -run '^$' -bench . -benchmem ./pkg/raft

# 4. 解析成本模型
go run ./cmd/kvbench -model -spec 1x5 -payload 256 -nic-gbps 1 -fsync-us 100 -batch 64

# 5. FAFT 方向验证
go run ./cmd/faftbench scale     # 收益与代价随 n 的变化
go run ./cmd/faftbench cost      # 副本粒度可用性：弹性 quorum 的真实代价
go run ./cmd/faftbench domain    # 按故障域组织能否恢复控制路径可用性

go run ./cmd/faftbench avail     # 独立失效 vs 相关失效

# 6. 端到端集群实验
go run ./cmd/kvbench -spec 2x3 -duration 10s -warmup 3s -concurrency 32 \
    -mode closed -read-ratio 0.9 -keys 5000 -out-json results/smoke.json

# 7. 共识层微基准（quorum 几何的真实收益与代价）
powershell -ExecutionPolicy Bypass -File scripts\run-raftbench.ps1   # 全部实验
go run ./cmd/raftbench -mode bench -n 5 -q1 3 -q2 3 -delay 5ms \
    -ops 200000 -warmup 2000 -concurrency 512 -repeat 3 -duration 2s
go run ./cmd/raftbench -mode avail -n 5 -q1 5 -q2 1 -kill 0 -trials 7
go run ./cmd/raftbench -mode lag   -n 5 -q1 5 -q2 1 -delay 1ms -lagops 1500

# 8. 故障参数拟合（有真实故障日志时）
go run ./cmd/faultfit -in failures.csv -window 6h -physical 20m -out-json faultparams.json
go run ./cmd/faultfit -synthetic -domains 6 -days 365 -window 6h -trueP 0.004 -trueQ 0.02 -trueK 3  # 自检

# 9. baseline 对照（解析级）
go run ./cmd/faftbench planners  # majority / FlexiRaft / Orca / TiKV PD / FAFT
go run ./cmd/faftbench evidence  # 全部结论的证据等级清单
```
