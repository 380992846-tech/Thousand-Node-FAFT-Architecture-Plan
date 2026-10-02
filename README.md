# FAFT — 千节点分片共识：故障域感知的弹性 quorum 拓扑

> **FAFT = Failure-Aware Flexible-quorum Topology**
>
> 把「分片副本放在哪些故障域」与「每个分片用多大的 quorum」当作**同一个优化问题**来解。

[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8.svg)](https://go.dev)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)](#验证状态)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](#验证状态)

---

## 这是什么

一个 Multi-Raft 分片键值存储的研究原型，目标是一篇系统会议论文。

**核心命题**（由本仓库的代码算出，见 [`docs/DESIGN.md`](docs/DESIGN.md) §2.3）：

> 弹性 quorum 把稳态写消息数从 1002 压到 4（**167 倍**）的代价，
> 是选举需要 **999/1000** 个副本在线，控制路径可用性从 ~1.0 崩到 **0.9953**
> —— 约 **13 倍**的可达性劣化。
>
> 但决定可达性的**不是 quorum 的大小，而是它的组织结构**：
> 按故障域组织后，用 `|Q2|=2 域 / |Q1|=4 域` 就能把可用性维持在 **0.99999**。

复现：

```bash
go run ./cmd/faftbench cost     # 弹性 quorum 的真实代价
go run ./cmd/faftbench domain   # 按故障域组织的恢复效果
```

---

## 快速开始

```powershell
# 1. 一次性环境准备（本机若无 Go）
pwsh -File scripts\bootstrap-go.ps1

# 2. 构建 + 测试
pwsh -File build.ps1 build
pwsh -File build.ps1 test

# 3. 起一个 2 分片 × 3 副本的集群跑一轮基准
go run ./cmd/kvbench -spec 2x3 -duration 10s -warmup 3s -concurrency 32
```

> **Windows 用户注意**：所有 go 命令必须带 `-p 1`，否则会随机 `0xc0000005` 崩溃。
> 原因与绕法见 [`docs/ROADMAP.md`](docs/ROADMAP.md) §四。`build.ps1` 已内置。

---

## 架构

```
                        ┌──────────────────────────────┐
   客户端 SDK ──────────▶│  路由层（无状态网关）          │
   (区间缓存 + 重试)      │  pkg/router                  │
                        └───────────────┬──────────────┘
                                        │ 按 key → 分片 Leader
                        ┌───────────────▼──────────────┐
                        │  数据集群 · Multi-Raft        │
                        │  Shard 0 … Shard N-1          │
                        │  每分片 n 个 Raft 副本         │
                        │  pkg/raft + pkg/nodeapi       │
                        └───────────────┬──────────────┘
                                        │ 拓扑 / Leader 上报
                        ┌───────────────▼──────────────┐
                        │  元数据层                     │
                        │  pkg/metadata                 │
                        │  etcd 支撑 或 进程内 LocalTopology │
                        └──────────────────────────────┘
```

**分片内的读路径**：`readIndex` 语义 —— Leader 只要在本任期提交过一条日志，
其 commit index 即为当时全集群最大值，可直接在该索引上服务读，
**无需额外网络或磁盘往返、且不引入任何时钟假设**。

---

## 目录结构

| 路径 | 说明 |
|---|---|
| `pkg/raft` | Raft 副本（FSM / 快照 / 成员变更 / readIndex 读） |
| `pkg/nodeapi` | 数据面 HTTP API（原项目缺失） |
| `pkg/metadata` | 拓扑层；`ClusterTopology`（etcd）与 `LocalTopology`（进程内） |
| `pkg/router` | 路由层：真转发 + Leader 提示重试 + 连接池复用 |
| `pkg/client` | 客户端 SDK：区间缓存 + singleflight + 有界重试 |
| `pkg/cluster` | **单进程多分片集群**（实验可复现性的关键） |
| `pkg/bench` | 延迟直方图 + 开环/闭环发压 + 结果序列化 |
| `pkg/model` | 解析成本模型（leader 出口 / 磁盘 / CPU 上限） |
| `pkg/faft` | **FAFT quorum 几何求解器 + 可用性计算器** |
| `third_party/flexiraft` | **hashicorp/raft 的 FPaxos 改造版**（可配置 \|Q1\|/\|Q2\|） |
| `cmd/kvbench` | 一体化实验驱动 |
| `cmd/faftbench` | FAFT 收益与代价分析 |
| `cmd/raftbench` | **共识层微基准**：7 种模式 —— 吞吐 / 突发 / 可用性 / 复制落后 / 故障注入 / `MaxAppendEntries` / 多分片整域故障 |
| `cmd/faultagg` | **节点级 → 域级**：把监控导出的节点故障聚合成域级区间（判据显式参数化） |
| `cmd/faultfit` | **域级 → 模型参数**：拟合成 `pkg/faft` 能吃的 P/Q/K |
| `cmd/simbench` | 离散事件模拟器驱动 |
| `docs/` | [BUGS](docs/BUGS.md) · [DESIGN](docs/DESIGN.md) · [MEASUREMENT](docs/MEASUREMENT.md) · [PAPER-DRAFT](docs/PAPER-DRAFT.md) · [FAULT-DATA](docs/FAULT-DATA.md) · [ROADMAP](docs/ROADMAP.md) |

---

## 文档

- **[docs/BUGS.md](docs/BUGS.md)** —— **42 项**缺陷的完整审计（39 条带编号，BUG-18 缺号；
  另 3 条未编号的新发现）：源码位置、成因、
  发现方式、修复与回归测试
- **[docs/DESIGN.md](docs/DESIGN.md)** —— 论文方向与实验方案：
  文献空白、理论支撑、FAFT 设计、baselines、指标、消融、风险
- **[docs/MEASUREMENT.md](docs/MEASUREMENT.md)** —— **证据分级与本机限制**：
  哪些数字是实测、哪些是解析、时钟地板的实测数据、注入延迟的语义
- **[docs/PAPER-DRAFT.md](docs/PAPER-DRAFT.md)** —— **论文骨架**：把已有结果按章节摆好，
  逐条标注证据等级与产出位置，附审稿人质疑自检清单
- **[docs/WALKTHROUGH.md](docs/WALKTHROUGH.md)** —— **答辩讲解稿**：缺陷审计逐条、
  时钟地板限制、确定性复现的真实范围；开篇纠正三处与事实不符的提法
- **[docs/FAULT-DATA.md](docs/FAULT-DATA.md)** —— **故障参数从哪来**：要挖哪些数据、
  怎么统计、拿不到怎么做敏感性扫描
- **[docs/ROADMAP.md](docs/ROADMAP.md)** —— 已完成 / 未完成 / **验证缺口**，
  以及 **§三之二 机时申请**（要什么、为什么、消掉哪几条 L —— 可直接转给资源管理员）
- **[docs/related-work-notes.md](docs/related-work-notes.md)** —— 顶会调研原始笔记
- **[_research_pdfs/](_research_pdfs/)** —— 调研引用的一手 PDF 与文本

---

## 验证状态

```
go build -p 1 ./...                通过
go vet   -p 1 ./...                通过
go test  -p 1 ./...                通过（21 个包，191 个测试函数 + 11 个 benchmark）
powershell -File build.ps1 race    通过（21 个包全部通过 -race，含 third_party/flexiraft）
```

> 测试数量以 `go test -list '.*' ./...` 的自报为准（191 / 11）。
> 其中**与确定性直接相关的只有 2 个**：`TestWorkloadDeterministic`（同种子 ⇒ 同负载序列）
> 与 `TestEncodeDeterministic`（同输入 ⇒ 同字节，200 次比对），另有 1 个反向对照
> `TestWorkloadDifferentSeedsDiffer`。
>
> ⚠️ **"同种子 ⇒ 同结果"指的是负载序列，不是测量结果。**
> 测量结果（吞吐、延迟分位）**不可能逐位复现** —— 同一配置 5 轮内波动可达 72k–184k ops/s。
> **解析结果**（`pkg/faft` 的可用性、消息数、下界）则**是**逐位可复现的。
> 详见 **[docs/WALKTHROUGH.md](docs/WALKTHROUGH.md)** §3。

测试覆盖：命令编解码与 CRC、快照往返与截断拒绝、并发读写 FSM、
二分查找对全键空间逐一对照、**三副本复制一致性**、
**Leader 硬停后重新选主**、幂等 Bootstrap、非 Leader 拒绝写、
ephemeral 端口解析。

`third_party/flexiraft` 与 `cmd/raftbench` 另有专门的回归测试
（这两个是本项目自己改出来的东西，没有上游测试兜底）：

| 测试 | 守什么 |
|---|---|
| `TestCommitmentDataQuorumArithmetic` | 提交门槛算术：`matched[len-q]`，逐档手算对照 |
| `TestCommitmentFallsBackToMajority` | 未设/越界时**必须**回退到多数 —— 这是"基线与上游逐行等价"的保证 |
| `TestCommitmentRespectsStartIndex` | Raft 提交规则没被破坏（新 leader 必须先复制本任期首条日志） |
| `TestElectionQuorumSizeIsEnforced` | `\|Q1\|` 在选主路径上真的生效：存活 2/3 时，多数能选出、`\|Q1\|=3` 选不出 |
| `TestDataQuorumOneStillCommits` | `\|Q2\|=1` 在真实提交路径上确实推进 commitIndex |
| `TestSweepConfigsSatisfyFPaxos` | sweep 生成的每一组配置都满足 `\|Q1\|+\|Q2\| > n` |
| `TestBuildClusterRejectsUnsafeQuorum` | 不安全组合在建集群前就被拦住 |
| `TestMakeCmdIsUniquePerCall` | payload 不能复用缓冲区（踩过的真坑） |

**未验证的缺口**（详见 [ROADMAP §三](docs/ROADMAP.md)）：

| 缺口 | 说明 |
|---|---|
| ~~`go test -race`~~ | ✅ **已跑通（2026-10）**：装了 MinGW-w64 gcc，`build.ps1 race` 覆盖 21 个包全部通过，**并做了正向对照**（故意制造数据竞争，确认本机 `-race` 真会报 `WARNING: DATA RACE`）——BUG-1 的修复从此有 race detector 独立验证。顺带查出 `build.ps1` 的包清单漏了 5 个包（41 个测试），见 BUG-34 |
| etcd 集成 | `pkg/metadata` 测试全部基于 `LocalTopology`，`ClusterTopology` 未端到端验证 |
| 1000 节点 | `pkg/model` 与 `pkg/faft` 的数字是**解析预测**；端到端实测停在 6 副本，共识层实测停在 n=9。另有一条硬约束：**联合求解 n=21 单次就要 70 秒，n=1000 不可行** |
| 真实多机 | 单机多进程无真实网络分区与跨域 RTT |
| 亚毫秒延迟 | 本机时钟地板 ≈300µs，低于 ~1ms 的延迟读数会读成 0，见 [MEASUREMENT](docs/MEASUREMENT.md) §4.1 |

---

## 实测性能

本机（Windows，单节点 BoltDB，**未开启写批处理**）：

```
BenchmarkGetRaw                 578 ns/op      ← 状态机本身
BenchmarkSet              5,447,163 ns/op      ← 一次 Raft 往返
BenchmarkBarrier          6,672,332 ns/op
```

**诊断结论**：`CommitTimeout` 从 1ms 调到 20ms，Barrier 成本纹丝不动
（5.72 → 5.87 ms）—— **瓶颈是 fsync，不是提交超时**。
用本仓库的模型复算：`b=1, t_fsync≈5.4ms → 185 ops/s`，
与 `BenchmarkSet` 的 ~184 ops/s 一致。

端到端（2 分片 × 3 副本，32 并发）：

| 指标 | 优化前 | 优化后 |
|---|---|---|
| 吞吐 | 396 ops/s | **2,105 ops/s** |
| get p50 | 39.13 ms | **4.09 ms** |
| get p99 | 63 ms | 33.40 ms |
| **put p50** | 99.24 ms | **17.72 ms** |
| **put p99** | 223.52 ms | **22.92 ms** |

两项优化各自解决一个瓶颈：

- **读路径**：实现 Raft §6.4 的 readIndex 语义，避免每个读都落盘一次
  `Barrier`（实测 6.67ms/次）→ 读延迟降约 **10 倍**，且不引入时钟假设。
- **写路径**：把多条命令装进**一条日志条目**（`opBatch`），
  每批只付一次 fsync，实测摊薄 **9.9 命令/fsync** → put p50 降约 **5.6 倍**。

> ⚠️ 这些是**本机单机**数字，不代表真实部署性能。
> 批处理的收益来自并发摊薄，**串行场景不应期待收益**（实测同量级）。
> 本机 fsync 波动较大（同配置不同轮次 4.6~8.1ms/op），
> 因此绝对数字不可跨轮次比较，只能看同轮内的相对关系。

---

## 弹性 quorum 的真实代价与收益（共识层实测）

上面那组是**端到端**数字，它回答不了 FAFT 的核心问题：`|Q1|`/`|Q2|`
这两个整数到底值多少吞吐、多少可用性。所以另建了一套装置
`cmd/raftbench`，直接驱动 [`third_party/flexiraft`](third_party/flexiraft) ——
`hashicorp/raft` 的 FPaxos 改造版。

> **基线为什么可信**：fork 在 `DataQuorumSize=0 && ElectionQuorumSize=0` 时
> 与上游**逐行等价**（`quorumSize()`、`recalculate()` 的回退值都还原成多数）。
> 所以「多数基线」与「FlexiRaft 实验组」跑的是**同一个二进制、同一段提交逻辑**，
> 只差两个整数 —— 实现差异这个混淆变量被彻底排除。

n=5，512 并发，每轮 2s，**5 轮取中位数**，每条 RPC 注入单程延迟：

| 注入延迟 | 多数 (\|Q1\|=3,\|Q2\|=3) | (\|Q1\|=4,\|Q2\|=2) | (\|Q1\|=5,\|Q2\|=1) | 极值比 |
|---|---|---|---|---|
| 0（纯内存介质） | 191,621 | 179,548 | 197,715 | 1.03 |
| 1 ms | 147,653 | 170,270 | 184,765 | 1.25 |
| 2 ms | 93,260 | 110,778 | 188,438 | 2.02 |
| **5 ms** | 41,186 | 50,644 | **166,747** | **4.05** |

- **延迟为 0 时三组没有区别**（区间互相重叠）—— 介质里没有等待，就没有收益。
  这是用来证伪"收益来自实现差异"的对照组。
- 多数 quorum 的吞吐随 RTT 近似反比下滑（192k → 148k → 93k → 41k）；
  `|Q2|=1` 几乎不受影响（198k → 185k → 188k → 167k），
  因为提交路径上完全不等 follower。
- **极值比随 RTT 单调上升**：1.03 → 1.25 → 2.02 → 4.05。
  这是本组实验最强的信号 —— 一条单调曲线，而不是一个单点数字。

**但代价必须一起报**，否则这个数字是误导的：

| 代价 | 实测 |
|---|---|
| 选主可用性 | `|Q1|` 变大后是**阶跃**的：存活副本数 < `|Q1|` 时 **0/7** 成功，≥ 时 **7/7**。`|Q1|=5` 意味着任何一个副本挂掉，集群就失去选主能力 |
| durability | `|Q2|` 就是「ack 那一刻日志所在副本数」的严格下界：实测 `|Q2|=3/2/1` 对应下界 **3/2/1**。`|Q2|=1` 时客户端收到成功的那一刻，数据 **100%** 只存在于 leader 一个副本上 |
| 复制落后 | `|Q2|` ≥ 多数时 leader 与最慢 follower 只差千条量级；`|Q2|=1` 时 leader 一路跑在前面，落后 **3.4 万–9.6 万条**且**无界累积** |
| 可行性条件 | 上面那条**不是宿命**：把 `MaxAppendEntries` 从上游默认的 64 提到 **256**，落后量就从"单调增长"变成"稳定有界"（约 2.5k–4k 条），而**吞吐不变**（150k–174k，噪声内）。零成本的修复，但默认值下确实是个陷阱 |

`|Q2|=1` 不违反安全性（`|Q1|+|Q2|>N` 保证已提交日志不回滚），
但**介质丢失 = 已提交数据丢失**，且 leader 挂掉后集群一直不可用直到它回来。
中段（如 `|Q2|=2`）才是可用的点 —— **而选哪个点取决于故障域结构**，
这正是 `pkg/faft` 求解器在做的事。

```bash
# 一键复现全部（结果落 results/raftbench-*.json）
powershell -ExecutionPolicy Bypass -File scripts\run-raftbench.ps1

# 单跑
go run ./cmd/raftbench -mode bench -n 5 -q1 3 -q2 3 -delay 2ms \
    -ops 200000 -warmup 2000 -concurrency 512 -repeat 3 -duration 2s
go run ./cmd/raftbench -mode avail -n 5 -q1 5 -q2 1 -kill 0 -trials 7
go run ./cmd/raftbench -mode lag   -n 5 -q1 5 -q2 1 -delay 1ms -lagops 1500
```

> ⚠️ **本机时钟地板 ≈ 300µs，跳变最大 4.5ms**（200ms 忙等里只有 142 次跳变）。
> 低于 ~1ms 的延迟读数会读成 0 —— 这不是 bug，是运行环境。
> `raftbench` 会自己测出这个地板并写进结果（`clock_floor_us` /
> `latency_usable`），延迟不可信时明确标注、主指标让给吞吐。
> 全部依据见 **[docs/MEASUREMENT.md](docs/MEASUREMENT.md)**。
> 注入的是**延迟模型**，不是真实网络。

---

## 引用与事实核查

本项目对文献引用做了逐条核对，并纠正了若干广为流传的错误引用：

| 常见引用 | 实际情况 |
|---|---|
| 「leader 带宽 ∝ 1/(n−1)」是本项目发现 | **不是**。Mencius (OSDI'08) §7 原文已写出 |
| 「Dolev-Lynch-Pochos-Strong 的 Ω(min(f²,n²))」 | **无法溯源**。正确结论是 Dolev–Reischuk 的 Ω(n+f²)（认证）/ Ω(nt)（未认证） |
| 「线性化读需要 ≥1 个到 quorum 的往返」是定理 | **未找到任何一手来源**，属民间传说 |
| Tempo 是 NSDI'22 | **EuroSys'21** |
| NOPaxos 是 NSDI | **OSDI'16** |
| MDCC 是 SIGMOD | **EuroSys'13** |
| SpecPaxos 是论文标题 | 是 **NSDI'15 最佳论文**里引入的协议，标题是 *Designing Distributed Systems Using Approximate Synchrony in Data Center Networks* |

---

## 许可

研究原型，尚未选择许可证。
