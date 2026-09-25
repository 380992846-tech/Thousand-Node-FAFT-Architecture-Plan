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
| `cmd/kvbench` | 一体化实验驱动 |
| `cmd/faftbench` | FAFT 收益与代价分析 |
| `docs/` | [BUGS](docs/BUGS.md) · [DESIGN](docs/DESIGN.md) · [ROADMAP](docs/ROADMAP.md) |

---

## 文档

- **[docs/BUGS.md](docs/BUGS.md)** —— 27 项缺陷的完整审计：源码位置、成因、
  发现方式、修复与回归测试
- **[docs/DESIGN.md](docs/DESIGN.md)** —— 论文方向与实验方案：
  文献空白、理论支撑、FAFT 设计、baselines、指标、消融、风险
- **[docs/ROADMAP.md](docs/ROADMAP.md)** —— 已完成 / 未完成 / **验证缺口**
- **[docs/related-work-notes.md](docs/related-work-notes.md)** —— 顶会调研原始笔记
- **[_research_pdfs/](_research_pdfs/)** —— 调研引用的一手 PDF 与文本

---

## 验证状态

```
go build -p 1 ./...     通过
go vet   -p 1 ./...     通过
go test  -p 1 ./...     通过
```

测试覆盖：命令编解码与 CRC、快照往返与截断拒绝、并发读写 FSM、
二分查找对全键空间逐一对照、**三副本复制一致性**、
**Leader 硬停后重新选主**、幂等 Bootstrap、非 Leader 拒绝写、
ephemeral 端口解析。

**未验证的缺口**（详见 [ROADMAP §三](docs/ROADMAP.md)）：

| 缺口 | 说明 |
|---|---|
| `go test -race` | 本机无 C 编译器，`-race` 需要 cgo。BUG-1 的修复目前只有逻辑推理 + 并发功能测试支撑 |
| etcd 集成 | `pkg/metadata` 测试全部基于 `LocalTopology`，`ClusterTopology` 未端到端验证 |
| 1000 节点 | `pkg/model` 与 `pkg/faft` 的数字是**解析预测**；端到端实测停在 6 副本 |
| 真实多机 | 单机多进程无真实网络分区与跨域 RTT |

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

端到端（2 分片 × 3 副本，32 并发，读占比 0.9，5s）：

| 指标 | readIndex 优化前 | 优化后 |
|---|---|---|
| 吞吐 | 396 ops/s | **2,014 ops/s** |
| get p50 | 39.13 ms | **4.09 ms** |
| get p99 | 63 ms | 33.40 ms |
| put p50 | 39.91 ms | 99.24 ms |

读延迟降约 **10 倍**。写路径仍受单批 fsync 限制，是 P0 待办。

> ⚠️ 这些是**本机单机**数字，不代表真实部署性能。
> 论文中的所有性能声明必须来自 §4.6 描述的实测 + 校准模拟器流程。

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
