# 对评审意见的逐条回应

> 供直接发送。每条都给了可复现的命令或代码位置，不靠论述。

---

## 1. 「FAFT 求解器、离散事件模拟器、写路径批处理都没实现」

三者都已实现。代码位置如下，命令可直接验证。

**FAFT 求解器**（`pkg/faft`）

| 函数 | 位置 |
|---|---|
| `Solve(t, cfg) []Plan` | `geometry.go:401` |
| `SolveWithFailures(t, cfg, av, targets)` | `solve_failure.go:364` |
| `SolveJoint` / `SolvePlacementOnly` / `ExhaustiveJoint` / `AblationLadder` | `placement.go` |
| 对照 planner：majority / FlexiRaft / Orca(PVLDB'26) / TiKV PD / FAFT / 消融 | `planner.go`、`planner_orca.go` |

**离散事件模拟器**：`pkg/sim/sim.go` 的 `Run` / `Scale`，`calib.go` 的
`Calibrate` / `FitOverheadFactor` / `ApplyFactor`，驱动在 `cmd/simbench`。

**写路径批处理**：`pkg/raft/batch.go` 共 20 个函数
（`encodeBatch` / `decodeBatch` / `applyBatch` / `startBatchLoop` /
`batchLoop` / `flushBatch` / `BatchStats`），有 `batch_test.go`。
端到端实测 put p50 **99.24 → 17.72 ms**。

**复现命令（任一条都能验证）**

```
go build -p 1 ./... && go vet -p 1 ./... && go test -p 1 ./...
        # 12 个包、173 个测试 + 11 个 benchmark 全过

go run ./cmd/faftbench planners      # baseline 对照
go run ./cmd/faftbench ablation      # 消融阶梯（放置 vs 几何）
go run ./cmd/simbench scale          # 模拟器
go run ./cmd/simbench calib          # 校准（结论：未通过）
go run ./cmd/kvbench -spec 2x3 -duration 10s -warmup 3s -concurrency 32
```

> 可能是看的是 `docs/` 而不是代码 —— 文档里确实大量在讨论"应该怎么做"，
> 容易读成立项书。代码侧的入口是 `cmd/` 下 8 个驱动 + `pkg/` 下 12 个包。

---

## 2. 「4.6 的 1000 节点外推只是代码解析预测，没测过」

**同意，而且已经写明**，不是漏了：

- `docs/ROADMAP.md` §三 验证缺口 V3：
  「`pkg/model` 与 `pkg/faft` 的数字是**解析预测**；端到端实测停在 6 副本」
- `docs/MEASUREMENT.md` §7「已知的、尚未测量的东西」
- `docs/DESIGN.md` §4.6

另外 `pkg/faft` 的每条结论在 `pkg/faft/evidence.go` 里都标了证据等级
（`MEASURED` / `ANALYSIS` / `SIMULATED` / `DERIVED`），并逐条写明
"可以主张什么 / 不可以主张什么"。打印：

```
go run ./cmd/faftbench evidence
```

这个分级是为了防止把解析结果写成实测 —— 项目早期犯过一次，现在有测试守着
（`TestAnalysisClaimsDoNotClaimPerformance`）。

---

## 3. 「§4.6 那句'单机跑 500 副本不代表真实多机'别删」

**确认保留**，原文在 `docs/DESIGN.md` §4.6：

> 如实说明：单机多进程与真实多机的差异在于
> （a）无真实网络分区，（b）共享 CPU / 页面缓存 / 磁盘，（c）无真实跨域 RTT。

同一节下面还加了本机时钟地板的量化限制：Go 单调时钟最小跳变 ≈300µs、
200ms 内最大跳变 4.5ms，**低于约 1ms 的延迟读数全部无效**。
每个结果文件里都带 `clock_floor_us` / `latency_usable` 字段。

---

## 4. 「先跑 MaxAppendEntries 滞后复现，看 3.4 万条堆积是否属实」

**已复现，属实。而且关键在于它是单调累积，不是单点数字。**

配置：n=5，`|Q1|=5 |Q2|=1`，注入单程延迟 1ms，512 并发，每轮 2s，5 轮。

| 窗口 | 1 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|
| 落后量（默认 mae=64） | 6,329 | 12,473 | 19,449 | 24,633 | **34,105** |
| 刚重跑的独立复现 | 9,336 | 20,344 | 28,984 | 49,016 | **66,040** |

**单调递增**。对照 `|Q2| ≥ 多数` 的配置，落后量在几百量级波动、无增长趋势：

| 配置 | 5 个窗口的落后量 |
|---|---|
| 延迟 1ms，`\|Q2\|=3` | 157 / 111 / 418 / 476 / 142 |
| 延迟 1ms，`\|Q2\|=2` | 679 / 412 / 47 / 411 / 30 |
| 延迟 5ms，`\|Q2\|=3` | 511 / 297 / 65 / 301 / 89 |

**把 `MaxAppendEntries` 从默认 64 提到 256，单调增长即消失**（吞吐不变）：

| mae | 吞吐（中位数） | 4 轮落后量 |
|---|---|---|
| **64**（上游默认） | 150,090 | 11,430 / 18,310 / 36,614 / **43,206** |
| 128 | 166,462 | 4,444 / 2,448 / 36,169 / 28,041 |
| **256** | 174,201 | 2,827 / 2,399 / 3,350 / 3,978 |
| 1024 | 166,383 | 2,750 / 2,275 / 2,478 / 3,283 |

复现命令：

```
go run ./cmd/raftbench -mode bench -n 5 -q1 5 -q2 1 -delay 1ms -mae 64 \
    -ops 200000 -warmup 2000 -concurrency 512 -repeat 5 -duration 2s -settle 180s
```

> ⚠️ 两点必须一起说：
> 1. **具体数值逐轮波动很大**（34k vs 66k）。可主张的是**趋势**（单调累积），
>    不是某个点的绝对值。
> 2. 介质是**进程内 in-memory**、延迟是**注入**的，不是真实网络。
>    结果 JSON 里带 `injected_delay_us`。

原始数据：`results/raftbench-mae-*.json`、`results/repro-mae64-q2-1-d1ms.json`。
方法论说明：`docs/MEASUREMENT.md` §5.6。

---

## 5. 「少写注释，先搭 Solve() 和 LP 下界」

**`Solve()` 已经有了**（`geometry.go:401`）。**LP 下界是好建议，我去做。**

现状与缺口：

- 现在能给出的"离最优有多远"只有**小规模穷举对照**（`ExhaustiveJoint`，D^n ≤ 20 万）。
  实测 5 组配置下**贪心与穷举完全一致**（同放置、同消息数、同裕度），
  而评估候选数只有穷举的 1/5 ~ 1/30。
- 但 **D=1000 时穷举不可行**（10^15），所以大规模上没有"最优性差距"的量化。
  **LP 下界正好补这个洞**：把放置 + quorum 选择放松成线性规划，
  得到可证的下界，就能对任意规模说"我们的解在最优的 X% 以内"。
- 这会直接强化消融那一节 —— 现在只能靠"A4 > max(A2,A3)"，
  有了下界就能说"距离理论最优还差多少"。

**注释问题接受**：新代码会精简。但请保留已有注释里的两类内容 ——
它们记的不是"代码在做什么"，而是**踩过的坑**与**判据的来源**
（例如为什么比较判据里消息数优先于裕度、为什么放置补齐必须摊开）。
删了会在下一次改动里重犯，而这类错误的表现是"数字看起来合理但结论反了"。
