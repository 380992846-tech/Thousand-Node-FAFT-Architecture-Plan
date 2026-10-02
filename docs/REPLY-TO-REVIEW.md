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
        # 12 个包、191 个测试 + 11 个 benchmark 全过

go run ./cmd/faftbench planners      # baseline 对照
go run ./cmd/faftbench ablation      # 消融阶梯（放置 vs 几何）
go run ./cmd/faftbench lowerbound    # 可证下界对照（RQ8，含穷举守门测试在 pkg/faft）
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

**两件都做了。** `Solve()` **本来就有**（`geometry.go:401`；故障感知版本在
`solve_failure.go:364` 的 `SolveWithFailures`）。下界已实现并跑出结果。

### 5.1 下界：已实现（`pkg/faft/lowerbound.go`）

一处必须先讲清楚的技术选择：**严格意义的 LP 在这里不成立**。
可用性是放置与成员选择的**非线性**函数（逐域二项分布卷积 + 区域事件项），
写成 LP 需要先做保守线性化，而线性化之后的界**比直接对非线性目标逐项放宽更松**。
所以实现的是**组合松弛（relaxed lower bound）**，四条松弛逐条写在源码顶部：

① 两条路径分别取上界，不要求 Q1 与 Q2 同时存活（忽略耦合）；
② 忽略域容量与同域堆积；③ 无事件项按最可靠域算；
④ 事件项对成员数的所有分拆取可用性最大值。

三条守门测试（这才是界可信的全部理由）：

| 测试 | 断言 |
|---|---|
| `TestUpperBoundIsSound` | 90 个随机拓扑 × 6 个随机放置 × 全部 q：**真实 ≤ 上界** |
| `TestLowerBoundSoundAgainstBruteForce` | **放置 × 成员子集 × quorum 大小**三者一起穷举求真实最优，断言 **下界 ≤ 真实最优** |
| `TestUpperBoundMonotoneInEventSeverity` | K 越大上界越低 |

**第一条测试第一次运行就抓到一个方向性错误**（BUG-28）：松弛 ④ 初版写成
"成员尽可能摊开"，而区域事件整域打掉时**堆叠更容易活**（只需一个域不被命中）。
方向反了会报出"我们的解**低于**理论最优"这种自相矛盾的数。
如果只写个能跑出数字的实现就交差，这个错误不会被发现。

### 5.2 结果（`bin\faftbench.exe lowerbound`，JSON 在 `results/faftbench-lowerbound.json`）

9 域 / 每分片 5 副本 / 目标 数据 0.999、控制 0.9999，48 个参数点：

| 结论 | 数字 |
|---|---|
| 下界可行 / 不可行 | 36 / 12 |
| **有区分力的点** | **12 / 36** |
| `faft-joint` 达到下界 | **36 / 36** ⇒ 消息数**可证最优** |
| `majority` / `tikv-pd` / `orca` | 各 7 个点达到、12 个点高于下界（中位 3.00×）、17 个点未达可达目标 |
| 副本数扫描 n=3…21 | 下界恒为 2 条消息（`\|Q2\|=1`），多数 quorum 为 `n+1` |

三条一起报，缺一条就会被读错：

1. "36/36 达到下界"是**最强**的结论 —— 但只在这个模型内；
2. **必须同时给 12/36** —— 其余 24 个点上所有可行方法都已达到下界，
   说明**界不够紧**（松弛 ① 最松），不能读成"谁做都一样"；
3. **12 个下界不可行的点上，任何方法的不可行都不是方法缺陷** —— 目标本身不可达。

**诚实的边界**：`D ≤ 12` 时事件项是精确最大；`D > 12` 时拓扑用轮转采样，
下界退回解析界 `1 - H_min/D`，**明显更松**（对 `D=1000, k≤3, q≤20`
甚至退化为"忽略区域事件"）。所以**"n=1000 也最优"目前没有下界支撑**，
这句话不能写。详见 `MEASUREMENT.md` §5.9。

### 5.3 与穷举对照的关系

`ExhaustiveJoint`（D^n ≤ 20 万）仍然保留：它给的是**真实最优**，
而下界给的是"任意规模下的下界"，两者互补。
此前 5 组配置下贪心与穷举完全一致（同放置、同消息数、同裕度），
评估候选数只有穷举的 1/5 ~ 1/30；现在又有下界独立印证这一点。

**注释问题接受**：新代码会精简。但请保留已有注释里的两类内容 ——
它们记的不是"代码在做什么"，而是**踩过的坑**与**判据的来源**
（例如为什么比较判据里消息数优先于裕度、为什么放置补齐必须摊开）。
删了会在下一次改动里重犯，而这类错误的表现是"数字看起来合理但结论反了"。
