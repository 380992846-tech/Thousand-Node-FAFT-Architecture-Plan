# 路线图与当前状态（ROADMAP.md）

本文件记录**已完成**与**未完成**的工作，以及每项的验证状态。
诚实标注验证缺口是本文档的主要目的 —— 未验证的东西不得进入论文。

---

## 一、已完成

### 1.1 缺陷修复（27 项）

详见 [`BUGS.md`](BUGS.md)。最严重的三条：

- **BUG-16** 路由层从不转发请求 —— 整个读写路径实际不存在
- **BUG-1** `FSM` 读写用了两把不相干的锁 —— 并发读写 map，未定义行为
- **BUG-21** 客户端出错时无限递归 —— Leader 持续不可用时栈溢出崩溃

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

详见 [`DESIGN.md`](DESIGN.md) §2.3。

---

## 二、未完成（按优先级）

### P0 · 阻塞论文的问题

| # | 任务 | 状态 |
|---|---|---|
| 1 | ~~写路径批处理~~ | ✅ **已完成**（见 §1.3.2）。put p50 99.24ms → 17.72ms |
| 2 | ~~相关故障下的联合优化~~ | ✅ **已完成**（见 §1.6）。求解器已以 quorum 可用性为准则 |
| 3 | **离散事件模拟器** | ❌ 未开始。RQ3 需要外推到 1000 节点，必须有校准曲线与偏离点披露 |
| 4 | **Baseline 重实现** | ❌ 未开始。FlexiRaft (CIDR'23) / Orca (PVLDB'26) / TiKV PD 启发式 |
| 5 | 真实 etcd 端到端 | ❌ 未开始（见 V2） |

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
| V1 | **`go test -race` 未执行** | BUG-1 的修复缺独立验证。本机无 C 编译器（无 gcc/clang/MSVC），`-race` 需要 cgo | 装 TDM-GCC 或 MSVC Build Tools 后 `powershell -File build.ps1 race` |
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
powershell -File build.ps1 race             # 需要 C 编译器（当前缺失，见 V1）
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
```
