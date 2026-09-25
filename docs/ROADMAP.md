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

### 1.3 性能优化（读延迟降约 10 倍）

| 指标 | 优化前 | 优化后 |
|---|---|---|
| 吞吐 | 396 ops/s | **2,014 ops/s** |
| get p50 | 39.13 ms | **4.09 ms** |
| get p99 | 63 ms | 33.40 ms |

手段：实现 Raft §6.4 的 readIndex 语义（`ReadBarrier`），
避免在每个读请求上做一次会落盘的 `Barrier`。
**不引入任何时钟假设** —— 这是唯一零时钟假设的线性化读路径。

### 1.4 实验基础设施

| 组件 | 作用 |
|---|---|
| `pkg/cluster` | 单进程内起任意规模分片集群，替代 1000 个 OS 进程的脚本 |
| `pkg/bench` | 对数直方图 + **开环/闭环双模式**发压（避免 coordinated omission） |
| `pkg/model` | 四上限成本模型 + Lamport 精确消息界 + FPaxos 约束 |
| `pkg/faft` | FAFT quorum 几何求解器 + 副本粒度可用性计算器 |
| `cmd/kvbench` | 一体化实验驱动：同命令 + 同种子 => 同结果 |
| `cmd/faftbench` | FAFT 收益与代价分析（`scale` / `cost` / `domain` / `curve` / `avail`） |

### 1.5 方向验证（关键产出）

用 `pkg/faft` 算出的结果**确认了研究方向成立**：

- 弹性 quorum 省 167× 消息的代价是控制路径可用性从 ~1.0 崩到 **0.9953**（约 13 倍劣化）
- 但**按故障域组织** quorum 后，用 `|Q2|=2 域 / |Q1|=4 域` 就能把可用性维持在 **0.99999**
- 决定可达性的是 quorum 的**组织结构**，不是它的大小

详见 [`DESIGN.md`](DESIGN.md) §2.3。

---

## 二、未完成（按优先级）

### P0 · 阻塞论文的问题

| # | 任务 | 为什么阻塞 | 预估 |
|---|---|---|---|
| 1 | **写路径批处理** | 实测 `put p50 = 99.24ms`，受单批 fsync 限制。写密集场景的数字目前没有可比性 | 1–2 周 |
| 2 | **相关故障下的联合优化** | 当前 `pkg/faft` 只做独立域失效假设。而实测表明相关失效**主导**可用性 | 2–3 周 |
| 3 | **离散事件模拟器** | RQ3 需要外推到 1000 节点，必须有校准曲线与偏离点披露 | 3–4 周 |
| 4 | **Baseline 重实现** | FlexiRaft (CIDR'23) / Orca (PVLDB'26) / TiKV PD 启发式。无论文代码则需重实现并披露差距 | 4–6 周 |

### P1 · 论文需要

| # | 任务 | 说明 |
|---|---|---|
| 5 | 分片分裂 / 合并 | 目前 `SplitKeySpace` 只在启动时静态切分 |
| 6 | Leader 均衡调度 | Leader 分布依赖随机选举，无均衡 |
| 7 | 热点检测 | TiKV 的 `balance-hot-region` 无对应实现 |
| 8 | 真实多机实验 | 至少 5 机 × 3 域的跨域 RTT 数据 |
| 9 | `pkg/bench` / `pkg/model` / `pkg/faft` 单元测试 | 目前 `[no test files]`，而它们的输出会进论文表格 |

### P2 · 工程完善

| # | 任务 |
|---|---|
| 10 | read-index 批量合并（当前每个读 goroutine 一次原子读） |
| 11 | 路由层统计接入 Prometheus（当前只有 JSON 端点） |
| 12 | 优雅下线 / drain 流程 |
| 13 | `scripts/*.sh` 在 Windows 下无验证，需重写或标注 |
| 14 | 架构决策记录（ADR） |

---

## 三、验证缺口（**必须补，或必须在论文中声明**）

| # | 缺口 | 影响 | 补齐方式 |
|---|---|---|---|
| V1 | **`go test -race` 未执行** | BUG-1 的修复缺独立验证。本机无 C 编译器（无 gcc/clang/MSVC），`-race` 需要 cgo | 装 TDM-GCC 或 MSVC Build Tools 后 `pwsh -File build.ps1 race` |
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
pwsh -File scripts\bootstrap-go.ps1   # 一次性：装工具链 + 建联接
pwsh -File build.ps1 tools            # 查看环境
pwsh -File build.ps1 build            # go build -p 1 ./...
pwsh -File build.ps1 vet
pwsh -File build.ps1 test
pwsh -File build.ps1 race             # 需要 C 编译器（当前缺失，见 V1）
pwsh -File build.ps1 bench
```

**注意**：编辑本仓库的 UTF-8 源文件时**不要用 PowerShell 的
`Get-Content`/`Set-Content` 做替换** —— 它会把 UTF-8 当 GBK 读入再写回，
产生乱码并把多行合并（本次开发中实际发生过两次）。
请使用支持 UTF-8 的编辑器。

---

## 五、复现命令

```powershell
# 1. 构建
pwsh -File build.ps1 build

# 2. 全量测试（含三副本复制、leader 故障重选）
pwsh -File build.ps1 test

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
