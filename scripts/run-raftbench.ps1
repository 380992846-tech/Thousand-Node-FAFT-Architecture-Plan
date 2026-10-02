# scripts/run-raftbench.ps1 —— 一键复现 raftbench 的全部实验
#
# 为什么要有这个脚本：
#   raftbench 的每一组数字都必须能被第三方**原样重跑**。散在命令历史里的
#   参数无法复现，所以把实验矩阵固化在这里，结果统一落到 results/。
#
# 用法：
#   powershell -ExecutionPolicy Bypass -File scripts\run-raftbench.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\run-raftbench.ps1 -SkipBuild
#   powershell -ExecutionPolicy Bypass -File scripts\run-raftbench.ps1 -Only sweep
#   -Only 取值：all | sweep | avail | scale | mae | fail | burst | shards
#
# 实验 A（sweep）   quorum 几何 × 注入延迟 —— 收益侧主实验
# 实验 B（avail）   |Q1| 变大后的选主可用性 —— 可用性代价
# 实验 C（scale）   规模维度 n=5/7/9
# 实验 D（mae）     MaxAppendEntries 转折点 —— |Q2| 变小的可行性条件
# 实验 E（fail）    故障注入下的安全性三层判定 —— 论文的底线问题
# 实验 F（shards）  多分片 + 整域故障 —— FAFT 核心主张的直接检验
#
# 全部跑完约 20–30 分钟（本机）。|Q2|=1 的组需要等 follower 追平才能做
# 一致性检查，所以每组末尾会有几十秒到两分钟的"安静时间"，属正常。
#
# 前置：third_party/flexiraft 已就位（本仓库自带），Go 工具链可用。
# 注意：Windows 上所有 go 命令必须带 -p 1（并发编译器会随机 0xc0000005）。

param(
  [string]$GoExe = 'go',
  [switch]$SkipBuild,
  [ValidateSet('all', 'sweep', 'avail', 'scale', 'mae', 'fail', 'burst', 'shards')]
  [string]$Only = 'all'
)

$ErrorActionPreference = 'Stop'

$Repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Results = Join-Path $Repo 'results'
$BinDir = Join-Path $Repo 'bin'
$Exe = Join-Path $BinDir 'raftbench.exe'

if (-not (Test-Path $Results)) { New-Item -ItemType Directory -Path $Results | Out-Null }
if (-not (Test-Path $BinDir)) { New-Item -ItemType Directory -Path $BinDir | Out-Null }

function Step($msg) { Write-Host "[raftbench] $msg" -ForegroundColor Cyan }
function Warn($msg) { Write-Host "[raftbench] $msg" -ForegroundColor Yellow }

# ── 构建 ────────────────────────────────────────────────────────────────
if (-not $SkipBuild) {
  Step "构建 cmd/raftbench（-p 1）"
  & $GoExe build -p 1 -o $Exe ./cmd/raftbench/
  if ($LASTEXITCODE -ne 0) { throw "构建失败" }
}

if (-not (Test-Path $Exe)) { throw "找不到 $Exe，先去掉 -SkipBuild 构建一次" }

# 本机时钟地板会随负载变化，每次跑都重新测（raftbench 自己会测并写进结果）。
# 若地板高于注入延迟的 1/3，延迟分位数就是噪声 —— 脚本会提醒，但不会阻止，
# 因为吞吐仍然可用。

# ── 实验 A：quorum 几何 × 注入延迟（主实验）────────────────────────────
#
# n=5 下扫 (|Q1|,|Q2|)：
#   (3,3) 多数 —— 也是上游 hashicorp/raft 的行为，基线
#   (4,2) FlexiRaft 一档
#   (5,1) FlexiRaft 极值 —— 写路径完全不等 follower
# 注入延迟从 0（纯内存介质）到 5ms（城际 RTT 量级）。
#
# 期望形状（也是要证伪的对象）：
#   延迟越大，|Q2| 变小的收益越大；delay=0 时介质里没有等待，收益应最小。
if ($Only -eq 'all' -or $Only -eq 'sweep') {
  Step "实验 A：quorum 几何 × 注入延迟（n=5）"
  & $Exe -mode sweep -n 5 `
      -ops 200000 -warmup 2000 -concurrency 512 -repeat 5 -duration 2s -settle 180s `
      -delays "0s,1ms,2ms,5ms" `
      -out (Join-Path $Results 'raftbench-sweep-n5.json')
  if ($LASTEXITCODE -ne 0) { throw "实验 A 失败" }
}

# ── 实验 B：大 |Q1| 的代价（可用性侧）──────────────────────────────────
#
# FlexiRaft 的取舍不是免费的：|Q2| 变小必须由 |Q1| 变大补偿。
# 这里量代价：杀掉 leader + k 个节点后，还能不能选出新 leader。
# 判据是算术关系 —— 存活数 < |Q1| 时**必然**失败，所以本实验主要是
# 把这个必然性落到实测上，避免"理论上会更脆弱"这类空口断言。
if ($Only -eq 'all' -or $Only -eq 'avail') {
  Step "实验 B：选举可用性（n=5，杀 1/2 个节点）"
  foreach ($cfg in @(@(3, 3), @(4, 2), @(5, 1))) {
    $q1 = $cfg[0]; $q2 = $cfg[1]
    foreach ($k in @(0, 1)) {
      $tag = "q1$q1-q2$q2-kill$($k + 1)"
      Step "  $tag"
      & $Exe -mode avail -n 5 -q1 $q1 -q2 $q2 -kill $k -trials 7 `
          -payload 64 -timeout 5s -electtimeout 5s `
          -out (Join-Path $Results "raftbench-avail-$tag.json")
      if ($LASTEXITCODE -ne 0) { Warn "  $tag 失败，继续" }
    }
  }
}

# ── 实验 C：规模维度 ────────────────────────────────────────────────────
#
# 消息复杂度随 n 增长的形状：|Q2| 固定、n 变大时，多数 quorum 的确认数
# 随 n 增长，而 |Q2| 固定的配置不增长。这一组是给"规模化"叙事用的。
# 只跑到 n=9：本机是单进程多 goroutine，n 再大测的就是调度器而不是协议了。
if ($Only -eq 'all' -or $Only -eq 'scale') {
  Step "实验 C：规模（n=5,7,9，注入延迟 2ms）"
  foreach ($n in @(5, 7, 9)) {
    # ⚠️ 必须用 [math]::Floor：PowerShell 的 [int]3.5 走**银行家舍入**得 4，
    # 于是 n=7 的多数会被算成 5（应为 4）。这个坑真的踩过一次。
    $maj = [int][math]::Floor($n / 2) + 1
    Step "  n=$n 多数=$maj"
    # (多数, 多数) 与 (n, 1) 两个端点
    foreach ($cfg in @(@($maj, $maj), @($n, 1))) {
      $q1 = $cfg[0]; $q2 = $cfg[1]
      if ($q1 + $q2 -le $n) { continue }
      $tag = "n$n-q1$q1-q2$q2"
      Step "  $tag"
      & $Exe -mode bench -n $n -q1 $q1 -q2 $q2 -delay 2ms `
          -ops 200000 -warmup 2000 -concurrency 512 -repeat 5 -duration 2s -settle 180s `
          -out (Join-Path $Results "raftbench-$tag.json")
      if ($LASTEXITCODE -ne 0) { Warn "  $tag 失败，继续" }
    }
  }
}

# ── 实验 D：|Q2| 变小的可行性条件（MaxAppendEntries 的转折点）──────────
#
# |Q2|=1 让 leader 不必等 follower，但 follower 仍要以某个速率收日志。
# 收不过来的部分只能在 leader 上堆积 —— 表现为"欠债"而不是"更快"。
# 控制变量是 MaxAppendEntries（每条 AppendEntries 最多带几条，上游默认 64）。
#
# 期望形状（也是要证伪的对象）：吞吐随 mae 基本不变，但落后量在某个 mae
# 处从"单调增长"翻转为"稳定有界"。上一轮实测转折点在 128 与 256 之间。
if ($Only -eq 'all' -or $Only -eq 'mae') {
  Step "实验 D：MaxAppendEntries 转折点（n=5, |Q1|=5, |Q2|=1, 注入延迟 5ms）"
  foreach ($mae in @(64, 128, 256, 512, 1024)) {
    $tag = "mae$mae"
    Step "  mae=$mae"
    & $Exe -mode bench -n 5 -q1 5 -q2 1 -delay 5ms -mae $mae `
        -ops 200000 -warmup 2000 -concurrency 512 -repeat 4 -duration 2s -settle 180s `
        -out (Join-Path $Results "raftbench-mae-$tag.json")
    if ($LASTEXITCODE -ne 0) { Warn "  $tag 失败，继续" }
  }
}

# ── 实验 E：故障注入下的安全性 ──────────────────────────────────────
#
# 论文的底线问题：故障切换之后，客户端已经收到「成功」的写还在不在？
# 判据分三层（无丢失 / 无回滚 / 崩溃可恢复），必须分开看 ——
# 混在一起会让任何方案都"不安全"（被杀节点必然缺它死后才确认的写）。
if ($Only -eq 'all' -or $Only -eq 'fail') {
  Step "实验 E：故障注入下的安全性（杀 leader，三层判定）"
  foreach ($cfg in @(@(3, 3), @(4, 2), @(5, 1))) {
    $q1 = $cfg[0]; $q2 = $cfg[1]
    $tag = "q1$q1-q2$q2"
    Step "  $tag"
    & $Exe -mode fail -n 5 -q1 $q1 -q2 $q2 -delay 2ms `
        -failload 3s -failaft 3s -concurrency 64 -timeout 10s -electtimeout 8s -settle 60s `
        -out (Join-Path $Results "raftbench-fail-$tag.json")
    if ($LASTEXITCODE -ne 0) { Warn "  $tag 失败，继续" }
  }
}

# ── 实验 F：多分片 + 整域故障（FAFT 核心主张的直接检验）─────────────
#
# 唯一能检验「决定可达性的是 quorum 的组织结构而不是大小」的形状。
# 副本按 (shard+i) % D 摊到 D 个故障域上，然后杀掉整域，统计还有多少
# 分片可用。判据是「该分片还有没有 leader」，算术界是「存活数 >= |Q1|」。
#
# ⚠️ 规模上限是硬性的：分片数 × 副本数 <= 3000。
# 实测 1000x5 = 5000 实例连续跑到第 7 次会把整台机器打到失去响应
# （不是内存问题 —— 出事时还剩 22.8GB；是选举风暴打满 16 个核）。
# 本模式量的是**比例**，400 分片与 1000 分片给出同一个数，没必要冒险。
if ($Only -eq 'all' -or $Only -eq 'shards') {
  Step "实验 F：多分片 + 整域故障（400 分片）"
  $matrix = @(
    @{N=3; Dm=5; A=2; B=2; K=1}, @{N=3; Dm=5; A=3; B=1; K=1}, @{N=3; Dm=5; A=2; B=2; K=2},
    @{N=5; Dm=5; A=3; B=3; K=1}, @{N=5; Dm=5; A=4; B=2; K=1}, @{N=5; Dm=5; A=5; B=1; K=1},
    @{N=5; Dm=3; A=3; B=3; K=1}, @{N=5; Dm=3; A=4; B=2; K=1},
    @{N=5; Dm=5; A=3; B=3; K=2}, @{N=5; Dm=5; A=4; B=2; K=2}
  )
  foreach ($cfg in $matrix) {
    $tag = "S400-n$($cfg.N)-d$($cfg.Dm)-q1$($cfg.A)-q2$($cfg.B)-kill$($cfg.K)"
    Step "  $tag"
    & $Exe -mode shards -shards 400 -n $cfg.N -domains $cfg.Dm `
        -q1 $cfg.A -q2 $cfg.B -hb 200ms -electto 200ms `
        -shardsteady 1s -shardrecover 10s -killdomains $cfg.K -electtimeout 30s `
        -out (Join-Path $Results "raftbench-shards-$tag.json")
    if ($LASTEXITCODE -ne 0) { Warn "  $tag 失败，继续" }
    Start-Sleep -Seconds 2
  }
}

Step "完成。结果在 $Results"
Get-ChildItem (Join-Path $Results 'raftbench-*.json') | ForEach-Object {
  Write-Host ("  {0}  ({1:N0} 字节)" -f $_.Name, $_.Length)
}
