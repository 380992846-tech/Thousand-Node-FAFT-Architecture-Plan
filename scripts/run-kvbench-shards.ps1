# scripts/run-kvbench-shards.ps1 — 端到端 Multi-Raft 分片扩展实验
#
# 目的：消掉 PAPER-DRAFT 的 L8（"端到端只有 2 分片，规模化主张受限"）。
#
# 为什么这件事重要，而不是"再跑一次同样的实验"：
#   Multi-Raft 的吞吐本应随**分片数**上升（每个分片有自己的 Leader，
#   写负载分散到多个 Raft 组），直到某处饱和。只有把分片数扫一遍，
#   才能说清"饱和点在哪里、是什么限制的"。
#
# 同时必须一起报 **Leader 分布**：随机选举会让少数节点当上大部分分片的
# Leader，于是吞吐上升可能只是"这轮恰好均匀"。位置随机、不报分布，
# 就是把一个混淆变量留在结论里（kvbench 现在把分布写进结果 JSON）。
#
# 用法：
#   powershell -ExecutionPolicy Bypass -File scripts\run-kvbench-shards.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\run-kvbench-shards.ps1 -Shards 1,2,4,8,16,32

param(
  [int[]]$Shards = @(1, 2, 4, 8, 16, 32),
  [int]$Replicas = 3,
  [int]$Concurrency = 128,
  [string]$Duration = '5s',
  [string]$Warmup = '2s',
  [double]$ReadRatio = 0.5,
  [string]$OutDir = 'results',
  # 每个规模重复次数：单轮数字不可比（本机轮间波动 ±30%），报中位数。
  [int]$Repeat = 3,
  # 错误率超标的轮次重跑次数上限。
  [int]$MaxRetries = 2
)

$ErrorActionPreference = 'Stop'
$Repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Bin = Join-Path $Repo 'bin\kvbench.exe'
$Out = Join-Path $Repo $OutDir

if (-not (Test-Path $Bin)) {
  Write-Host "[shards] 缺少 $Bin，先构建： go build -p 1 -o bin\kvbench.exe ./cmd/kvbench/" -ForegroundColor Red
  exit 2
}
if (-not (Test-Path $Out)) { New-Item -ItemType Directory -Path $Out | Out-Null }

Write-Host "[shards] 仓库: $Repo"
Write-Host "[shards] 分片数: $($Shards -join ', ')  副本数: $Replicas  并发: $Concurrency"
Write-Host "[shards] 每档重复 $Repeat 轮，报中位数（本机单轮波动可达 ±30%）"
Write-Host ""

$rows = @()
foreach ($s in $Shards) {
  $spec = "${s}x${Replicas}"
  $thr = @()
  $p50 = @()
  $p99 = @()
  $errs = 0
  $leaderMax = 0
  $leaderMean = 0.0
  $attempts = 0
  $rejected = 0

  for ($r = 1; $r -le $Repeat; $r++) {
    # 有效轮次计数：错误率 > 1% 的轮次**不能**用于比较 ——
    # 客户端侧的 500ms 拨号超时会把吞吐压低一个数量级（实测单分片
    # 并发 32 时出现过 305 ops/s / 44% 错误 vs 3,013 ops/s / 0 错误），
    # 那是"压测客户端扛不住"，不是"集群变慢了"。把它当成集群故障，
    # 或者直接混进中位数，都会得出错误结论。所以丢弃重跑，并记下重试次数。
    $ok = $false
    for ($try = 1; $try -le ($MaxRetries + 1); $try++) {
      $attempts++
      $json = Join-Path $Out ("kvbench-shards-{0}-r{1}.json" -f $spec, $r)
      $log  = Join-Path $Out ("kvbench-shards-{0}-r{1}.log" -f $spec, $r)
      # ⚠️ PowerShell 陷阱：脚本开头设了 $ErrorActionPreference='Stop'，
      # 而 `*>` 会把子进程的 stderr 变成 ErrorRecord —— 于是**子进程只要往
      # stderr 写一行，整个脚本就被终止**（实测在另一组扫描里被这个坑断过）。
      # 调用原生程序期间临时放开 EAP，拿到退出码后再收回来。
      $ErrorActionPreference = 'Continue'
      & $Bin -spec $spec -duration $Duration -warmup $Warmup `
          -concurrency $Concurrency -read-ratio $ReadRatio `
          -label $spec -out-json $json *> $log
      $childExit = $LASTEXITCODE
      $ErrorActionPreference = 'Stop'

      if ($childExit -ne 0) {
        Write-Host "[shards] $spec 第 $r 轮失败（exit $childExit），日志 $log" -ForegroundColor Red
        continue
      }
      $j = Get-Content $json -Encoding UTF8 -Raw | ConvertFrom-Json
      if ([double]$j.error_rate -gt 0.01) {
        $cls = ''
        if ($j.error_classes) {
          $cls = ($j.error_classes.PSObject.Properties |
                  Sort-Object Value -Descending | Select-Object -First 2 |
                  ForEach-Object { "$($_.Name)×$($_.Value)" }) -join ' ; '
        }
        Write-Host ("[shards] {0} r{1} try{2} 错误率 {3:P1} 过高，丢弃重跑。主要错误: {4}" -f `
          $spec, $r, $try, $j.error_rate, $cls) -ForegroundColor Yellow
        $rejected++
        continue
      }

      $thr += [double]$j.throughput
      $p50 += [double]$j.latency.p50_ms
      $p99 += [double]$j.latency.p99_ms
      $errs += [int]$j.errors
      if ($j.leader_max) { $leaderMax = [int]$j.leader_max }
      if ($j.leader_mean) { $leaderMean = [double]$j.leader_mean }
      $ok = $true
      break
    }
    if (-not $ok) {
      Write-Host "[shards] $spec 第 $r 轮重试 $MaxRetries 次后仍不合格，跳过该轮" -ForegroundColor Red
    }
  }

  if ($thr.Count -eq 0) { continue }

  function Median([double[]]$xs) {
    $ys = $xs | Sort-Object
    return $ys[[int][math]::Floor($ys.Count / 2)]
  }

  $rows += [pscustomobject]@{
    Spec        = $spec
    Shards      = $s
    Nodes       = $s * $Replicas
    Throughput  = [math]::Round((Median $thr), 0)
    ThrMin      = [math]::Round(($thr | Measure-Object -Minimum).Minimum, 0)
    ThrMax      = [math]::Round(($thr | Measure-Object -Maximum).Maximum, 0)
    P50Ms       = [math]::Round((Median $p50), 2)
    P99Ms       = [math]::Round((Median $p99), 2)
    Errors      = $errs
    LeaderMax   = $leaderMax
    LeaderMean  = [math]::Round($leaderMean, 2)
    Rounds      = $thr.Count
    Attempts    = $attempts
    Rejected    = $rejected
  }
  Write-Host ("[shards] {0,-8} 吞吐中位数 {1,8:N0} ops/s（{2} 轮）" -f $spec, (Median $thr), $thr.Count)
}

Write-Host ""
Write-Host "=== 端到端 Multi-Raft 分片扩展（$Replicas 副本，并发 $Concurrency，读占比 $ReadRatio）==="
Write-Host ""
$fmt = "{0,-8} {1,-6} {2,-7} {3,-13} {4,-15} {5,-8} {6,-8} {7,-7} {8,-12} {9}"
Write-Host ($fmt -f 'spec', '分片', 'Raft组', '吞吐(ops/s)', '轮内区间', 'p50(ms)', 'p99(ms)', '错误',
  'Leader最忙/均', '轮次(重跑/丢弃)')
Write-Host ('-' * 112)
foreach ($r in $rows) {
  Write-Host ($fmt -f $r.Spec, $r.Shards, $r.Nodes, ("{0:N0}" -f $r.Throughput),
    ("{0:N0}-{1:N0}" -f $r.ThrMin, $r.ThrMax), $r.P50Ms, $r.P99Ms, $r.Errors,
    ("{0}/{1}" -f $r.LeaderMax, $r.LeaderMean),
    ("{0}({1}/{2})" -f $r.Rounds, $r.Attempts, $r.Rejected))
}
Write-Host ""
Write-Host "读法（写进论文时必须一起给）："
Write-Host "  1. 吞吐应随分片数上升直到饱和 —— 报**饱和点**，不要只报最大那一档。"
Write-Host "  2. **单轮数字不可跨档比较**：轮内区间若与档间差距同量级，只能说趋势。"
Write-Host "  3. Leader 最忙/均值 是分布不均衡度的证据：若最忙节点带了远多于均值的分片，"
Write-Host "     吞吐瓶颈可能来自那一台，而不是协议本身。"
Write-Host "  4. 本机是单进程多 Raft 组，共享同一块盘与同一批 CPU 核："
Write-Host "     这是「分片数」的扩展，**不是**「机器数」的扩展（见 MEASUREMENT.md §4.2）。"
Write-Host "  5. 「轮次(重跑/丢弃)」这一列必须看：错误率 > 1% 的轮次会被丢弃重跑，"
Write-Host "     因为那是**客户端 500ms 拨号超时**（压测端扛不住），不是集群变慢。"
Write-Host "     丢弃次数不为 0 说明该档在本机不稳，写论文时要如实标注。"
