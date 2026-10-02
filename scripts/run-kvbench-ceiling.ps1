# scripts/run-kvbench-ceiling.ps1 — 端到端吞吐天花板的定位实验
#
# 目的：回答"端到端吞吐为什么不随分片数上升"。
#
# 起因：分片扩展实验（run-kvbench-shards.ps1）在**并发 32** 下得到
# 1→32 分片全程 ~3.1–3.5k ops/s 的平线。但这个数字**不能**直接解释成
# "瓶颈在协议/客户端" —— 因为 16x3 在**并发 128** 下跑到了 6,439 ops/s。
# 也就是说并发 32 时根本没打满，那条平线是"没踩到油门"，不是"车到顶了"。
#
# 所以必须再做一步：**在同一并发下**扫分片数，找到真正的天花板，
# 再看这个天花板与分片数有没有关系。
#
# 判据（写进论文时必须一起给）：
#   若各分片数在同一并发下吞吐相同 ⇒ 天花板来自**共享资源**（单进程 / 单盘），
#     与分片数无关 ⇒ 不能主张 Multi-Raft 可扩展性；
#   若吞吐随分片数上升 ⇒ 才谈得上可扩展性。
param(
  [int[]]$Shards = @(1, 4, 16, 32),
  [int]$Replicas = 3,
  [int[]]$Concurrencies = @(64, 128),
  [string]$Duration = '4s',
  [string]$Warmup = '1s',
  [string]$OutDir = 'results',
  [int]$Repeat = 3,
  [int]$MaxRetries = 2
)

$ErrorActionPreference = 'Stop'
$Repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Bin = Join-Path $Repo 'bin\kvbench.exe'
$Out = Join-Path $Repo $OutDir
if (-not (Test-Path $Bin)) { Write-Host "[ceiling] 缺少 $Bin" -ForegroundColor Red; exit 2 }
if (-not (Test-Path $Out)) { New-Item -ItemType Directory -Path $Out | Out-Null }

function Median([double[]]$xs) {
  $ys = $xs | Sort-Object
  return $ys[[int][math]::Floor($ys.Count / 2)]
}

$rows = @()
foreach ($c in $Concurrencies) {
  foreach ($s in $Shards) {
    $spec = "${s}x${Replicas}"
    $thr = @(); $p50 = @(); $rejected = 0; $attempts = 0
    for ($r = 1; $r -le $Repeat; $r++) {
      $ok = $false
      for ($try = 1; $try -le ($MaxRetries + 1); $try++) {
        $attempts++
        $json = Join-Path $Out ("kvbench-ceiling-{0}-c{1}-r{2}.json" -f $spec, $c, $r)
        $log  = Join-Path $Out ("kvbench-ceiling-{0}-c{1}-r{2}.log" -f $spec, $c, $r)
        # ⚠️ PowerShell 陷阱：脚本开头设了 $ErrorActionPreference='Stop'，
        # 而 `*>` 会把子进程的 stderr 变成 ErrorRecord —— 于是**子进程只要往
        # stderr 写一行，整个脚本就被终止**。实测被这个坑断过一次：
        # 一组 c=128 扫到 16x3 就整体退出，表都没打出来。
        # 正确做法：调用原生程序期间临时放开 EAP，拿到退出码后再收回来。
        $ErrorActionPreference = 'Continue'
        & $Bin -spec $spec -duration $Duration -warmup $Warmup -concurrency $c `
            -label ("{0}-c{1}" -f $spec, $c) -out-json $json *> $log
        $childExit = $LASTEXITCODE
        $ErrorActionPreference = 'Stop'
        if ($childExit -ne 0) {
          Write-Host ("[ceiling] {0} c={1} r{2} try{3} 子进程退出码 {4}（看门狗触发时为 9，日志含 goroutine 栈）" -f `
            $spec, $c, $r, $try, $childExit) -ForegroundColor Red
          continue
        }
        $j = Get-Content $json -Encoding UTF8 -Raw | ConvertFrom-Json
        if ([double]$j.error_rate -gt 0.01) {
          # 错误率高的轮次不可比：客户端 500ms 拨号超时会把吞吐压一个数量级。
          # ⚠️ 第一版把重跑的 JSON 写到**同一个路径**，于是那次塌陷的
          # error_classes 被覆盖掉了 —— 塌陷原因就此丢失（实测撞到过一次
          # 99.4% 错误的轮次，等发现时证据已经没了）。
          # 现在把被丢弃的轮次另存为 -rejectedN.json，并当场打印主要错误类。
          $rejJson = Join-Path $Out ("kvbench-ceiling-{0}-c{1}-r{2}-rejected{3}.json" -f $spec, $c, $r, $try)
          $rejLog  = Join-Path $Out ("kvbench-ceiling-{0}-c{1}-r{2}-rejected{3}.log" -f $spec, $c, $r, $try)
          Copy-Item $json $rejJson -Force
          Copy-Item $log  $rejLog  -Force
          $cls = ''
          if ($j.error_classes) {
            $cls = ($j.error_classes.PSObject.Properties |
                    Sort-Object Value -Descending | Select-Object -First 3 |
                    ForEach-Object { "$($_.Name)×$($_.Value)" }) -join ' ; '
          }
          Write-Host ("[ceiling] {0} c={1} r{2} 错误率 {3:P1} 过高，丢弃重跑。主要错误: {4}" -f `
            $spec, $c, $r, $j.error_rate, $cls) -ForegroundColor Yellow
          $rejected++
          continue
        }
        $thr += [double]$j.throughput
        $p50 += [double]$j.latency.p50_ms
        $ok = $true
        break
      }
      if (-not $ok) { Write-Host "[ceiling] $spec c=$c r$r 重试后仍不合格" -ForegroundColor Red }
    }
    if ($thr.Count -eq 0) { continue }
    $rows += [pscustomobject]@{
      Concurrency = $c
      Spec        = $spec
      Shards      = $s
      Throughput  = [math]::Round((Median $thr), 0)
      ThrMin      = [math]::Round(($thr | Measure-Object -Minimum).Minimum, 0)
      ThrMax      = [math]::Round(($thr | Measure-Object -Maximum).Maximum, 0)
      P50Ms       = [math]::Round((Median $p50), 2)
      Rounds      = $thr.Count
      Rejected    = $rejected
    }
    Write-Host ("[ceiling] c={0,-4} {1,-6} 吞吐 {2,8:N0} ops/s p50 {3,6:N2}ms" -f `
      $c, $spec, (Median $thr), (Median $p50))
  }
}

Write-Host ""
Write-Host "=== 端到端吞吐天花板（$Replicas 副本，每档 $Repeat 轮取中位数）==="
Write-Host ""
$fmt = "{0,-12} {1,-8} {2,-13} {3,-15} {4,-9} {5}"
Write-Host ($fmt -f '并发', 'spec', '吞吐(ops/s)', '轮内区间', 'p50(ms)', '轮次(丢弃)')
Write-Host ('-' * 84)
foreach ($r in ($rows | Sort-Object Concurrency, Shards)) {
  Write-Host ($fmt -f $r.Concurrency, $r.Spec, ("{0:N0}" -f $r.Throughput),
    ("{0:N0}-{1:N0}" -f $r.ThrMin, $r.ThrMax), $r.P50Ms,
    ("{0}({1})" -f $r.Rounds, $r.Rejected))
}
Write-Host ""
Write-Host "读法："
Write-Host "  · **同一并发**下横向比才是可扩展性的证据；跨并发比毫无意义"
Write-Host "    （并发 32 与 128 的 16x3 差近一倍，那是「没踩到油门」）。"
Write-Host "  · 若同一并发下各分片数吞吐相同 ⇒ 天花板来自共享资源"
Write-Host "    （单进程压测端 / 单块盘），**不能主张 Multi-Raft 可扩展性**。"
Write-Host "  · 分片多的档位能承受更高并发而不塌（错误率 < 1%）："
Write-Host "    这是「分片数带来的真实好处」，但它是**客户端抗压**层面的，"
Write-Host "    不是协议吞吐层面的，两者不要混为一谈。"
