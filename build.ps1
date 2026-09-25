# build.ps1 — 本机 Go 构建/测试辅助脚本
#
# 为什么需要它（三个本机特有的坑，全部已实测确认）：
#
#  坑 1：本机未安装 Go。工具链以便携方式解包到 ASCII 路径 D:\dsh-work\go。
#
#  坑 2：项目路径含非 ASCII 字符（"千节点Raft架构方案"）。Go 在向 compile.exe /
#        link.exe 传递工具链路径时按 ANSI 代码页转换，中文目录名会变成乱码
#        （实测："千节点Raft架构方案" -> "鍗冭妭鐐筊aft鏋舵瀯鏂规"），
#        子进程拿到不存在的路径后 Go 运行时以 0xc0000005 访问违例崩溃。
#        绕法：建一个纯 ASCII 的目录联接（junction）指向项目，在联接里构建。
#
#  坑 3（最关键）：即使路径全是 ASCII，`go build` 在**并发**启动多个编译器进程时
#        仍会随机 0xc0000005 崩溃。实测规律非常明确：
#          - `go build ./pkg/metrics`      成功（单包，仅需一次 compile 调用）
#          - `go build ./pkg/model`        崩溃（纯标准库包，仍崩）
#          - `go build -p 1 ./...`         全部成功
#        根因是 Go 的 build driver 并发 spawn 工具链进程时的竞态（见
#        cmd/go/internal/work.(*Builder).toolID -> os/exec.StartProcess ->
#        syscall.CreateProcess）。**所有 go 命令都必须带 -p 1**，本脚本已内置。
#
# 用法：
#   pwsh -File build.ps1 tools      # 打印工具链环境
#   pwsh -File build.ps1 build      # go build -p 1 ./...
#   pwsh -File build.ps1 vet        # go vet -p 1 ./...
#   pwsh -File build.ps1 test       # go test -p 1 -count=1 ./pkg/... ./cmd/...
#   pwsh -File build.ps1 race       # go test -race -p 1
#   pwsh -File build.ps1 bench      # go test -bench . -benchmem -p 1
#   pwsh -File build.ps1 clean      # 清理构建缓存

param(
  [ValidateSet('build','vet','test','race','bench','tools','clean')]
  [string]$Task = 'build',
  [string]$GoRoot = 'D:\dsh-work\go',
  [string]$AsciiRoot = 'D:\dsh-work\raft1000'
)

$ErrorActionPreference = 'Stop'
$GoExe = Join-Path $GoRoot 'bin\go.exe'

if (-not (Test-Path $GoExe)) {
  Write-Host "未找到 Go 工具链: $GoExe" -ForegroundColor Red
  Write-Host "请先运行: pwsh -File scripts\bootstrap-go.ps1" -ForegroundColor Yellow
  exit 127
}
if (-not (Test-Path $AsciiRoot)) {
  Write-Host "未找到 ASCII 目录联接: $AsciiRoot" -ForegroundColor Red
  Write-Host "请先运行: pwsh -File scripts\bootstrap-go.ps1" -ForegroundColor Yellow
  exit 127
}

$env:GOTOOLCHAIN = 'local'
$env:GOROOT      = $GoRoot
$env:CGO_ENABLED = '0'

$outDir = Join-Path $AsciiRoot '.build'
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

# 受限沙箱下无法用管道捕获子进程输出，一律重定向到文件后再读。
function Invoke-Go {
  param([string[]]$GoArgs, [string]$Name)

  $outFile = Join-Path $outDir "$Name.out"
  $errFile = Join-Path $outDir "$Name.err"

  $proc = Start-Process -FilePath $GoExe -ArgumentList $GoArgs -WorkingDirectory $AsciiRoot `
            -NoNewWindow -PassThru `
            -RedirectStandardOutput $outFile -RedirectStandardError $errFile

  # 加超时保护：Go 的 build driver 偶发挂死，不能无限等。
  if (-not $proc.WaitForExit(600000)) {
    try { $proc.Kill() } catch {}
    Write-Host "== $Name 超时（600s），已终止 ==" -ForegroundColor Red
    return 124
  }
  $proc.Refresh()
  $code = $proc.ExitCode

  $color = if ($code -eq 0) { 'Green' } else { 'Red' }
  Write-Host "== $Name  exit=$code ==" -ForegroundColor $color

  foreach ($f in @($outFile, $errFile)) {
    if (Test-Path $f) {
      $lines = Get-Content $f
      if ($lines.Count -gt 0) { $lines | ForEach-Object { Write-Host $_ } }
    }
  }
  return $code
}

# 所有包（含测试），显式列出以避免 "./..." 在某些 Go 版本上的解析差异。
$allPkgs = @(
  './pkg/bench', './pkg/client', './pkg/cluster', './pkg/metadata',
  './pkg/metrics', './pkg/model', './pkg/nodeapi', './pkg/raft', './pkg/router',
  './cmd/kvbench', './cmd/kvstore-client', './cmd/kvstore-node', './cmd/kvstore-router'
)

switch ($Task) {
  'tools' {
    & $GoExe version
    & $GoExe env GOROOT GOOS GOARCH CGO_ENABLED GOMODCACHE
    Write-Host "项目联接 : $AsciiRoot"
    Write-Host "工具链   : $GoRoot"
  }
  'build' { exit (Invoke-Go @('build','-p','1') + $allPkgs 'build') }
  'vet'   { exit (Invoke-Go @('vet','-p','1')   + $allPkgs 'vet') }
  'test'  { exit (Invoke-Go (@('test','-p','1','-count=1','-timeout','300s') + $allPkgs) 'test') }
  'race'  { exit (Invoke-Go (@('test','-race','-p','1','-count=1','-timeout','600s') + $allPkgs) 'race') }
  'bench' { exit (Invoke-Go (@('test','-p','1','-run=^$','-bench=.','-benchmem','-count=1') + $allPkgs) 'bench') }
  'clean' { exit (Invoke-Go @('clean','-cache','-testcache') 'clean') }
}
