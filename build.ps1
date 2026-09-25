# build.ps1 — 在本机运行 Go 构建/测试的辅助脚本
#
# 为什么需要它：
#   1. 本机未安装 Go，项目使用 .tools 下的便携工具链；
#   2. 项目路径含非 ASCII 字符（"千节点Raft架构方案"），Go 在向子进程传递
#      工具链路径时会把该路径转成 ANSI 代码页，导致 compile.exe/link.exe
#      收到乱码路径而崩溃（0xc0000005）。因此构建必须经由一个纯 ASCII 的
#      目录联接（junction）进行。
#   3. 受限沙箱下不能使用管道捕获子进程输出，脚本一律把输出重定向到文件。
#
# 用法：
#   pwsh -File build.ps1 build      # go build ./...
#   pwsh -File build.ps1 vet        # go vet ./...
#   pwsh -File build.ps1 test       # go test ./...
#   pwsh -File build.ps1 race       # go test -race ./...
#   pwsh -File build.ps1 bench      # go test -bench . -benchmem ./pkg/...
#   pwsh -File build.ps1 tools      # 仅打印环境信息

param(
  [ValidateSet('build','vet','test','race','bench','tools')]
  [string]$Task = 'build'
)

$ErrorActionPreference = 'Stop'

# ---- 环境 ----
$GoRoot = 'D:\dsh-work\go'
$GoExe  = Join-Path $GoRoot 'bin\go.exe'
$AsciiRoot = 'D:\dsh-work\raft1000'
$RepoPath  = (Resolve-Path (Join-Path $PSScriptRoot '.')).Path

if (-not (Test-Path $GoExe)) {
  Write-Error "未找到 Go 工具链：$GoExe`n请先运行 scripts/bootstrap-go.ps1 安装便携工具链。"
}
if (-not (Test-Path $AsciiRoot)) {
  Write-Error "未找到 ASCII 目录联接：$AsciiRoot`n请先运行 scripts/bootstrap-go.ps1 创建联接。"
}

$env:GOTOOLCHAIN = 'local'
$env:GOROOT      = $GoRoot
$env:CGO_ENABLED = '0'

$outDir = Join-Path $AsciiRoot '.build'
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

function Invoke-Go {
  param([string[]]$GoArgs, [string]$Name)
  $outFile = Join-Path $outDir "$Name.out"
  $errFile = Join-Path $outDir "$Name.err"
  $p = Start-Process -FilePath $GoExe -ArgumentList $GoArgs -WorkingDirectory $AsciiRoot `
        -NoNewWindow -PassThru -Wait -RedirectStandardOutput $outFile -RedirectStandardError $errFile
  Write-Host "== $Name (exit $($p.ExitCode)) ==" -ForegroundColor Cyan
  if (Test-Path $outFile) { Get-Content $outFile | Write-Host }
  if (Test-Path $errFile) {
    $e = Get-Content $errFile
    if ($e.Count -gt 0) { $e | Write-Host -ForegroundColor Yellow }
  }
  return $p.ExitCode
}

$packages = @(
  './pkg/...',
  './cmd/...'
)

switch ($Task) {
  'tools' {
    & $GoExe version
    & $GoExe env GOROOT GOOS GOARCH CGO_ENABLED
    Write-Host "repo   : $RepoPath"
    Write-Host "ascii  : $AsciiRoot"
  }
  'build' { exit (Invoke-Go @('build', './...') 'build') }
  'vet'   { exit (Invoke-Go @('vet', './...') 'vet') }
  'test'  { exit (Invoke-Go (@('test','-count=1') + $packages) 'test') }
  'race'  { exit (Invoke-Go (@('test','-race','-count=1') + $packages) 'race') }
  'bench' { exit (Invoke-Go (@('test','-run=^$','-bench=.','-benchmem','-count=1') + $packages) 'bench') }
}
