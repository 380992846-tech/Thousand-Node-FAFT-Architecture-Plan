# scripts/bootstrap-go.ps1 — 安装便携 Go 工具链并创建 ASCII 构建联接
#
# 本机原本没有 Go。本脚本做三件事，全部可逆、不改系统环境变量：
#
#   1. 下载官方 Go 压缩包到 D:\dsh-work\go（纯 ASCII 路径，刻意避开项目目录）
#   2. 在 D:\dsh-work\raft1000 创建指向本仓库的目录联接（junction）
#   3. 写入 .build 输出目录并自检一次 `go version`
#
# 为什么要这么做，见 build.ps1 顶部的"三个坑"说明。
#
# 用法：
#   pwsh -File scripts/bootstrap-go.ps1
#   pwsh -File scripts/bootstrap-go.ps1 -Version go1.24.6 -Force

param(
  [string]$Version = 'go1.24.6',
  [string]$Root    = 'D:\dsh-work',
  [switch]$Force
)

$ErrorActionPreference = 'Stop'

function Info($m) { Write-Host "[bootstrap-go] $m" -ForegroundColor Cyan }
function Warn($m) { Write-Host "[bootstrap-go] $m" -ForegroundColor Yellow }
function Die($m)  { Write-Host "[bootstrap-go] $m" -ForegroundColor Red; exit 1 }

# 仓库根 = 本脚本所在目录的上一级
$RepoPath = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Info "仓库路径: $RepoPath"

if ($RepoPath -match '[^\x00-\x7F]') {
  Warn "仓库路径含非 ASCII 字符。Go 工具链在调用子编译器时会按 ANSI 代码页转换路径，"
  Warn "中文目录名会导致 0xc0000005 崩溃。本脚本将通过 ASCII 目录联接绕过。"
}

$GoRoot    = Join-Path $Root 'go'
$AsciiRoot = Join-Path $Root 'raft1000'
$GoExe     = Join-Path $GoRoot 'bin\go.exe'

New-Item -ItemType Directory -Force -Path $Root | Out-Null

# ---- 1. 工具链 ----
if ((Test-Path $GoExe) -and -not $Force) {
  Info "Go 工具链已存在，跳过下载: $GoExe"
} else {
  $zipName = "$Version.windows-amd64.zip"
  $zipPath = Join-Path $Root $zipName
  $url     = "https://go.dev/dl/$zipName"

  if (-not (Test-Path $zipPath) -or $Force) {
    Info "下载 $url"
    Info "（约 85 MB，视网速可能需要几分钟；中断可直接重跑本脚本）"
    Invoke-WebRequest -Uri $url -OutFile $zipPath -TimeoutSec 1800 -UseBasicParsing
    $mb = [math]::Round((Get-Item $zipPath).Length / 1MB, 1)
    Info "下载完成: $mb MB"
  } else {
    Info "复用已下载的压缩包: $zipPath"
  }

  if (Test-Path $GoRoot) { Remove-Item -Recurse -Force $GoRoot }
  Info "解压到 $Root ..."
  Expand-Archive -Path $zipPath -DestinationPath $Root -Force

  if (-not (Test-Path $GoExe)) { Die "解压后未找到 $GoExe" }
  Info "解压完成"
}

& $GoExe version
if ($LASTEXITCODE -ne 0) { Die "go version 执行失败" }

# ---- 2. ASCII 目录联接 ----
if (Test-Path $AsciiRoot) {
  $existing = (Get-Item $AsciiRoot -Force).Target
  if ($existing -and ($existing -ne $RepoPath)) {
    if ($Force) {
      Info "移除旧联接（指向 $existing）"
      cmd /c rmdir "$AsciiRoot" | Out-Null
    } else {
      Die "已存在联接但指向别处: $existing`n加 -Force 可覆盖。"
    }
  }
}

if (-not (Test-Path $AsciiRoot)) {
  Info "创建目录联接: $AsciiRoot -> $RepoPath"
  cmd /c mklink /J "$AsciiRoot" "$RepoPath" | Out-Null
  if (-not (Test-Path $AsciiRoot)) { Die "创建联接失败（可能需要管理员权限，或目标已存在）" }
} else {
  Info "目录联接已存在: $AsciiRoot"
}

# ---- 3. 自检 ----
Info "自检：在联接路径内编译一个最小程序"
$probe = Join-Path $AsciiRoot '.build\probe'
New-Item -ItemType Directory -Force -Path $probe | Out-Null
Set-Content (Join-Path $probe 'go.mod') "module probe`n`ngo 1.22" -Encoding ASCII
Set-Content (Join-Path $probe 'main.go') 'package main

import "fmt"

func main() { fmt.Println("bootstrap-ok") }' -Encoding ASCII

$env:GOTOOLCHAIN = 'local'
$env:GOROOT      = $GoRoot
$env:CGO_ENABLED = '0'

$p = Start-Process -FilePath $GoExe -ArgumentList 'build','-p','1','-o',(Join-Path $probe 'probe.exe'),'./...' `
      -WorkingDirectory $probe -NoNewWindow -PassThru `
      -RedirectStandardOutput (Join-Path $probe 'o.txt') -RedirectStandardError (Join-Path $probe 'e.txt')

if (-not $p.WaitForExit(180000)) { $p.Kill(); Die "自检编译超时" }

if ($p.ExitCode -ne 0) {
  Warn "自检编译失败，stderr:"
  Get-Content (Join-Path $probe 'e.txt') -ErrorAction SilentlyContinue | ForEach-Object { Warn $_ }
  Die "工具链不可用"
}

& (Join-Path $probe 'probe.exe')

Write-Host ""
Info "完成。现在可以运行："
Write-Host "    pwsh -File build.ps1 build"
Write-Host "    pwsh -File build.ps1 test"
Write-Host "    pwsh -File build.ps1 bench"
Write-Host ""
Warn "注意：所有 go 命令必须带 -p 1，否则会随机 0xc0000005 崩溃（详见 build.ps1 注释）。"
