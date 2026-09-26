# scripts/git-push.ps1 — 把本地提交推到 GitHub（网络恢复后跑这个）
#
# 为什么单独写个脚本：
#   本机 git 没有配置 credential helper，直接 push 会报
#   "could not read Username for 'https://github.com'"。
#   凭据其实存在 Windows 凭据管理器里（LegacyGeneric:target=git:https://github.com），
#   但需要显式指定用户名才能命中。本脚本用 git credential fill 取出凭据，
#   通过 http.extraHeader 传递（凭据不写入 remote URL、不进 reflog、不落盘）。
#
# 用法：
#   powershell -File scripts\git-push.ps1
#   powershell -File scripts\git-push.ps1 -Message "自定义提交信息"   # 先提交再推
#
# 网络不稳时它不会破坏本地状态：push 失败时本地提交依然完整。

param(
  # 可选：提交信息。给了就先 git add -A && git commit，再 push。
  [string]$Message,
  [string]$Remote = 'origin',
  [string]$Branch = 'main',
  [string]$User   = '380992846-tech'
)

$ErrorActionPreference = 'Stop'
$env:GIT_TERMINAL_PROMPT = '0'

# 仓库根 = 本脚本所在目录的上一级
$Repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Write-Host "[git-push] 仓库: $Repo" -ForegroundColor Cyan

# ---- 1. 可选的提交 ----
if ($Message) {
  Write-Host "[git-push] 暂存并提交..." -ForegroundColor Cyan
  git -C $Repo add -A
  $staged = git -C $Repo diff --cached --name-only
  if ($staged) {
    git -C $Repo -c user.name=$User -c user.email="$User@users.noreply.github.com" commit -q -m $Message
    Write-Host "[git-push] 已提交: $Message"
  } else {
    Write-Host "[git-push] 没有需要提交的改动"
  }
}

# ---- 2. 看看有没有东西要推 ----
$local  = (git -C $Repo rev-parse HEAD).Trim()
$upstream = (git -C $Repo rev-parse "$Remote/$Branch" 2>$null)
if ($upstream) { $upstream = $upstream.Trim() }

if ($upstream -eq $local) {
  Write-Host "[git-push] 本地与 $Remote/$Branch 已一致（$local），无需推送" -ForegroundColor Green
  exit 0
}

$ahead = git -C $Repo rev-list --count "$Remote/$Branch..HEAD" 2>$null
Write-Host "[git-push] 本地领先 $ahead 个提交，准备推送" -ForegroundColor Yellow

# ---- 3. 取凭据 ----
$cred = "protocol=https`nhost=github.com`nusername=$User`n`n" | git credential fill 2>$null
$tok = ($cred | Where-Object { $_ -match '^password=' }) -replace '^password=',''

if (-not $tok) {
  Write-Host "[git-push] 未能从凭据管理器取到 token。" -ForegroundColor Red
  Write-Host "           检查：cmdkey /list | findstr github" -ForegroundColor Red
  Write-Host "           本地提交完好，不影响后续重试。" -ForegroundColor Yellow
  exit 2
}

# ---- 4. 推送 ----
# 凭据通过 http.extraHeader 传递：不写进 remote URL，不留痕。
$auth = "Authorization: Basic " + [Convert]::ToBase64String(
          [Text.Encoding]::ASCII.GetBytes("${User}:${tok}"))

Write-Host "[git-push] 推送到 $Remote/$Branch ..." -ForegroundColor Cyan
git -C $Repo `
    -c "http.extraHeader=$auth" `
    -c http.postBuffer=52428800 `
    -c http.lowSpeedLimit=1000 `
    -c http.lowSpeedTime=120 `
    push $Remote $Branch 2>&1 | ForEach-Object { Write-Host $_ }

$code = $LASTEXITCODE
$tok = $null; $cred = $null; $auth = $null

if ($code -ne 0) {
  Write-Host "[git-push] 推送失败（exit $code）。" -ForegroundColor Red
  Write-Host "           网络恢复后重跑本脚本即可；本地提交完好无损。" -ForegroundColor Yellow
  exit $code
}

Write-Host "[git-push] 推送成功。" -ForegroundColor Green
git -C $Repo log --oneline -3
