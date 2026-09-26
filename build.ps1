# build.ps1 — Go 构建/测试辅助脚本
#
# 兼容 Windows PowerShell 5.1（本机默认 shell）与 PowerShell 7+。
#
# 踩过的两个 PS 5.1 语法坑，记录下来以免重犯：
#
#   1. 不允许多个数组操作数隐式参与一条表达式，例如
#        Invoke-Go @('build','-p','1') + $allPkgs 'build'
#      会被解析成 ('build','-p','1') + ('.out') 之类的怪东西，且报语法错。
#      => 所有参数数组必须在调用前用显式变量组装好再传入。
#
#   2. 函数参数名不能以 args 开头（如 $GoArgs）：它与自动变量 $args
#      前缀冲突，解析器会把 [string[]]$GoArgs 判为非法 token。
#      => 用 $CmdArgs。
#
#   3. 注释里绝对不能出现反引号（Markdown 行内代码标记用别的方式表达）。
#      反引号是 PowerShell 的行续接符，出现在注释里会把下一行吞进注释，
#      导致后续块结构错乱并报 "Unexpected token '}'"。
#      本文件因此全部用普通引号标注代码片段，注释里一个反引号都不放。
#      代码里的续行反引号、以及字符串里的换行转义序列，是合法的，必须保留。
#
# ---------------------------------------------------------------------------
# 三个本机特有的坑（全部已实测确认）
# ---------------------------------------------------------------------------
#
#  坑 1：本机未安装 Go。工具链以便携方式解包到 ASCII 路径。
#
#  坑 2：项目路径含非 ASCII 字符（"千节点Raft架构方案"）。Go 在向
#        compile.exe / link.exe 传递工具链路径时按 ANSI 代码页转换，中文目录名
#        会变成乱码（实测 "千节点Raft架构方案" -> "鍗冭妭鐐筊aft鏋舵瀯鏂规"），
#        子进程拿到不存在的路径后 Go 运行时以 0xc0000005 访问违例崩溃。
#        绕法：建一个纯 ASCII 的目录联接（junction）指向项目，在联接里构建。
#
#  坑 3（最关键）：即使路径全是 ASCII，go build 在**并发**启动多个编译器
#        进程时仍会随机 0xc0000005 崩溃。实测规律非常明确：
#          - go build ./pkg/metrics      成功（单包，只需一次 compile 调用）
#          - go build ./pkg/model        崩溃（纯标准库包，仍崩）
#          - go build -p 1 ./...         全部成功
#        根因是 Go 的 build driver 并发 spawn 工具链进程时的竞态
#        （cmd/go/internal/work.(*Builder).toolID -> os/exec.StartProcess ->
#        syscall.CreateProcess）。**所有 go 命令都必须带 -p 1**，本脚本已内置。
#
# ---------------------------------------------------------------------------
# 用法
# ---------------------------------------------------------------------------
#
#   powershell -File build.ps1 tools      # 打印工具链环境
#   powershell -File build.ps1 build      # go build -p 1 ./...
#   powershell -File build.ps1 vet        # go vet -p 1 ./...
#   powershell -File build.ps1 test       # go test -p 1 -count=1
#   powershell -File build.ps1 race       # go test -race -p 1（需 C 编译器）
#   powershell -File build.ps1 bench      # go test -bench . -benchmem -p 1
#   powershell -File build.ps1 clean      # 清理构建缓存

param(
  [ValidateSet('build', 'vet', 'test', 'race', 'bench', 'tools', 'clean')]
  [string]$Task = 'build',
  [string]$GoRoot = 'D:\dsh-work\go',
  [string]$AsciiRoot = 'D:\dsh-work\raft1000'
)

$ErrorActionPreference = 'Stop'
$GoExe = Join-Path $GoRoot 'bin\go.exe'

if (-not (Test-Path $GoExe)) {
  Write-Host "未找到 Go 工具链: $GoExe" -ForegroundColor Red
  Write-Host "请先运行: powershell -File scripts\bootstrap-go.ps1" -ForegroundColor Yellow
  exit 127
}
if (-not (Test-Path $AsciiRoot)) {
  Write-Host "未找到 ASCII 目录联接: $AsciiRoot" -ForegroundColor Red
  Write-Host "请先运行: powershell -File scripts\bootstrap-go.ps1" -ForegroundColor Yellow
  exit 127
}

$env:GOTOOLCHAIN = 'local'
$env:GOROOT = $GoRoot
$env:CGO_ENABLED = '0'

$outDir = Join-Path $AsciiRoot '.build'
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

# 受限沙箱下无法用管道捕获子进程输出，一律重定向到文件后再读。
function Invoke-Go {
  param([string[]]$CmdArgs, [string]$Name)

  $outFile = Join-Path $outDir ($Name + '.out')
  $errFile = Join-Path $outDir ($Name + '.err')

  $proc = Start-Process -FilePath $GoExe -ArgumentList $CmdArgs -WorkingDirectory $AsciiRoot `
    -NoNewWindow -PassThru `
    -RedirectStandardOutput $outFile -RedirectStandardError $errFile

  # 加超时保护：Go 的 build driver 偶发挂死，不能无限等。
  if (-not $proc.WaitForExit(900000)) {
    try { $proc.Kill() } catch { }
    Write-Host ("== {0} 超时（900s），已终止 ==" -f $Name) -ForegroundColor Red
    return 124
  }
  $proc.Refresh()
  $code = $proc.ExitCode

  if ($code -eq 0) {
    Write-Host ("== {0}  exit=0 ==" -f $Name) -ForegroundColor Green
  }
  else {
    Write-Host ("== {0}  exit={1} ==" -f $Name, $code) -ForegroundColor Red
  }

  foreach ($f in @($outFile, $errFile)) {
    if (Test-Path $f) {
      $lines = Get-Content $f
      if ($lines -and $lines.Count -gt 0) {
        foreach ($l in $lines) { Write-Host $l }
      }
    }
  }
  return $code
}

# 所有包（含测试），显式列出以避免 "./..." 在某些 Go 版本上的解析差异。
$allPkgs = @(
  './pkg/bench', './pkg/client', './pkg/cluster', './pkg/faft', './pkg/metadata',
  './pkg/metrics', './pkg/model', './pkg/nodeapi', './pkg/raft', './pkg/router',
  './cmd/faftbench', './cmd/kvbench', './cmd/kvstore-client',
  './cmd/kvstore-node', './cmd/kvstore-router'
)

$code = 0

switch ($Task) {
  'tools' {
    & $GoExe version
    & $GoExe env GOROOT GOOS GOARCH CGO_ENABLED GOMODCACHE
    Write-Host ("项目联接 : {0}" -f $AsciiRoot)
    Write-Host ("工具链   : {0}" -f $GoRoot)
    $code = 0
  }
  'build' {
    $a = @('build', '-p', '1') + $allPkgs
    $code = Invoke-Go -CmdArgs $a -Name 'build'
  }
  'vet' {
    $a = @('vet', '-p', '1') + $allPkgs
    $code = Invoke-Go -CmdArgs $a -Name 'vet'
  }
  'test' {
    $a = @('test', '-p', '1', '-count=1', '-timeout', '300s') + $allPkgs
    $code = Invoke-Go -CmdArgs $a -Name 'test'
  }
  'race' {
    # -race 需要 cgo，即需要一个 C 编译器（gcc / clang / MSVC）。
    # 本机当前没有，因此这条命令预计会失败并给出 "C compiler not found"。
    $env:CGO_ENABLED = '1'
    $a = @('test', '-race', '-p', '1', '-count=1', '-timeout', '600s') + $allPkgs
    $code = Invoke-Go -CmdArgs $a -Name 'race'
  }
  'bench' {
    $a = @('test', '-p', '1', '-run=^$', '-bench=.', '-benchmem', '-count=1') + $allPkgs
    $code = Invoke-Go -CmdArgs $a -Name 'bench'
  }
  'clean' {
    $a = @('clean', '-cache', '-testcache')
    $code = Invoke-Go -CmdArgs $a -Name 'clean'
  }
}

exit $code
