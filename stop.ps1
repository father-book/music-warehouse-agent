<#
.SYNOPSIS
    AC-20 一键部署 · 停止全部服务。

.DESCRIPTION
    停止顺序与启动相反：先断入口（Web），再断缓存，最后断库。
    反过来会出现"Web 还活着但库已经没了"，那几秒里的请求全部报错，
    而且用户看到的是"服务挂了"而不是"我在停服务"。

    只停**本项目起的**实例：
      · Web：按 logs\webapp.pid 里的 PID；
      · Redis：只停 127.0.0.1 上本项目的那个（run_redis.py --stop 自己会判）；
      · PG：只停 third_party\pgdata 这个数据目录的实例。
    外部实例（比如 C:\Users\Public\da_pg）不动——停别人的库不是这个脚本的事。

.PARAMETER KeepPg
    保留 PostgreSQL（调试时想反复重启 Web 用）。
#>
[CmdletBinding()]
param([switch]$KeepPg)

$ErrorActionPreference = "Continue"

# Python 子进程输出 UTF-8，PowerShell 5.1 默认按 GBK 解码中文会乱码。
try {
    [Console]::OutputEncoding = [Text.Encoding]::UTF8
    $OutputEncoding = [Text.Encoding]::UTF8
    $env:PYTHONIOENCODING = "utf-8"
} catch { }

$Root    = Split-Path -Parent $MyInvocation.MyCommand.Path
$AgentDir = Join-Path $Root "code\agent"
$PgDir   = Join-Path $Root "third_party\pgsql"
$DataDir = Join-Path $Root "third_party\pgdata"
$LogDir  = Join-Path $Root "third_party\logs"
$Py      = Join-Path $AgentDir "venv\Scripts\python.exe"
$PgPort  = 55432

function Say($m) { Write-Host $m }
function Step($n, $t) { Write-Host ""; Write-Host "[$n] $t" -ForegroundColor Cyan }
function Ok($m)   { Write-Host "    OK  $m" -ForegroundColor Green }
function Warn($m) { Write-Host "    !   $m" -ForegroundColor Yellow }

Say "=============================================================="
Say " Data Agent · 停止服务"
Say "=============================================================="

# ---------------------------------------------------------------- [1] Web
Step 1 "Web"
$pidFile = Join-Path $LogDir "webapp.pid"
$killed = $false
if (Test-Path $pidFile) {
    $wid = (Get-Content $pidFile -Raw).Trim()
    if ($wid -match '^\d+$') {
        $proc = Get-Process -Id ([int]$wid) -ErrorAction SilentlyContinue
        if ($proc) {
            Stop-Process -Id ([int]$wid) -Force -ErrorAction SilentlyContinue
            Ok "已停止 PID $wid"
            $killed = $true
        } else {
            Warn "PID $wid 已不存在（可能已退出）"
        }
    }
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
} else {
    # 没有 pid 文件时按端口找：只在本项目没记录的情况下兜底，
    # 且要和用户确认——杀错别人的 8000 端口进程是很烦的事。
    $conn = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue |
            Select-Object -First 1
    if ($conn) {
        Warn "没有 pid 记录，但 8000 被 PID $($conn.OwningProcess) 占用；未自动停止"
        Warn "需要的话：Stop-Process -Id $($conn.OwningProcess) -Force"
    } else {
        Ok "没有在跑"
    }
}
if (-not $killed) { Ok "无本项目 Web 进程" }

# ---------------------------------------------------------------- [2] Redis
Step 2 "Redis"
if (Test-Path $Py) {
    Push-Location $AgentDir
    try { & $Py -u run_redis.py --stop 2>&1 | ForEach-Object { Say "    $_" } }
    finally { Pop-Location }
} else { Warn "找不到 venv，跳过" }

# ---------------------------------------------------------------- [3] PG
Step 3 "PostgreSQL"
if ($KeepPg) {
    Warn "已跳过（-KeepPg）"
} elseif (Test-Path (Join-Path $PgDir "bin\pg_ctl.exe")) {
    if (Test-Path (Join-Path $DataDir "PG_VERSION")) {
        & (Join-Path $PgDir "bin\pg_ctl.exe") -D $DataDir stop -m fast 2>&1 |
            ForEach-Object { Say "    $_" }
        Ok "third_party\pgdata 实例已停"
    } else {
        Warn "项目内还没有数据目录，跳过"
    }
    # 端口上如果还有别人（外部实例），明确说出来，别让人以为停干净了
    $still = Get-NetTCPConnection -LocalPort $PgPort -State Listen -ErrorAction SilentlyContinue |
             Select-Object -First 1
    if ($still) {
        Warn "$PgPort 仍有进程在听（PID $($still.OwningProcess)）—— 那是外部实例，本脚本不动它"
    }
} else { Warn "third_party\pgsql 不存在，跳过" }

Say ""
Say "=============================================================="
Say " 已停止（外部实例不受影响）"
Say "=============================================================="
