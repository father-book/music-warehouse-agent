<#
.SYNOPSIS
    AC-20 一键部署 · 第二步：一条命令起 PostgreSQL + Redis + Web，并自检。

.DESCRIPTION
    "一键部署"的判据不是"脚本没报错"，而是**所有服务都被真的问过一次**：
    PG 能连、审计表在、触发器在岗、Redis 没降级、Doris 通、护栏能跑、
    Web 返回 200 且看得到治理库。这些都在 deploy_smoke.py 里逐项验。

    为什么不用 docker-compose：本机 wsl.exe 被安全策略禁止启动、当前会话
    非管理员、Windows 家庭版没有 Hyper-V —— 三条同时成立，Docker Desktop
    装上也起不来。与其交一份"写了 compose 但从未跑过"的文件，不如把
    依赖收进 third_party 用原生命令起，这样每一条都是真跑过的。

.PARAMETER Port
    Web 端口，默认 8000。

.PARAMETER NoBusiness
    跳过 smoke.py（业务冒烟，要连 Doris 且较慢）。

.EXAMPLE
    .\start.ps1
    .\start.ps1 -Port 8001 -NoBusiness
#>
[CmdletBinding()]
param(
    [int]$Port = 8000,
    [switch]$NoBusiness
)

$ErrorActionPreference = "Stop"

# Python 子进程输出 UTF-8，而 PowerShell 5.1 默认按系统 ANSI(GBK) 解码，
# 中文会变成"閮ㄧ讲鍐掔儫"这种乱码。更坑的是路径比较：SHOW data_directory
# 返回的路径里带中文用户名，解码错位后和 $DataDir 永远对不上，
# 于是"在项目内"被判成"外部实例"。这不只是显示问题。
try {
    [Console]::OutputEncoding = [Text.Encoding]::UTF8
    $OutputEncoding = [Text.Encoding]::UTF8
    $env:PYTHONIOENCODING = "utf-8"
    # psql 在 Windows 上默认按控制台代码页输出，中文路径会变成 κ/ҵ。
    # 显式要 UTF8，PowerShell 这边才解得对。
    $env:PGCLIENTENCODING = "UTF8"
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
function Die($m)  { Write-Host "    失败: $m" -ForegroundColor Red; exit 1 }

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

Say "=============================================================="
Say " Data Agent · 一键启动"
Say "=============================================================="

if (-not (Test-Path (Join-Path $PgDir "bin\pg_ctl.exe"))) {
    Die "third_party\pgsql 不存在。先跑一次: .\setup.ps1"
}
if (-not (Test-Path $Py)) { Die "找不到虚拟环境: $Py" }

# ---------------------------------------------------------------- [1] PostgreSQL
Step 1 "PostgreSQL (127.0.0.1:$PgPort)"
$pgIsReady = Join-Path $PgDir "bin\pg_isready.exe"
$psql      = Join-Path $PgDir "bin\psql.exe"

function Test-PgUp {
    & $pgIsReady -h 127.0.0.1 -p $PgPort -q 2>$null
    return ($LASTEXITCODE -eq 0)
}

if (Test-PgUp) {
    Ok "已在运行"
} else {
    if (-not (Test-Path (Join-Path $DataDir "PG_VERSION"))) {
        Die "数据目录不存在: $DataDir。先跑一次: .\setup.ps1"
    }
    $pidFile = Join-Path $DataDir "postmaster.pid"
    if (Test-Path $pidFile) {
        # 上次非正常退出会留下 pid 文件，pg_ctl 会拒绝启动
        Warn "清理残留 postmaster.pid"
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    }
    $opts = "-p $PgPort -h 127.0.0.1 -c listen_addresses=127.0.0.1"
    # 【2026-11-14 踩坑】不能写成 `pg_ctl ... 2>&1 | Out-Null`。
    # pg_ctl 拉起的 postgres 会继承这条管道的写端：pg_ctl 自己退出后管道仍未关闭，
    # PowerShell 就一直等管道 EOF —— 端口早就在听了、日志也写了"准备接受连接"，
    # 但脚本看起来像死了。改成 Start-Process + 重定向到文件，父进程不碰管道。
    $pgCtl  = Join-Path $PgDir "bin\pg_ctl.exe"
    $pgArgs = @("-D", $DataDir, "-l", (Join-Path $LogDir "postgres.log"), "-o", "`"$opts`"", "-w", "start")
    try {
        $ctl = Start-Process -FilePath $pgCtl -ArgumentList $pgArgs -WindowStyle Hidden `
               -RedirectStandardOutput (Join-Path $LogDir "pgctl.out") `
               -RedirectStandardError  (Join-Path $LogDir "pgctl.err") -PassThru -ErrorAction Stop
        [void]$ctl.WaitForExit(90000)
    } catch {
        # 进程环境里存在大小写重复的变量（如 HTTP_PROXY / http_proxy）时，
        # Start-Process 组环境块会抛 ArgumentException，这时退回直接调用。
        & $pgCtl @pgArgs 2>&1 | Out-Null
    }
    # 不等固定秒数，轮询到真的能连上为止（pg_ctl -w 返回后通常 1 秒内就绪）
    for ($i = 0; $i -lt 20; $i++) {
        if (Test-PgUp) { break }
        Start-Sleep -Milliseconds 500
    }
    if (Test-PgUp) { Ok "已启动" }
    else {
        $lg = Join-Path $LogDir "postgres.log"
        if (Test-Path $lg) { Get-Content $lg -Tail 12 | ForEach-Object { Say "      $_" } }
        Die "启动失败（日志: $lg）"
    }
}

# 诚实报告"现在是哪个数据目录在服务"。端口通 ≠ 用的是项目内那份数据。
#
# 【2026-11-14 踩坑】这里必须先把 stdin 关掉（$null |）。一旦参数传递出了问题，
# 例如 psql 没认出 -tAc、退回交互式提示符，脚本就会静静地停在 "postgres=#" 上等输入——
# 端口早就通了、日志也写了"准备接受连接"，但 start.ps1 看起来像卡死。
# 管道喂一个 $null 之后，交互式 psql 读到 EOF 立刻退出，最差也只是拿不到值，不会挂住。
$dd = $null | & $psql -h 127.0.0.1 -p $PgPort -U postgres -d postgres -tAc "SHOW data_directory" 2>$null
if ($dd) {
    $dd = $dd.Trim()
    # 按结尾匹配而不是逐字相等：盘符 / 大小写 / 中文用户名都不该影响判断。
    # 之前用 StartsWith 逐字比，中文路径解码一错位就永远判成"外部实例"。
    $inside = $dd.Replace("/", "\").ToLower().TrimEnd("\") -like "*\third_party\pgdata"
    # 项目内时不回显 $dd 原文：psql 在 Windows 上输出中文路径仍会丢字节
    # （已设 PGCLIENTENCODING 也不管用，那是控制台代码页的事）。
    # 判定已经用"结尾匹配"做过了，这里没必要再展示一串乱码。
    if ($inside) { Ok "数据目录: third_party\pgdata（项目内）" }
    else { Warn "数据目录: $dd（**不在项目内**，属于外部实例；迁移请跑 .\setup.ps1）" }
}

# ---------------------------------------------------------------- [2] Redis
Step 2 "Redis（项目自带）"
Push-Location $AgentDir
try {
    $out = & $Py -u run_redis.py 2>&1
} finally { Pop-Location }
$redisOk = $false
foreach ($ln in $out) {
    if ($ln -match "已就绪|已在运行|PONG|redis") { $redisOk = $true }
    Say "    $ln"
}
if ($redisOk) { Ok "Redis 可用" } else { Warn "Redis 未确认（会降级为进程内缓存，跨实例共享失效）" }

# ---------------------------------------------------------------- [3] Web
Step 3 "Web (http://127.0.0.1:$Port)"
$pidFile = Join-Path $LogDir "webapp.pid"
$portBusy = $false
try {
    $c = New-Object Net.Sockets.TcpClient
    $c.Connect("127.0.0.1", $Port)
    $c.Close()
    $portBusy = $true
} catch { $portBusy = $false }

if ($portBusy) {
    Ok "端口 $Port 已被占用，复用现有服务"
} else {
    $webLog = Join-Path $LogDir "webapp.log"
    $webErr = Join-Path $LogDir "webapp.err.log"
    $env:PYTHONIOENCODING = "utf-8"
    $p = Start-Process -FilePath $Py `
         -ArgumentList @("-u", "webapp.py", "--port", "$Port") `
         -WorkingDirectory $AgentDir -WindowStyle Hidden `
         -RedirectStandardOutput $webLog -RedirectStandardError $webErr -PassThru
    $p.Id | Set-Content -Path $pidFile -Encoding ascii
    Say "    PID $($p.Id)  日志: $webLog"

    # 等它真的能应答，而不是 sleep 固定秒数
    $ready = $false
    for ($i = 0; $i -lt 40; $i++) {
        Start-Sleep -Milliseconds 500
        try {
            $r = Invoke-WebRequest -Uri "http://127.0.0.1:$Port/api/health" -TimeoutSec 2 -UseBasicParsing
            if ($r.StatusCode -eq 200) { $ready = $true; break }
        } catch { }
        if ($p.HasExited) { break }
    }
    if ($ready) { Ok "已启动并应答" }
    else {
        if ($p.HasExited) {
            Say "    进程已退出，错误输出："
            if (Test-Path $webErr) { Get-Content $webErr -Tail 20 | ForEach-Object { Say "      $_" } }
        }
        Die "Web 未能应答（日志: $webLog）"
    }
}

# ---------------------------------------------------------------- [4] 自检
Step 4 "部署冒烟（依赖逐项验证）"
Push-Location $AgentDir
try {
    & $Py -u deploy_smoke.py --port $Port
    $smokeCode = $LASTEXITCODE
} finally { Pop-Location }

# ---------------------------------------------------------------- [5] 业务冒烟
Step 5 "业务冒烟（护栏 / 执行面 / 拒答）"
if ($NoBusiness) {
    Warn "已跳过（-NoBusiness）"
} else {
    Push-Location $AgentDir
    try {
        & $Py -u smoke.py
        $bizCode = $LASTEXITCODE
    } finally { Pop-Location }
    if ($bizCode -eq 0) { Ok "业务冒烟通过" } else { Warn "业务冒烟未全过（退出码 $bizCode）" }
}

Say ""
Say "=============================================================="
if ($smokeCode -eq 0) {
    Say " 部署可用" -ForegroundColor Green
    Say "=============================================================="
    Say " 地址: http://127.0.0.1:$Port"
    Say " 账号: analyst / analyst_east / engineer / pm   密码: Demo@2026"
    Say " 停止: .\stop.ps1"
} else {
    Say " 未就绪 —— deploy_smoke 有 FAIL 项" -ForegroundColor Red
    Say "=============================================================="
}
Say " 日志: $LogDir"

exit $smokeCode
