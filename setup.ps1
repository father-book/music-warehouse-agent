<#
.SYNOPSIS
    AC-20 一键部署 · 第一步：把 PostgreSQL 收进项目，并准备好数据目录。

.DESCRIPTION
    为什么需要这一步：
      原来 PG 装在 C:\Users\Public\da_pg —— **项目目录之外**。
      换台机器、或者把仓库 clone 到别处，整个治理面（审计 + 成本台账）
      就不存在了，而"一键部署"的前提恰恰是"依赖都在项目里"。
      Redis 早就是这么做的（third_party/redis-win64，2.4MB）。

    为什么不像 Redis 那样直接塞进 git：
      PG 的 bin+lib+share 有 124MB，进仓库会让 clone 变慢、还会撞
      GitHub 的单文件上限。所以走"二进制放 gitignore、用脚本准备"的路子：
      仓库里只有脚本，拿到仓库的人跑一次 setup 就有了。

    两种来源：
      1. -FromInstalled（默认）：从本机已有安装复制，离线、几秒钟；
      2. 没有本机安装时，从 EDB 官方下 zip（约 300MB），慢但能自举。

.PARAMETER Fresh
    不接管现有数据，initdb 一个空库（审计历史会没有）。

.PARAMETER Port
    PG 端口，默认 55432（与 code/agent/config.yaml 的 governance.dsn 一致）。

.EXAMPLE
    .\setup.ps1                    # 接管现有数据，保留审计历史
    .\setup.ps1 -Fresh             # 空库重来
    .\setup.ps1 -Force             # 二进制已存在也重新复制
#>
[CmdletBinding()]
param(
    [int]$Port = 55432,
    [switch]$Fresh,
    [switch]$Force,
    [string]$SourcePg = "C:\Users\Public\da_pg\pgsql",
    [string]$SourceData = "C:\Users\Public\da_pg\data"
)

$ErrorActionPreference = "Stop"

# Python 子进程输出 UTF-8，PowerShell 5.1 默认按 GBK 解码中文会乱码。
try {
    [Console]::OutputEncoding = [Text.Encoding]::UTF8
    $OutputEncoding = [Text.Encoding]::UTF8
    $env:PYTHONIOENCODING = "utf-8"
} catch { }

$Root   = Split-Path -Parent $MyInvocation.MyCommand.Path
$PgDir  = Join-Path $Root "third_party\pgsql"
$DataDir = Join-Path $Root "third_party\pgdata"
$LogDir = Join-Path $Root "third_party\logs"
$Py     = Join-Path $Root "code\agent\venv\Scripts\python.exe"

function Say($msg) { Write-Host $msg }
function Step($n, $t) { Write-Host ""; Write-Host "[$n] $t" -ForegroundColor Cyan }
function Ok($msg)   { Write-Host "    OK  $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "    !   $msg" -ForegroundColor Yellow }
function Die($msg)  { Write-Host "    失败: $msg" -ForegroundColor Red; exit 1 }

# robocopy 的退出码 0~7 都算成功，>=8 才是真失败。直接判 $LASTEXITCODE 会误报。
function Copy-Tree($src, $dst) {
    if (-not (Test-Path $src)) { Die "源目录不存在: $src" }
    New-Item -ItemType Directory -Force -Path $dst | Out-Null
    robocopy $src $dst /E /NFL /NDL /NJH /NJS /NP /R:1 /W:1 | Out-Null
    if ($LASTEXITCODE -ge 8) { Die "复制失败($LASTEXITCODE): $src -> $dst" }
    $global:LASTEXITCODE = 0
}

Say "=============================================================="
Say " AC-20 一键部署 · 准备阶段"
Say "=============================================================="

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# ---------------------------------------------------------------- [1] 二进制
Step 1 "PostgreSQL 二进制 -> third_party\pgsql"
$havePg = (Test-Path (Join-Path $PgDir "bin\postgres.exe")) -and
          (Test-Path (Join-Path $PgDir "bin\initdb.exe")) -and
          (Test-Path (Join-Path $PgDir "bin\pg_ctl.exe"))

if ($havePg -and -not $Force) {
    Ok "已存在，跳过（要重装加 -Force）"
} else {
    $srcPostgres = Join-Path $SourcePg "bin\postgres.exe"
    if (Test-Path $srcPostgres) {
        Say "    从本机安装复制: $SourcePg"
        # 只复制运行必需的三个目录。include(16MB)/doc(19MB)/pgAdmin 用不到，
        # 复制它们只是让"项目为什么会大"这个问题更难回答。
        foreach ($sub in @("bin", "lib", "share")) {
            Say "      - $sub"
            Copy-Tree (Join-Path $SourcePg $sub) (Join-Path $PgDir $sub)
        }
        Ok "二进制就位"
    } else {
        Warn "本机没有现成安装，改为从 EDB 官方下载（约 300MB，需要联网）"
        $ver  = "16.4-1"
        $url  = "https://get.enterprisedb.com/postgresql/postgresql-$ver-windows-x64-binaries.zip"
        $zip  = Join-Path $env:TEMP "pgsql-$ver.zip"
        $tmp  = Join-Path $env:TEMP "pgsql-unzip"
        try {
            Say "    下载 $url"
            [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
            Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
            Say "    解压..."
            if (Test-Path $tmp) { Remove-Item $tmp -Recurse -Force }
            Expand-Archive -Path $zip -DestinationPath $tmp -Force
            # zip 里是 pgsql/ 一层，取它的三个子目录
            foreach ($sub in @("bin", "lib", "share")) {
                Copy-Tree (Join-Path $tmp "pgsql\$sub") (Join-Path $PgDir $sub)
            }
            Remove-Item $zip -Force -ErrorAction SilentlyContinue
            Remove-Item $tmp -Recurse -Force -ErrorAction SilentlyContinue
            Ok "下载并解压完成"
        } catch {
            Die "下载失败: $($_.Exception.Message)`n    可手动下载 $url 解压后放到 third_party\pgsql"
        }
    }
}

$binPg = Join-Path $PgDir "bin\postgres.exe"
$verOut = & $binPg --version
Ok $verOut

# ---------------------------------------------------------------- [2] 数据目录
Step 2 "数据目录 -> third_party\pgdata"
$needInit = $false
if ((Test-Path (Join-Path $DataDir "PG_VERSION")) -and -not $Force) {
    Ok "已存在（版本 $(Get-Content (Join-Path $DataDir 'PG_VERSION'))），保留现有数据"
} elseif ((-not $Fresh) -and (Test-Path (Join-Path $SourceData "PG_VERSION"))) {
    # 接管现有数据：先把它停干净，再整目录复制。
    # 运行中的 PG 数据文件是"看起来完整、实际不一致"的——必须停。
    Say "    接管现有数据: $SourceData"
    $pidFile = Join-Path $SourceData "postmaster.pid"
    if (Test-Path $pidFile) {
        Say "    停止正在运行的旧实例..."
        & (Join-Path $PgDir "bin\pg_ctl.exe") -D $SourceData stop -m fast 2>$null | Out-Null
        Start-Sleep -Seconds 2
        if (Test-Path $pidFile) {
            # pg_ctl 停不掉就直说，别硬复制出一个坏副本
            Die "旧实例没停下来。先手动停掉 55432 上的 postgres 再重试"
        }
        Ok "旧实例已停"
    }
    Say "    复制数据（约 90MB）..."
    Copy-Tree $SourceData $DataDir
    # 运行态文件不该跟着搬
    foreach ($f in @("postmaster.pid", "postmaster.opts")) {
        $p = Join-Path $DataDir $f
        if (Test-Path $p) { Remove-Item $p -Force -ErrorAction SilentlyContinue }
    }
    Ok "数据接管完成（原目录未删除，仍是完整备份）"
} else {
    $needInit = $true
}

if ($needInit) {
    Say "    initdb 全新数据目录（-Fresh 或没有可接管的数据）"
    $pwFile = Join-Path $env:TEMP "pgpw.txt"
    # 本地 trust 认证：只监听 127.0.0.1，够用且不需要管密码。
    & (Join-Path $PgDir "bin\initdb.exe") -D $DataDir -U postgres -E UTF8 `
        --locale=C --auth-local=trust --auth-host=trust 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { Die "initdb 失败" }
    Ok "initdb 完成"
}

# ---------------------------------------------------------------- [3] 启动
Step 3 "启动 PostgreSQL ($Port)"
$pgLog = Join-Path $LogDir "postgres.log"

function Test-PgUp {
    & (Join-Path $PgDir "bin\pg_isready.exe") -h 127.0.0.1 -p $Port -q 2>$null
    return ($LASTEXITCODE -eq 0)
}

if (Test-PgUp) {
    Ok "端口 $Port 已有实例在服务，直接复用"
} else {
    $pidFile = Join-Path $DataDir "postmaster.pid"
    if (Test-Path $pidFile) {
        Warn "数据目录有残留 postmaster.pid，清理后重试"
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    }
    $opts = "-p $Port -h 127.0.0.1 -c listen_addresses=127.0.0.1"
    & (Join-Path $PgDir "bin\pg_ctl.exe") -D $DataDir -l $pgLog -o $opts -w start 2>&1 | Out-Null
    if (-not (Test-PgUp)) {
        if (Test-Path $pgLog) { Get-Content $pgLog -Tail 15 | ForEach-Object { Say "      $_" } }
        Die "PG 启动失败（日志: $pgLog）"
    }
    Ok "已启动，监听 127.0.0.1:$Port"
}

# ---------------------------------------------------------------- [4] 建库
Step 4 "治理库 agent_gov"
$psql = Join-Path $PgDir "bin\psql.exe"
$exists = & $psql -h 127.0.0.1 -p $Port -U postgres -d postgres -tAc `
    "SELECT 1 FROM pg_database WHERE datname='agent_gov'" 2>$null
if ($exists -eq "1") {
    Ok "已存在"
} else {
    & $psql -h 127.0.0.1 -p $Port -U postgres -d postgres -c "CREATE DATABASE agent_gov" 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { Die "建库失败" }
    Ok "已创建"
}

# ---------------------------------------------------------------- [5] schema
Step 5 "应用 schema（表 + 不可变触发器）"
if (-not (Test-Path $Py)) { Die "找不到虚拟环境: $Py" }
Push-Location (Join-Path $Root "code\agent")
try {
    $out = & $Py -c "import sys; sys.path.insert(0,'..'); from governance import store; ok,err=store.init_schema(); print(('OK' if ok else 'ERR')+' '+err[:200])" 2>&1
} finally { Pop-Location }
if ($out -match "^OK") {
    Ok "schema 就绪"
} else {
    Die "schema 应用失败: $out"
}

Say ""
Say "=============================================================="
Say " 准备完成。下一步:  .\start.ps1"
Say "=============================================================="
Say " 二进制 : $PgDir"
Say " 数据   : $DataDir"
Say " 日志   : $LogDir"
