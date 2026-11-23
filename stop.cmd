@echo off
REM ============================================================
REM  AC-20 one-click deploy - stop everything
REM
REM  Stops only the instances this project started:
REM    Web   : by third_party\logs\webapp.pid
REM    Redis : third_party\redis-win64 (loopback only)
REM    PG    : third_party\pgdata
REM  External instances (e.g. C:\Users\Public\da_pg) are left alone.
REM
REM  Usage:  stop.cmd
REM          stop.cmd -KeepPg
REM ============================================================
setlocal
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop.ps1" %*
pause
exit /b 0
