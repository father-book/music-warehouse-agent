@echo off
REM ============================================================
REM  AC-20 one-click deploy - start everything
REM
REM  Starts PostgreSQL (third_party\pgdata) + Redis + Web, then runs:
REM    - deploy_smoke.py  : dependencies really reachable?
REM    - smoke.py         : business logic still correct?
REM  Exit code 0 only if deploy_smoke passes.
REM
REM  Usage:  start.cmd
REM          start.cmd -Port 8001 -NoBusiness
REM ============================================================
setlocal
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %*
set RC=%ERRORLEVEL%
if %RC% NEQ 0 (
  echo.
  echo [!] not ready - see FAIL lines above. Logs: third_party\logs
)
pause
exit /b %RC%
