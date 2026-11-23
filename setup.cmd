@echo off
REM ============================================================
REM  AC-20 one-click deploy - step 1 (prepare)
REM
REM  Why a .cmd wrapper: Windows defaults to ExecutionPolicy=Restricted,
REM  so double-clicking a .ps1 just opens Notepad or errors out. Changing
REM  the machine-wide policy needs admin rights. -ExecutionPolicy Bypass
REM  applies to this single invocation only - nothing on the system changes.
REM
REM  Usage:  setup.cmd            (adopt existing data)
REM          setup.cmd -Fresh     (empty database)
REM ============================================================
setlocal
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup.ps1" %*
set RC=%ERRORLEVEL%
if %RC% NEQ 0 (
  echo.
  echo [!] setup failed with exit code %RC%
)
pause
exit /b %RC%
