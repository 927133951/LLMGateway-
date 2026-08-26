@echo off
setlocal EnableExtensions
title LLM Gateway - localhost:4567
cd /d "%~dp0"

rem ---------- 1. locate a usable node (system -> bundled -> auto-download) ----------
set "NODE_EXE="

where node >nul 2>nul
if not errorlevel 1 (
    node -e "process.exit(parseInt(process.versions.node)>=18?0:1)" >nul 2>nul
    if not errorlevel 1 set "NODE_EXE=node"
)

if not defined NODE_EXE if exist ".runtime\node\node.exe" set "NODE_EXE=.runtime\node\node.exe"

if defined NODE_EXE goto :have_node

echo.
echo  ============================================
echo   first run: Node.js not found
echo   downloading portable runtime (~30MB, one time only)
echo   no admin rights needed, please wait...
echo  ============================================
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup-node.ps1"
if exist ".runtime\node\node.exe" (
    set "NODE_EXE=.runtime\node\node.exe"
) else (
    echo.
    echo  [X] auto-setup failed. check your network, or install Node.js manually:
    echo      https://nodejs.org/  then re-run this file.
    echo.
    pause
    exit /b 1
)

:have_node
echo using node: %NODE_EXE%
echo gateway: http://localhost:4567   (console opens automatically)
echo.

rem ---------- 2. open console page after server boots ----------
start "" /min cmd /c "timeout /t 2 /nobreak >nul & start "" http://localhost:4567"

rem ---------- 3. run ----------
"%NODE_EXE%" server.js
echo.
echo gateway stopped.
pause
