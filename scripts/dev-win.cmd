@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0dev-win.ps1" %*
exit /b %errorlevel%
