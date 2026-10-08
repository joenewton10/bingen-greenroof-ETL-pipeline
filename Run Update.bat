@echo off
REM Rebuilds the dataset from whatever CSV files are currently in data\raw\...
REM Safe to run any time: it builds into a staging file and verifies it before
REM touching the live database, so a failed run never breaks the dashboard.

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo Setup has not been run yet. Double-click setup.bat first.
    pause
    exit /b 1
)

".venv\Scripts\python.exe" scripts\run_update.py

echo.
pause
