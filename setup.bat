@echo off
REM One-time setup: creates a Python virtual environment and installs everything
REM the pipeline and dashboard need. Safe to run again later (e.g. after moving
REM this folder to a new PC) — it will just reuse/update the existing venv.

cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
    echo Python was not found on this PC. Install Python 3.11+ from https://python.org
    echo ^(tick "Add python.exe to PATH" during install^), then run this file again.
    pause
    exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
    echo Creating virtual environment...
    python -m venv .venv
)

echo Installing/updating dependencies...
".venv\Scripts\python.exe" -m pip install --upgrade pip
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
    echo.
    echo Setup FAILED: dependency installation ran into an error ^(see above^).
    echo The pipeline and dashboard will not work until this is fixed.
    pause
    exit /b 1
)

echo.
echo Setup complete. You can now use "Run Update.bat" and "Open Dashboard.bat".
pause
