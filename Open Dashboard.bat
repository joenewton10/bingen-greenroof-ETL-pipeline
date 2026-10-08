@echo off
REM Opens the dashboard in your web browser. Leave this window open while you
REM use the dashboard — closing it stops the dashboard.

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo Setup has not been run yet. Double-click setup.bat first.
    pause
    exit /b 1
)

".venv\Scripts\python.exe" -m streamlit run dashboard\app.py

pause
