@echo off
setlocal

REM Start collector in separate window
start "collector" cmd /k "cd /d %~dp0.. && python collector\agent.py"

REM Start scheduler in separate window; it invokes reasoner only when needed
start "reasoner-scheduler" cmd /k "cd /d %~dp0.. && python scripts\run_reasoner_if_needed.py"

echo Collector and reasoner scheduler started.
endlocal
