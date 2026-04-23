@echo off
setlocal
cd /d "%~dp0"

set "PYTHONW_EXE=%~dp0.venv311\Scripts\pythonw.exe"
if not exist "%PYTHONW_EXE%" set "PYTHONW_EXE=%~dp0.venv\Scripts\pythonw.exe"

if not exist "%PYTHONW_EXE%" (
    echo Python environment not found.
    echo Expected .venv311\Scripts\pythonw.exe or .venv\Scripts\pythonw.exe
    pause
    exit /b 1
)

start "" "%PYTHONW_EXE%" -m stock_screener

endlocal
