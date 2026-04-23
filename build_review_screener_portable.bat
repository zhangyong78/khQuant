@echo off
setlocal
cd /d "%~dp0"

set "PYTHON_EXE=%~dp0.venv311\Scripts\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"

if not exist "%PYTHON_EXE%" (
    echo Python environment not found.
    echo Expected .venv311\Scripts\python.exe or .venv\Scripts\python.exe
    pause
    exit /b 1
)

"%PYTHON_EXE%" tools\build_review_screener_portable.py
if errorlevel 1 (
    echo.
    echo Build failed.
    pause
)

endlocal
