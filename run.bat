@echo off
chcp 65001 > nul
echo ========================================
echo Starting Dashboard
echo Time: %date% %time%
echo ========================================

REM Set working directory
cd /d "%~dp0"

REM Set Python environment variable
set PYTHONIOENCODING=utf-8

REM Activate conda environment
echo Activating conda environment ccxt_dashboard...

REM Try to find conda installation
where conda >nul 2>nul
if %ERRORLEVEL% equ 0 (
    REM Conda is in PATH – use conda activate (requires conda init)
    call conda activate ccxt_dashboard
) else (
    REM Search common installation paths
    if exist "%USERPROFILE%\miniconda3\Scripts\activate.bat" (
        call "%USERPROFILE%\miniconda3\Scripts\activate.bat" ccxt_dashboard
    ) else if exist "%USERPROFILE%\Anaconda3\Scripts\activate.bat" (
        call "%USERPROFILE%\Anaconda3\Scripts\activate.bat" ccxt_dashboard
    ) else if exist "C:\ProgramData\miniconda3\Scripts\activate.bat" (
        call "C:\ProgramData\miniconda3\Scripts\activate.bat" ccxt_dashboard
    ) else if exist "C:\ProgramData\Anaconda3\Scripts\activate.bat" (
        call "C:\ProgramData\Anaconda3\Scripts\activate.bat" ccxt_dashboard
    ) else (
        echo ERROR: Conda installation not found.
        pause
        exit /b 1
    )
)

if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to activate Conda environment
    pause
    exit /b 1
)

REM Check if run.py exists
if not exist "run.py" (
    echo ERROR: File run.py not found in current directory
    echo Current directory: %cd%
    pause
    exit /b 1
)

echo Starting server...
echo ========================================

REM Run with UTF-8 mode and redirect output to log
python -X utf8 run.py >> run.log 2>&1