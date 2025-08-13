@echo off

rem Check if PYTHONPATH is provided as a parameter by the user
if "%1"=="" (
    echo Error: PYTHONPATH is not provided.
    echo Usage: %0 PYTHONPATH
    exit /b 1
)

rem Set the provided PYTHONPATH variable
set PYTHONPATH=%1

rem Define the log file
set LOG_FILE=output.log

rem Loop through the Python scripts in the current directory
for %%f in (*.py) do (

rem Optional line for only running a subset of files based on filename
rem for /f "tokens=*" %%f in ('dir /b /a-d *.py ^| findstr /v "well"') do (

    rem Get the current system time and date
    setlocal enabledelayedexpansion
    set CURRENT_TIME=!TIME:~0,8!
    set CURRENT_DATE=!DATE!
    
    echo [!CURRENT_DATE! !CURRENT_TIME!] Executing %%f
    echo [!CURRENT_DATE! !CURRENT_TIME!] Output for %%f >> %LOG_FILE%
    python "%%f" >> %LOG_FILE% 2>&1
    if errorlevel 1 (
        echo [!CURRENT_DATE! !CURRENT_TIME!] Error occurred while executing %%f
    )
    endlocal
)

echo %date% %time% All Python scripts executed
