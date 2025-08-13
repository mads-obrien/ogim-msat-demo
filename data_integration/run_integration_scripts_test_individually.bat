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

rem List of specific .py files to execute. EDIT THIS LINE AS NEEDED.
rem set PY_FILES=("_flaring_detections.py" "mexico.py" "usa_texas_wells.py" "canada_other_provinces.py" "canada_manitoba.py" "canada_alberta.py" "canada_alberta_wells.py" "canada_saskatchewan.py" "canada_british_columbia.py" "usa_wells.py")

rem List the files you want to run, in the order to run them.
rem Enclose file names in quotation marks, and separate each file name with a space.
set PY_FILES=("_wells.py" )

rem Loop through the specified Python scripts
for %%f in %PY_FILES% do (

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

echo %date% %time% All specified Python scripts executed.
