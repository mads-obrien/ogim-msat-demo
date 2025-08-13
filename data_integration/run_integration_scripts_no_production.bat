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

rem Use the following line for NO production files
set PY_FILES=("_compressor_stations.py" "_equipment_components.py" "_fields_and_basins.py" "_flaring_detections.py" "_gathering_processing_facilities.py" "_injection_disposal_facilities.py" "_lng_facilities.py" "_offshore_platforms.py" "_petroleum_terminals.py" "_pipelines.py" "_refineries.py" "_stations_other.py" "_tank_batteries.py" "_wells.py" "canada_alberta.py" "canada_alberta_wells.py" "canada_british_columbia.py" "canada_manitoba.py" "canada_other_provinces.py" "canada_saskatchewan.py" "mexico.py" "usa_texas_wells.py" "usa_wells.py")

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
