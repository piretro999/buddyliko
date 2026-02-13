@echo off
setlocal enabledelayedexpansion

echo ================================
echo DataMapper Pro - Launcher v2.0
echo ================================
echo.

REM Parse command line arguments
set AUTH_MODE=disabled
set DB_MODE=sqlite
set BACKEND_PORT=8080
set FRONTEND_PORT=8000

:parse_args
if "%~1"=="" goto end_parse
if /i "%~1"=="--auth" (
    set AUTH_MODE=enabled
    shift
    goto parse_args
)
if /i "%~1"=="--no-auth" (
    set AUTH_MODE=disabled
    shift
    goto parse_args
)
if /i "%~1"=="--db" (
    set DB_MODE=%~2
    shift
    shift
    goto parse_args
)
if /i "%~1"=="--port" (
    set BACKEND_PORT=%~2
    shift
    shift
    goto parse_args
)
if /i "%~1"=="--help" goto show_help
if /i "%~1"=="-h" goto show_help
shift
goto parse_args
:end_parse

REM Display configuration
echo Configuration:
echo   Authentication: %AUTH_MODE%
echo   Database:       %DB_MODE%
echo   Backend Port:   %BACKEND_PORT%
echo   Frontend Port:  %FRONTEND_PORT%
echo.

REM Check dependencies
echo Checking dependencies...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python not found!
    echo Please install Python 3.8+ from python.org
    pause
    exit /b 1
)

REM Create data directory
if not exist "data" mkdir data
if not exist "logs" mkdir logs

REM Set environment variable for auth
if "%AUTH_MODE%"=="enabled" (
    set AUTH_ENABLED=true
) else (
    set AUTH_ENABLED=false
)

echo.
echo Starting Backend...
start "DataMapper Backend" cmd /k "cd backend && set AUTH_ENABLED=%AUTH_ENABLED% && set DB_MODE=%DB_MODE% && python -m uvicorn api:app --host 127.0.0.1 --port %BACKEND_PORT% --reload"

REM Wait for backend
timeout /t 3 /nobreak > nul

echo Starting Frontend...
start "DataMapper Frontend" cmd /k "python -m http.server %FRONTEND_PORT% --directory frontend"

REM Wait for frontend
timeout /t 2 /nobreak > nul

echo.
echo ================================
echo System Running!
echo ================================
echo Backend:  http://localhost:%BACKEND_PORT%
echo Frontend: http://localhost:%FRONTEND_PORT%
echo API Docs: http://localhost:%BACKEND_PORT%/docs
echo.

if "%AUTH_MODE%"=="enabled" (
    echo 🔐 Authentication: ENABLED
    echo    Register at: http://localhost:%FRONTEND_PORT%/#/register
    echo    Login at:    http://localhost:%FRONTEND_PORT%/#/login
) else (
    echo 🔓 Authentication: DISABLED
    echo    All features accessible without login
)

echo.
echo Press Ctrl+C in each window to stop
echo.

REM Open browser
start http://localhost:%FRONTEND_PORT%

goto :eof

:show_help
echo.
echo DataMapper Pro - Launcher
echo.
echo Usage: run.bat [OPTIONS]
echo.
echo Options:
echo   --auth          Enable authentication (requires login)
echo   --no-auth       Disable authentication (default)
echo   --db MODE       Database: sqlite (default), tinydb, postgresql, memory
echo   --port PORT     Backend port (default: 8080)
echo   --help, -h      Show this help
echo.
echo Examples:
echo   run.bat                                 Start with defaults (no auth, SQLite)
echo   run.bat --auth                          Enable authentication
echo   run.bat --auth --db postgresql          Auth + PostgreSQL database
echo   run.bat --db tinydb                     Use TinyDB (JSON file)
echo   run.bat --port 9000                     Custom backend port
echo   run.bat --auth --db sqlite --port 9000  All options combined
echo.
echo Quick Start Scripts:
echo   run_with_auth.bat        Start with authentication enabled
echo   run_no_auth.bat          Start without authentication
echo.
exit /b 0
