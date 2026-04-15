@echo off

REM Script to push code to GitHub repository

echo ==========================================
echo  GitHub Push Script
echo ==========================================
echo.

REM Check if Git is installed
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Git is not installed or not in PATH
    pause
    exit /b 1
)
echo [OK] Git is installed

REM Show current git status
echo.
echo [INFO] Current Git status:
git status --short

REM Check for uncommitted changes
git diff --quiet
if %errorlevel% equ 0 (
    echo [INFO] No uncommitted changes
) else (
    echo [INFO] Found uncommitted changes, preparing to commit...
    
    REM Add all changes
    echo.
    echo [EXEC] git add .
    git add .
    
    REM Commit changes
    echo [EXEC] git commit -m "Update from Trae IDE"
    git commit -m "Update from Trae IDE"
    
    if %errorlevel% neq 0 (
        echo [ERROR] Commit failed
        pause
        exit /b 1
    )
    echo [OK] Commit successful
)

REM GitHub repository URL
set github_repo=https://github.com/Fool-MrRice/redis-rust.git

echo.
echo [INFO] Pushing to GitHub repository:
echo        %github_repo%
echo.

REM Check if remote exists
git remote get-url github >nul 2>&1
if %errorlevel% neq 0 (
    echo [EXEC] Adding GitHub remote...
    git remote add github %github_repo%
    echo [OK] Remote added
) else (
    echo [OK] GitHub remote already exists
)

REM Execute push command
echo [EXEC] git push github master
git push github master

REM Check if push was successful
if %errorlevel% equ 0 (
    echo.
    echo ==========================================
    echo  [SUCCESS] Push successful!
    echo ==========================================
) else (
    echo.
    echo ==========================================
    echo  [FAILED] Push failed
    echo ==========================================
    echo Please check:
    echo   1. Network connection
    echo   2. GitHub repository exists
    echo   3. Push permissions
    echo   4. GitHub Token configuration
    echo ==========================================
    pause
    exit /b 1
)

echo.
pause
