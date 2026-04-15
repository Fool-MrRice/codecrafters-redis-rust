#!/bin/bash

# Script to push code to GitHub repository

echo "=========================================="
echo "  GitHub Push Script"
echo "=========================================="
echo ""

# Check if Git is installed
if ! command -v git &> /dev/null; then
    echo "[ERROR] Git is not installed or not in PATH"
    read -p "Press Enter to continue..."
    exit 1
fi
echo "[OK] Git is installed: $(git --version)"

# Show current git status
echo ""
echo "[INFO] Current Git status:"
git status --short

# Check for staged changes
if git diff --cached --quiet; then
    # No staged changes, check for unstaged changes
    if git diff --quiet; then
        echo "[INFO] No uncommitted changes"
    else
        echo "[INFO] Found uncommitted changes, preparing to commit..."
        
        # Add all changes
        echo ""
        echo "[EXEC] git add ."
        git add .
        
        # Commit changes
        echo "[EXEC] git commit -m \"Update from Trae IDE\""
        if ! git commit -m "Update from Trae IDE"; then
            echo "[ERROR] Commit failed"
            read -p "Press Enter to continue..."
            exit 1
        fi
        echo "[OK] Commit successful"
    fi
else
    echo "[INFO] Found staged changes, committing..."
    
    # Commit changes
    echo "[EXEC] git commit -m \"Update from Trae IDE\""
    if ! git commit -m "Update from Trae IDE"; then
        echo "[ERROR] Commit failed"
        read -p "Press Enter to continue..."
        exit 1
    fi
    echo "[OK] Commit successful"
fi

# GitHub repository URL
github_repo="https://github.com/Fool-MrRice/redis-rust.git"

echo ""
echo "[INFO] Pushing to GitHub repository:"
echo "       $github_repo"
echo ""

# Check if remote exists
if ! git remote get-url github &> /dev/null; then
    echo "[EXEC] Adding GitHub remote..."
    git remote add github "$github_repo"
    echo "[OK] Remote added"
else
    echo "[OK] GitHub remote already exists"
fi

# Execute push command
echo "[EXEC] git push github master"
if git push github master; then
    echo ""
    echo "=========================================="
    echo "  [SUCCESS] Push successful!"
    echo "=========================================="
else
    echo ""
    echo "=========================================="
    echo "  [FAILED] Push failed"
    echo "=========================================="
    echo "Please check:"
    echo "  1. Network connection"
    echo "  2. GitHub repository exists"
    echo "  3. Push permissions"
    echo "  4. GitHub Token configuration"
    echo "=========================================="
    read -p "Press Enter to continue..."
    exit 1
fi

echo ""
read -p "Press Enter to continue..."
