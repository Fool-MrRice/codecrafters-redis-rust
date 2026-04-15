#!/bin/bash

# 脚本功能：将当前代码推送到GitHub仓库

# GitHub仓库地址
github_repo="https://github.com/Fool-MrRice/redis-rust.git"

echo "正在推送到GitHub仓库..."

# 执行推送命令
git push $github_repo master

# 检查推送是否成功
if [ $? -eq 0 ]; then
    echo "推送成功！"
else
    echo "推送失败，请检查网络连接或GitHub仓库配置。"
    exit 1
fi
