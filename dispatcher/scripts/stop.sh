#!/bin/bash

# 数据开发工具项目停止脚本

echo "🛑 停止数据开发工具项目..."

# 停止所有服务
docker-compose down

echo "✅ 服务已停止" 