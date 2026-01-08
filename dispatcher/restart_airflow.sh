#!/bin/bash
# 简化版服务重启脚本
# 仅用于紧急情况下的服务重启

echo "===== 开始重启Airflow服务 ====="
echo "注意: 该脚本仅用于紧急情况，正常情况下服务应当自动恢复"

# 重启服务
echo "正在重启服务..."
docker compose restart airflow || docker compose up -d airflow

echo "等待服务启动 (30秒)..."
sleep 30

# 检查服务状态
echo "检查服务状态..."
docker compose ps

echo "检查健康状态..."
curl -s -u admin:admin http://localhost:8080/api/v1/health || true

echo "===== Airflow服务重启完成 ====="
echo "如果问题仍然存在，请使用 'docker compose down && docker compose up -d' 完全重建服务"
