#!/bin/bash
# Airflow服务修复脚本
# 用于解决scheduler不健康和CSRF令牌问题

echo "===== 开始修复Airflow服务 ====="

# 检查当前容器状态
echo "正在检查容器状态..."
docker-compose ps

# 检查健康状态
echo "检查Airflow健康状态..."
curl -s -u admin:admin http://localhost:8080/api/v1/health

# 重置并同步权限
echo "重置权限..."
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow db reset -y' || echo "跳过数据库重置"
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow db init'
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow db upgrade'
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow users create --username admin --firstname Admin --lastname User --email admin@example.com --role Admin --password admin' || echo "用户已存在"
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow sync-perm'

# 重启相关服务
echo "重启调度器..."
docker exec -it dispatcher-airflow-1 pkill -f "airflow scheduler" || echo "没有发现调度器进程"
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow scheduler -D'

# 重启triggerer
echo "重启triggerer..."
docker exec -it dispatcher-airflow-1 pkill -f "airflow triggerer" || echo "没有发现triggerer进程"
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow triggerer -D'

# 确认修复完成
echo "修复完成，请等待30秒后再次检查健康状态"
sleep 30
curl -s -u admin:admin http://localhost:8080/api/v1/health

echo "===== Airflow服务修复完成 ====="
echo "如果问题仍然存在，请考虑使用rebuild.sh脚本进行完整重建"
