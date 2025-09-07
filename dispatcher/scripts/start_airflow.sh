#!/bin/bash
# 容器启动脚本
# 用于初始化环境和启动Airflow服务

# 日志目录设置
echo "===== 初始化日志目录 ====="
mkdir -p /opt/airflow/logs
chmod -R 777 /opt/airflow/logs
chown -R airflow:root /opt/airflow/logs

# 初始化Airflow环境
echo "===== 初始化Airflow环境 ====="
airflow db init
airflow db upgrade
(airflow users create --username admin --firstname Admin --lastname User --email admin@example.com --role Admin --password admin || echo '用户已存在')
airflow sync-perm

# 停止可能已存在的进程
echo "===== 清理可能存在的进程 ====="
pkill -f 'airflow scheduler' || echo '没有遗留的scheduler进程'
pkill -f 'airflow webserver' || echo '没有遗留的webserver进程'
pkill -f 'airflow triggerer' || echo '没有遗留的triggerer进程'

# 启动监控脚本
echo "===== 启动自动恢复监控 ====="
nohup /opt/airflow/scripts/watchdog.sh > /opt/airflow/logs/watchdog.log 2>&1 &

# 启动Airflow服务
echo "===== 启动Airflow服务 ====="
su airflow -c 'airflow scheduler -D'
su airflow -c 'airflow triggerer -D'

# 启动Webserver作为主进程
echo "===== 所有后台服务已启动，正在启动Webserver... ====="
su airflow -c 'airflow webserver'
