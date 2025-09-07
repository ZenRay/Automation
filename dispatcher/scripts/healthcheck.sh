#!/bin/bash
# 增强版健康检查脚本
# 用于Docker健康检查

# 日志文件
LOG_FILE="/opt/airflow/logs/healthcheck.log"
mkdir -p "$(dirname "$LOG_FILE")"

# 记录健康检查开始
echo "$(date '+%Y-%m-%d %H:%M:%S') - 健康检查开始" >> "$LOG_FILE"

# 检查API健康状态
api_status=$(curl -s -u admin:admin http://localhost:8080/api/v1/health)

# 检查关键进程
scheduler_process=$(pgrep -f "airflow scheduler")
webserver_process=$(pgrep -f "airflow webserver")
triggerer_process=$(pgrep -f "airflow triggerer")

# 输出状态信息到日志
echo "API状态: $api_status" >> "$LOG_FILE"
echo "Scheduler进程: $scheduler_process" >> "$LOG_FILE"
echo "Webserver进程: $webserver_process" >> "$LOG_FILE"
echo "Triggerer进程: $triggerer_process" >> "$LOG_FILE"

# 检查API是否正常
if ! curl -sf http://localhost:8080/api/v1/health >/dev/null; then
  echo "API检查失败" >> "$LOG_FILE"
  exit 1
fi

# 检查必要进程是否运行
if [ -z "$scheduler_process" ]; then
  echo "Scheduler进程不存在" >> "$LOG_FILE"
  exit 1
fi

if [ -z "$webserver_process" ]; then
  echo "Webserver进程不存在" >> "$LOG_FILE"
  exit 1
fi

if [ -z "$triggerer_process" ]; then
  echo "Triggerer进程不存在" >> "$LOG_FILE"
  exit 1
fi

# 如果所有检查都通过
echo "健康检查通过" >> "$LOG_FILE"
exit 0
