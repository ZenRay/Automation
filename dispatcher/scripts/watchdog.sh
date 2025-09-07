#!/bin/bash
# 自动恢复脚本 - 监控Airflow服务并在进程不存在时自动重启

# 日志目录
LOG_DIR="/opt/airflow/logs"
mkdir -p "$LOG_DIR"

# 日志文件
SCHEDULER_LOG="$LOG_DIR/scheduler_watchdog.log"
TRIGGERER_LOG="$LOG_DIR/triggerer_watchdog.log"
WEBSERVER_LOG="$LOG_DIR/webserver_watchdog.log"
MAIN_LOG="$LOG_DIR/watchdog.log"

# 日志函数
log_message() {
  local message="$(date '+%Y-%m-%d %H:%M:%S') - $1"
  echo "$message" >> "$MAIN_LOG"
  echo "$message"
}

# 初始化日志
log_message "===== Airflow服务监控脚本启动 ====="
log_message "监控间隔: 30秒"

# 监控循环
while true; do
  # 检查scheduler进程
  if ! pgrep -f "airflow scheduler" > /dev/null; then
    log_message "检测到scheduler进程不存在，正在重启..."
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 重启scheduler进程" >> "$SCHEDULER_LOG"
    su airflow -c "airflow scheduler -D"
    log_message "scheduler进程已重启"
  else
    log_message "scheduler进程正在运行"
  fi
  
  # 检查triggerer进程
  if ! pgrep -f "airflow triggerer" > /dev/null; then
    log_message "检测到triggerer进程不存在，正在重启..."
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 重启triggerer进程" >> "$TRIGGERER_LOG"
    su airflow -c "airflow triggerer -D"
    log_message "triggerer进程已重启"
  else
    log_message "triggerer进程正在运行"
  fi
  
  # 检查webserver进程
  if ! pgrep -f "airflow webserver" > /dev/null; then
    log_message "检测到webserver进程不存在"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - webserver进程不存在" >> "$WEBSERVER_LOG"
    # 不直接启动webserver，因为它应该作为主进程运行
    # 如果webserver停止，容器应该由Docker自动重启
    log_message "webserver应该是主进程，不自动重启，依赖容器重启策略"
  else
    log_message "webserver进程正在运行"
  fi
  
  # 检查服务健康状态
  if curl -s -u admin:admin http://localhost:8080/api/v1/health > /dev/null 2>&1; then
    log_message "Airflow API健康检查通过"
  else
    log_message "警告: Airflow API健康检查失败"
  fi
  
  # 等待30秒
  log_message "监控周期完成，等待30秒..."
  sleep 30
done
