#!/bin/bash
# Airflow服务修复脚本
# 用于解决scheduler不健康和CSRF令牌问题
# 增强版: 增加了健康检查和自诊断功能

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
  echo -e "${YELLOW}[INFO] $1${NC}"
}

log_success() {
  echo -e "${GREEN}[SUCCESS] $1${NC}"
}

log_error() {
  echo -e "${RED}[ERROR] $1${NC}"
}

# 健康检查函数
check_health() {
  log_info "检查Airflow健康状态..."
  HEALTH_CHECK=$(curl -s -u admin:admin http://localhost:8080/api/v1/health)
  
  echo "$HEALTH_CHECK"
  
  if echo "$HEALTH_CHECK" | grep -q '"status": "unhealthy"'; then
    log_error "检测到不健康状态"
    return 1
  else
    log_success "所有服务健康状态正常"
    return 0
  fi
}

# 检查进程函数
check_processes() {
  log_info "检查Airflow进程..."
  local process_errors=0
  
  # 检查scheduler进程
  if ! docker exec -it dispatcher-airflow-1 pgrep -f "airflow scheduler" > /dev/null; then
    log_error "Scheduler进程未运行"
    process_errors=$((process_errors+1))
  else
    log_success "Scheduler进程正在运行"
  fi
  
  # 检查webserver进程
  if ! docker exec -it dispatcher-airflow-1 pgrep -f "airflow webserver" > /dev/null; then
    log_error "Webserver进程未运行"
    process_errors=$((process_errors+1))
  else
    log_success "Webserver进程正在运行"
  fi
  
  # 检查triggerer进程
  if ! docker exec -it dispatcher-airflow-1 pgrep -f "airflow triggerer" > /dev/null; then
    log_error "Triggerer进程未运行"
    process_errors=$((process_errors+1))
  else
    log_success "Triggerer进程正在运行"
  fi
  
  if [ $process_errors -eq 0 ]; then
    log_success "所有关键进程正在运行"
    return 0
  else
    log_error "发现 $process_errors 个进程问题"
    return 1
  fi
}

# 诊断函数
diagnose() {
  log_info "开始进行Airflow服务诊断..."
  
  # 检查容器状态
  log_info "检查容器状态..."
  docker-compose ps
  
  # 检查健康状态
  check_health
  
  # 检查进程
  check_processes
  
  # 检查日志
  log_info "检查最近的日志..."
  docker-compose logs --tail=20 airflow
  
  log_info "诊断完成"
}

# 如果使用--check参数运行，则只执行健康检查
if [ "$1" = "--check" ]; then
  log_info "===== 开始Airflow健康检查 ====="
  
  # 检查容器是否运行
  if ! docker ps | grep -q dispatcher-airflow-1; then
    log_error "Airflow容器未运行，请先启动容器"
    exit 1
  fi
  
  # 执行健康检查
  check_health
  check_processes
  
  log_info "===== Airflow健康检查完成 ====="
  exit 0
fi

# 如果使用--diagnose参数运行，则执行诊断
if [ "$1" = "--diagnose" ]; then
  log_info "===== 开始Airflow服务诊断 ====="
  diagnose
  log_info "===== Airflow服务诊断完成 ====="
  exit 0
fi

# 正常修复流程
echo "===== 开始修复Airflow服务 ====="

# 检查当前容器状态
log_info "正在检查容器状态..."
docker-compose ps

# 检查健康状态
log_info "检查Airflow健康状态..."
curl -s -u admin:admin http://localhost:8080/api/v1/health

# 重置并同步权限
log_info "重置权限..."
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow db reset -y' || log_error "跳过数据库重置"
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow db init'
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow db upgrade'
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow users create --username admin --firstname Admin --lastname User --email admin@example.com --role Admin --password admin' || log_error "用户已存在"
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow sync-perm'

# 重启相关服务
log_info "重启调度器..."
docker exec -it dispatcher-airflow-1 pkill -f "airflow scheduler" || log_error "没有发现调度器进程"
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow scheduler -D'

# 重启triggerer
log_info "重启triggerer..."
docker exec -it dispatcher-airflow-1 pkill -f "airflow triggerer" || log_error "没有发现triggerer进程"
sleep 2
docker exec -it dispatcher-airflow-1 su airflow -c 'airflow triggerer -D'

# 确认修复完成
log_info "修复完成，请等待30秒后再次检查健康状态"
sleep 30

# 再次检查健康状态和进程
check_health
check_processes

log_success "===== Airflow服务修复完成 ====="
log_info "如果问题仍然存在，请考虑使用rebuild.sh脚本进行完整重建"
