#!/bin/bash

# 数据开发工具项目启动脚本
# 用于启动 Airflow 调度系统

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# 检查函数
check_command() {
    if ! command -v $1 &> /dev/null; then
        log_error "$1 未安装"
        return 1
    fi
    log_success "$1 已安装"
    return 0
}

check_file() {
    if [ ! -f "$1" ]; then
        log_error "$1 文件不存在"
        return 1
    fi
    log_success "$1 文件存在"
    return 0
}

check_directory() {
    if [ ! -d "$1" ]; then
        log_warning "$1 目录不存在，正在创建..."
        mkdir -p "$1"
    fi
    log_success "$1 目录就绪"
}

# 本地 Airflow 启动函数
start_local_airflow() {
    log_info "启动本地 Python Airflow 环境..."
    
    # 检查 Python 环境
    if ! command -v python &> /dev/null; then
        log_error "Python 未安装，请先安装 Python"
        exit 1
    fi
    
    # 检查 pip
    if ! command -v pip &> /dev/null; then
        log_error "pip 未安装，请先安装 pip"
        exit 1
    fi
    
    # 检查是否在 conda 环境中
    if [ -n "$CONDA_DEFAULT_ENV" ]; then
        log_info "检测到 conda 环境: $CONDA_DEFAULT_ENV"
        log_success "使用当前 conda 环境"
    else
        # 创建虚拟环境（如果不是在 conda 环境中）
        if [ ! -d "airflow_env" ]; then
            log_info "创建 Python 虚拟环境..."
            python -m venv airflow_env
        fi
        
        # 激活虚拟环境
        log_info "激活虚拟环境..."
        source airflow_env/bin/activate
    fi
    
    # 安装 Airflow
    log_info "安装 Airflow..."
    pip install apache-airflow[celery] pyodps
    
    # 设置 Airflow 环境变量
    export AIRFLOW_HOME=$(pwd)/airflow_home
    export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
    export AIRFLOW__CORE__PLUGINS_FOLDER=$(pwd)/plugins
    export AIRFLOW__LOGGING__BASE_LOG_FOLDER=$(pwd)/logs
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///$(pwd)/airflow_home/airflow.db
    export AIRFLOW__CORE__EXECUTOR=LocalExecutor
    export AIRFLOW__CORE__LOAD_EXAMPLES=false
    export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
    
    # 创建 Airflow 目录
    mkdir -p $AIRFLOW_HOME
    
    # 初始化数据库
    log_info "初始化 Airflow 数据库..."
    airflow db init
    
    # 创建用户
    log_info "创建 Airflow 用户..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
    
    # 启动 Airflow
    log_info "启动 Airflow Web 服务器..."
    airflow webserver --port 8080 --daemon
    
    log_info "启动 Airflow 调度器..."
    airflow scheduler --daemon
    
    # 等待服务启动
    log_info "等待服务启动..."
    sleep 10
    
    # 检查服务状态
    if curl -f http://localhost:8080/health > /dev/null 2>&1; then
        log_success "本地 Airflow 启动成功！"
        echo ""
        echo "🎉 本地 Airflow 启动成功！"
        echo "=================================="
        echo "🌐 Web UI: http://localhost:8080"
        echo "👤 用户名: admin"
        echo "🔑 密码: admin"
        echo "📁 数据目录: $AIRFLOW_HOME"
        echo ""
        echo "📝 常用命令："
        echo "  停止服务: pkill -f airflow"
        echo "  查看日志: tail -f $AIRFLOW_HOME/logs/scheduler/latest/*.log"
        echo "  查看状态: ps aux | grep airflow"
        echo ""
    else
        log_error "本地 Airflow 启动失败"
        exit 1
    fi
}

# 主函数
main() {
    echo "🚀 启动数据开发工具项目..."
    echo "=================================="
    
    # 1. 环境检查
    log_info "检查运行环境..."
    
    # 检查 Docker
    if ! check_command "docker"; then
        log_error "请先安装 Docker"
        exit 1
    fi
    
    # 检查 Docker Compose
    if ! check_command "docker-compose"; then
        log_error "请先安装 Docker Compose"
        exit 1
    fi
    
    # 检查 Docker 服务状态
    if ! docker info > /dev/null 2>&1; then
        log_error "Docker 服务未启动，请先启动 Docker"
        exit 1
    fi
    log_success "Docker 服务正常运行"
    
    # 2. 配置文件检查
    log_info "检查配置文件..."
    
    # 检查 docker-compose.yml
    if ! check_file "docker-compose.yml"; then
        log_error "请在项目根目录运行此脚本"
        exit 1
    fi
    
    # 检查 Dockerfile
    check_file "Dockerfile"
    
    # 检查 requirements.txt
    check_file "requirements.txt"
    
    # 3. 目录结构检查
    log_info "检查目录结构..."
    
    check_directory "dags"
    check_directory "logs"
    check_directory "plugins"
    
    # 4. 环境变量设置
    log_info "设置环境变量..."
    
    # 检查并创建 .env 文件
    if [ ! -f ".env" ]; then
        log_warning ".env 文件不存在，正在创建..."
        cat > .env << EOF
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=
MYSQL_ROOT_PASSWORD=airflow
MYSQL_DATABASE=airflow
MYSQL_USER=airflow
MYSQL_PASSWORD=airflow
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
EOF
        log_success ".env 文件已创建"
    else
        log_success ".env 文件已存在"
    fi
    
    # 设置 Airflow UID
    export AIRFLOW_UID=${AIRFLOW_UID:-50000}
    log_success "AIRFLOW_UID 设置为: $AIRFLOW_UID"
    
    # 5. 权限设置
    log_info "设置目录权限..."
    
    # 设置目录权限
    sudo chown -R $AIRFLOW_UID:0 logs dags plugins 2>/dev/null || {
        log_warning "无法设置目录权限，可能需要 sudo 权限"
    }
    
    # 6. 检查现有容器
    log_info "检查现有容器状态..."
    
    if docker-compose ps | grep -q "Up"; then
        log_warning "检测到正在运行的服务"
        read -p "是否要重启服务？(y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            log_info "停止现有服务..."
            docker-compose down
        else
            log_info "保持现有服务运行"
            docker-compose ps
            exit 0
        fi
    fi
    
    # 7. 构建镜像
    log_info "构建自定义 Airflow 镜像..."
    
    # 检查网络连接
    if ! curl -s --connect-timeout 10 https://registry-1.docker.io > /dev/null 2>&1; then
        log_warning "网络连接有问题，尝试使用本地 Python 环境启动..."
        start_local_airflow
        return 0
    fi
    
    # 构建镜像
    if ! docker-compose build; then
        log_error "镜像构建失败，尝试使用本地 Python 环境启动..."
        start_local_airflow
        return 0
    fi
    
    log_success "镜像构建完成"
    
    # 8. 初始化数据库（如果需要）
    log_info "检查数据库状态..."
    
    if ! docker-compose ps mysql | grep -q "Up"; then
        log_info "初始化数据库..."
        docker-compose up airflow-init -d
        sleep 10
        log_success "数据库初始化完成"
    else
        log_success "数据库已就绪"
    fi
    
    # 9. 启动服务
    log_info "启动 Airflow 服务..."
    
    # 启动所有服务
    docker-compose up -d
    
    # 10. 等待服务启动
    log_info "等待服务启动..."
    
    # 等待数据库就绪
    log_info "等待数据库就绪..."
    timeout=60
    while [ $timeout -gt 0 ]; do
        if docker-compose exec -T mysql mysqladmin ping -h localhost --silent; then
            log_success "数据库就绪"
            break
        fi
        sleep 2
        timeout=$((timeout - 2))
    done
    
    if [ $timeout -le 0 ]; then
        log_error "数据库启动超时"
        exit 1
    fi
    
    # 等待 Redis 就绪
    log_info "等待 Redis 就绪..."
    timeout=30
    while [ $timeout -gt 0 ]; do
        if docker-compose exec -T redis redis-cli ping > /dev/null 2>&1; then
            log_success "Redis 就绪"
            break
        fi
        sleep 2
        timeout=$((timeout - 2))
    done
    
    if [ $timeout -le 0 ]; then
        log_error "Redis 启动超时"
        exit 1
    fi
    
    # 等待 Airflow 服务就绪
    log_info "等待 Airflow 服务就绪..."
    timeout=120
    while [ $timeout -gt 0 ]; do
        if curl -f http://localhost:8080/health > /dev/null 2>&1; then
            log_success "Airflow Web 服务就绪"
            break
        fi
        sleep 5
        timeout=$((timeout - 5))
    done
    
    if [ $timeout -le 0 ]; then
        log_error "Airflow 服务启动超时"
        log_info "查看服务日志..."
        docker-compose logs --tail=50
        exit 1
    fi
    
    # 11. 验证服务状态
    log_info "验证服务状态..."
    
    # 检查所有服务状态
    docker-compose ps
    
    # 检查 Web 服务
    if curl -f http://localhost:8080/health > /dev/null 2>&1; then
        log_success "Airflow Web 服务正常"
    else
        log_error "Airflow Web 服务异常"
        exit 1
    fi
    
    # 检查 Scheduler 服务
    if docker-compose exec airflow-scheduler airflow jobs check --job-type SchedulerJob --hostname "$(hostname)" > /dev/null 2>&1; then
        log_success "Airflow Scheduler 服务正常"
    else
        log_warning "Airflow Scheduler 服务可能还在启动中"
    fi
    
    # 检查 Worker 服务
    if docker-compose ps airflow-worker | grep -q "Up"; then
        log_success "Airflow Worker 服务正常"
    else
        log_warning "Airflow Worker 服务可能还在启动中"
    fi
    
    # 12. 显示访问信息
    echo ""
    echo "🎉 Airflow 启动成功！"
    echo "=================================="
    echo "🌐 Web UI: http://localhost:8080"
    echo "👤 用户名: admin"
    echo "🔑 密码: admin"
    echo "🌸 Flower 监控: http://localhost:5555"
    echo "📊 健康检查: http://localhost:8080/health"
    echo ""
    echo "📝 常用命令："
    echo "  查看日志: docker-compose logs -f [service_name]"
    echo "  停止服务: ./scripts/stop.sh"
    echo "  重启服务: docker-compose restart [service_name]"
    echo "  查看状态: docker-compose ps"
    echo "  重新构建: docker-compose build --no-cache"
    echo ""
    
    # 13. 可选：打开浏览器
    read -p "是否要打开浏览器访问 Airflow UI？(y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if command -v xdg-open &> /dev/null; then
            xdg-open http://localhost:8080
        elif command -v open &> /dev/null; then
            open http://localhost:8080
        else
            log_warning "无法自动打开浏览器，请手动访问 http://localhost:8080"
        fi
    fi
}

# 错误处理
trap 'log_error "启动过程中发生错误，请检查日志"; exit 1' ERR

# 运行主函数
main "$@" 