#!/bin/bash

# 数据开发工具项目部署脚本

set -e

echo "🚀 部署数据开发工具项目..."

# 检查环境
echo "🔍 检查部署环境..."

# 检查Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker未安装"
    exit 1
fi

# 检查Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose未安装"
    exit 1
fi

# 检查配置文件
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ docker-compose.yml文件不存在"
    exit 1
fi

# 构建自定义镜像
echo "🔨 构建自定义Airflow镜像..."
docker-compose build

# 检查并创建 .env 文件
if [ ! -f ".env" ]; then
    echo "📝 创建 .env 文件..."
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
fi

# 设置环境变量
export AIRFLOW_UID=50000

# 创建目录
mkdir -p logs dags plugins

# 设置权限
sudo chown -R $AIRFLOW_UID:0 logs dags plugins 2>/dev/null || echo "⚠️ 无法设置权限，请手动检查"

# 启动服务
echo "🚀 启动所有服务..."
docker-compose up -d

# 等待服务启动
echo "⏳ 等待服务启动..."
echo "等待 MySQL 和 Redis 就绪..."
timeout=60
while [ $timeout -gt 0 ]; do
    if docker-compose exec -T mysql mysqladmin ping -h localhost --silent 2>/dev/null && \
       docker-compose exec -T redis redis-cli ping >/dev/null 2>&1; then
        echo "✅ 基础服务就绪"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done

echo "等待 Airflow 服务启动..."
sleep 30

# 检查服务状态
echo "🔍 检查服务状态..."
docker-compose ps

# 验证服务
echo "✅ 验证服务..."
if curl -f http://localhost:8080/health > /dev/null 2>&1; then
    echo "✅ Airflow Web服务正常"
else
    echo "❌ Airflow Web服务异常"
    exit 1
fi

echo "🎉 部署完成！"
echo "🌐 Airflow UI: http://localhost:8080"
echo "👤 用户名: admin"
echo "🔑 密码: admin"
echo "🌸 Flower监控: http://localhost:5555" 