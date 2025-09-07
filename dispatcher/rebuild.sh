#!/bin/bash
# 重建 Airflow Docker 服务脚本
# 完整重建Airflow环境，包括镜像和容器

echo "===== 开始重建 Airflow 服务 ====="

# 步骤 1: 确保停止所有现有容器
echo "正在停止所有现有容器..."
# 首先使用 docker-compose 停止服务
docker-compose down
# 查找并强制停止所有与 dispatcher 或 airflow 相关的容器
echo "检查是否有遗留的容器..."
CONTAINERS=$(docker ps -a -q --filter name=dispatcher --filter name=airflow)
if [ -n "$CONTAINERS" ]; then
  echo "发现遗留容器，正在停止和删除..."
  docker stop $CONTAINERS
  docker rm $CONTAINERS
fi
echo "所有容器已停止"

# 步骤 2: 生成新的 FERNET_KEY
echo "正在生成新的 FERNET_KEY..."
if command -v python3 &>/dev/null; then
  NEW_FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
  if [ -n "$NEW_FERNET_KEY" ]; then
    echo "生成了新的 FERNET_KEY: $NEW_FERNET_KEY"
    # 更新 .env 文件中的 FERNET_KEY
    sed -i "s|^AIRFLOW__CORE__FERNET_KEY=.*|AIRFLOW__CORE__FERNET_KEY=$NEW_FERNET_KEY|" .env
    echo "已更新 .env 文件中的 FERNET_KEY"
  else
    echo "无法生成 FERNET_KEY，将使用默认值"
  fi
else
  echo "未找到 python3，将使用默认 FERNET_KEY"
fi

# 步骤 3: 清理日志目录
echo "正在清理日志目录..."
# 确保删除所有日志文件和目录
rm -rf ./logs/*
# 删除任何隐藏文件
rm -rf ./logs/.*
# 重新创建目录并设置权限
mkdir -p ./logs
# 确保目录具有完全的读写权限
chmod -R 777 ./logs
# WSL 环境下可能需要这样设置权限
echo "尝试使用 sudo 设置权限（可能需要密码）..."
sudo chmod -R 777 ./logs 2>/dev/null || echo "继续执行，忽略 sudo 错误"

# 步骤 4: 询问是否清理数据库卷
echo "检查数据库卷..."
# 使用docker volume ls 检查数据库卷是否存在
if docker volume ls | grep -q "dispatcher_mysql-db-volume"; then
  echo "数据库卷已存在"
  read -p "是否要清理数据库卷？这将删除所有现有的 Airflow 元数据（y/N）: " CLEAN_DB
  if [ "$CLEAN_DB" = "y" ] || [ "$CLEAN_DB" = "Y" ]; then
    echo "正在删除数据库卷..."
    docker volume rm dispatcher_mysql-db-volume 2>/dev/null || true
    echo "数据库卷已删除，将在启动时创建新的数据库"
  else
    echo "保留现有数据库卷"
  fi
else
  echo "未找到数据库卷，将创建新的空数据库"
fi

# 步骤 5: 删除旧容器和镜像
echo "正在删除旧容器和镜像..."
# 删除与此项目相关的所有容器
docker rm -f $(docker ps -a -q --filter name=dispatcher) 2>/dev/null || true
# 删除旧镜像
docker rmi dispatcher-airflow:latest 2>/dev/null || true

# 步骤 6: 重新构建镜像
echo "正在重新构建镜像..."
docker build --network=host -t dispatcher-airflow:latest .

# 步骤 7: 启动容器
echo "正在启动容器..."
docker-compose up -d

# 步骤 8: 等待容器启动
echo "等待容器启动 (10秒)..."
sleep 10

# 步骤 9: 检查容器状态
echo "检查容器状态..."
if docker ps | grep -q dispatcher-airflow-1; then
  echo "容器已成功启动"
  # 检查环境变量设置
  echo "检查容器内环境变量..."
  docker exec -it dispatcher-airflow-1 bash -c "env | grep -i proxy" || echo "无法读取环境变量"
else
  echo "警告: 容器未启动，请检查日志"
  docker-compose logs
fi

# 步骤 10: 再次等待服务完全启动
echo "等待服务完全启动 (30秒)..."
sleep 30

# 步骤 11: 检查容器状态
echo "正在检查容器状态..."
docker-compose ps

# 步骤 12: 验证服务健康状态
echo "正在验证服务健康状态..."
if curl -s -u admin:admin http://localhost:8080/api/v1/health; then
  echo -e "\nAirflow 服务健康检查完成"
else
  echo -e "\n警告: 无法获取健康状态，可能需要更多时间启动"
fi

# 步骤 13: 验证服务进程
echo "检查关键服务进程..."
docker exec -it dispatcher-airflow-1 bash -c "ps -ef | grep 'airflow' | grep -v grep"

# 步骤 14: 确保服务运行正常
echo "正在确认服务是否需要修复..."
if curl -s -u admin:admin http://localhost:8080/api/v1/health | grep -q '"status": "unhealthy"'; then
  echo "检测到服务异常，尝试修复..."
  chmod +x ./fix_airflow.sh
  ./fix_airflow.sh
else
  echo "服务状态正常，无需修复"
fi

echo "===== Airflow 服务重建完成 ====="
echo "请访问 http://localhost:8080 检查 Airflow UI"
echo "默认登录凭证: admin / admin"
echo ""
echo "如果遇到问题，可以使用以下命令:"
echo "  ./fix_airflow.sh    - 修复常见的Airflow问题"
echo "  ./test_maxcompute_master.sh -c  - 测试MaxCompute连接（使用Airflow连接）"
echo "  ./test_maxcompute_master.sh -e  - 测试MaxCompute连接（使用环境变量）"

# 步骤15: 最终验证
echo "执行最终环境验证..."
echo -e "\n检查FERNET_KEY配置:"
grep "AIRFLOW__CORE__FERNET_KEY" .env

echo -e "\n检查容器内代理设置:"
docker exec -it dispatcher-airflow-1 bash -c "env | grep -i proxy" || echo "无法连接容器，请检查容器状态"

echo -e "\n检查scheduler服务状态:"
curl -s -u admin:admin http://localhost:8080/api/v1/health | grep scheduler || echo "无法获取scheduler状态"

echo -e "\n重建过程完成!"
