#!/bin/bash
# 这个脚本用于重置Airflow数据库
# 警告: 会删除所有数据，包括DAG历史、用户和配置!

echo "警告: 将重置Airflow数据库。所有数据将被删除!"
echo "5秒钟后继续，按Ctrl+C取消..."
sleep 5

echo "正在重置数据库..."
docker exec -it dispatcher-airflow-1 bash -c "airflow db reset -y"

echo "正在初始化数据库..."
docker exec -it dispatcher-airflow-1 bash -c "airflow db init && airflow db upgrade"

echo "创建管理员用户..."
docker exec -it dispatcher-airflow-1 bash -c "airflow users create --username admin --firstname Admin --lastname User --email admin@example.com --role Admin --password admin"

echo "重启Airflow服务..."
docker exec -it dispatcher-airflow-1 bash -c "airflow webserver -D && airflow scheduler -D && airflow triggerer -D"

echo "完成! 请访问 http://localhost:8080 使用admin/admin登录。"
