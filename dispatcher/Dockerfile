# 使用官方Airflow镜像作为基础镜像
FROM apache/airflow:2.11.0-python3.12

# 切换到root用户以安装软件
USER root

# 安装git
RUN apt-get update && apt-get install -y git vim curl


# 设置工作目录
WORKDIR /opt/airflow

# 切换到airflow用户
USER airflow


# 创建必要的目录，确保client目录及其子目录存在（如果不存在）
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins \
    /opt/airflow/Automation


# 复制requirements文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

# 设置环境变量
ENV PYTHONPATH="/opt/airflow:/opt/airflow/automation:/opt/airflow/dispatcher"
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Shanghai
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# 更新bash配置文件以包含Python搜索路径
RUN echo 'export PYTHONPATH="/opt/airflow:/opt/airflow/automation:/opt/airflow/dispatcher:$PYTHONPATH"' >> ~/.bashrc


# 健康检查
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# 暴露端口
EXPOSE 8080

# 使用 Airflow 的默认入口点
ENTRYPOINT ["/entrypoint"]
CMD ["bash"]
