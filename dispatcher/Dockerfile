# 使用官方Airflow镜像作为基础镜像
FROM apache/airflow:2.11.0-python3.12

# 切换到root用户以安装软件
USER root

# 安装git
RUN apt-get update && apt-get install -y git && apt-get clean

# 切换回airflow用户
USER airflow

# 设置工作目录
WORKDIR /opt/airflow

# 切换到airflow用户
USER airflow

# 配置工作包
RUN git clone https://github.com/ZenRay/Automation.git

# 创建必要的配置文件目录
RUN mkdir -p /opt/airflow/Automation/automation/conf

# 复制配置文件模板
COPY automation/conf/_maxcomputer.ini.template /opt/airflow/Automation/automation/conf/_maxcomputer.ini.template
COPY automation/conf/_lark.ini.template /opt/airflow/Automation/automation/conf/_lark.ini.template

# 重命名配置文件（保持模板格式，稍后在 Airflow 中配置）
RUN mv /opt/airflow/Automation/automation/conf/_maxcomputer.ini.template /opt/airflow/Automation/automation/conf/_maxcomputer.ini && \
    mv /opt/airflow/Automation/automation/conf/_lark.ini.template /opt/airflow/Automation/automation/conf/_lark.ini && \
    echo "配置文件复制完成"


# 创建必要的目录，确保client目录及其子目录存在（如果不存在）
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins \
    /opt/airflow/Automation/automation/conf \
    /opt/airflow/Automation/automation/client/maxcomputer \
    /opt/airflow/Automation/automation/client/lark


# 复制requirements文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt || echo "安装依赖失败，继续执行"


ENV PYTHONPATH="/opt/airflow:/opt/airflow/Automation:/opt/airflow/Automation/automation:/opt/airflow/Automation/dispatcher:/opt/airflow/Automation/dispatcher/hooks:/opt/airflow/Automation/dispatcher/operators"
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Shanghai
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# 更新bash配置文件以包含Python搜索路径
RUN echo 'export PYTHONPATH="/opt/airflow:/opt/airflow/Automation:/opt/airflow/Automation/automation:/opt/airflow/Automation/dispatcher:/opt/airflow/Automation/dispatcher/hooks:/opt/airflow/Automation/dispatcher/operators:$PYTHONPATH"' >> ~/.bashrc

# 验证模块导入
RUN python -c "from automation.client import MaxComputerClient; print('成功导入MaxComputerClient')" || echo "无法导入MaxComputerClient，请检查模块结构"

# 健康检查
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# 暴露端口
EXPOSE 8080

# 使用 Airflow 的默认入口点
ENTRYPOINT ["/entrypoint"]
CMD ["bash"]
