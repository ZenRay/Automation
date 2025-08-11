# 使用官方Airflow镜像作为基础镜像
FROM apache/airflow:2.7.1-python3.10

# 设置工作目录
WORKDIR /opt/airflow

# 先以root用户安装git
USER root
RUN apt-get update && apt-get install -y git

# 切换到airflow用户
USER airflow

# 配置工作包
RUN git clone https://github.com/ZenRay/Automation.git

# 复制requirements文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 创建必要的目录
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# 验证并创建Automation/automation目录结构
RUN ls -la /opt/airflow/ && \
    echo "验证Automation目录结构..." && \
    ls -la /opt/airflow/Automation/ && \
    echo "验证automation子目录..." && \
    ls -la /opt/airflow/Automation/automation/

# 设置环境变量
ENV PYTHONPATH=/opt/airflow:/opt/airflow/Automation/automation
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Shanghai
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# 更新bash配置文件以包含Python搜索路径
RUN echo 'export PYTHONPATH="/opt/airflow:/opt/airflow/Automation/automation:$PYTHONPATH"' >> ~/.bashrc && \
    echo 'export PYTHONPATH="/opt/airflow:/opt/airflow/Automation/automation:$PYTHONPATH"' >> ~/.profile && \
    echo 'export PYTHONPATH="/opt/airflow/Automation/automation:$PYTHONPATH"' >> ~/.bash_profile

# 创建Python配置文件以确保PYTHONPATH在所有环境中都可用
RUN echo 'import sys' > /opt/airflow/Automation/automation/__init__.py && \
    echo 'sys.path.insert(0, "/opt/airflow/Automation/automation")' >> /opt/airflow/Automation/automation/__init__.py && \
    echo 'print("Automation packages path added to Python sys.path")' >> /opt/airflow/Automation/automation/__init__.py

# 创建.pythonrc文件以确保在Python交互式环境中也能正确设置路径
RUN echo 'import sys' > ~/.pythonrc && \
    echo 'sys.path.insert(0, "/opt/airflow/Automation/automation")' >> ~/.pythonrc && \
    echo 'print("Python interactive environment: Automation packages path added")' >> ~/.pythonrc

# 验证Python路径设置
RUN python -c "import sys; print('Python路径:'); [print(p) for p in sys.path]; print('\\n验证Automation路径是否在sys.path中:'); print('/opt/airflow/Automation/automation' in sys.path)"

# 验证关键包安装
RUN python -c "import airflow, pandas, numpy, odps; print('所有依赖包安装成功')"

# 健康检查
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# 暴露端口
EXPOSE 8080

# 默认命令
CMD ["airflow", "webserver"] 