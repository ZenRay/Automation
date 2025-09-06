# 使用官方Airflow镜像作为基础镜像
FROM apache/airflow:2.11.0-python3.12

# 设置工作目录
WORKDIR /opt/airflow

# 先以root用户安装git
USER root
RUN apt-get update && apt-get install -y git

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

# 复制requirements文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt


ENV PYTHONPATH="/opt/airflow:/opt/airflow/Automation:/opt/airflow/Automation/automation:/opt/airflow/Automation/dispatcher:/opt/airflow/Automation/dispatcher/hooks:/opt/airflow/Automation/dispatcher/operators"
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Shanghai
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# 更新bash配置文件以包含Python搜索路径
RUN echo 'export PYTHONPATH="/opt/airflow:/opt/airflow/Automation:/opt/airflow/Automation/automation:/opt/airflow/Automation/dispatcher:/opt/airflow/Automation/dispatcher/hooks:/opt/airflow/Automation/dispatcher/operators:$PYTHONPATH"' >> ~/.bashrc

# # 创建Python配置文件以确保PYTHONPATH在所有环境中都可用
# RUN echo 'import sys' > /opt/airflow/Automation/automation/__init__.py && \
#     echo 'sys.path.insert(0, "/opt/airflow/Automation/automation")' >> /opt/airflow/Automation/automation/__init__.py && \
#     echo 'print("Automation packages path added to Python sys.path")' >> /opt/airflow/Automation/automation/__init__.py

# 创建.pythonrc文件以确保在Python交互式环境中也能正确设置路径
RUN echo 'import sys' > ~/.pythonrc && \
    echo 'sys.path.insert(0, "/opt/airflow/Automation/automation")' >> ~/.pythonrc && \
    echo 'sys.path.insert(0, "/opt/airflow/Automation")' >> ~/.pythonrc && \
    echo 'print("Python interactive environment: Automation packages path added")' >> ~/.pythonrc

# 验证Python路径设置
RUN python -c "import sys; print('Python路径:'); [print(p) for p in sys.path]; print('\\n验证Automation路径是否在sys.path中:'); print('/opt/airflow/Automation/automation' in sys.path)"

# 验证关键包安装
RUN python -c "import airflow, pandas, numpy, odps; print('所有依赖包安装成功')"
# 注意：暂时跳过 automation 模块导入验证，因为需要配置文件
# RUN python -c "import automation; print('Automation模块导入成功')"

# 健康检查
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# 暴露端口
EXPOSE 8080

# 使用 Airflow 的默认入口点和命令 