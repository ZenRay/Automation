# 使用官方Airflow镜像作为基础镜像
FROM apache/airflow:2.11.0-python3.12

# 切换到root用户以安装软件
USER root

# 安装git和系统工具
RUN apt-get update && apt-get install -y git procps curl vim iputils-ping net-tools && apt-get clean

# 切换回airflow用户
USER airflow

# 设置工作目录
WORKDIR /opt/airflow

# 配置工作包：在同一 RUN 步骤中临时清除可能的代理环境变量，确保能直接访问 GitHub
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

# 安装Python依赖 — 在网络/依赖问题时让构建失败以便暴露并修复
# 在同一 RUN 步骤临时清除代理相关环境变量，避免宿主代理影响构建内部的 pip/git 访问
RUN pip install --no-cache-dir -r requirements.txt \
    || (echo "pip install failed, retrying once..." && pip install --no-cache-dir -r requirements.txt)


ENV PYTHONPATH="/opt/airflow:/opt/airflow/Automation:/opt/airflow/Automation/automation:/opt/airflow/Automation/dispatcher:/opt/airflow/Automation/dispatcher/etl_sentence:/opt/airflow/Automation/dispatcher/hooks:/opt/airflow/Automation/dispatcher/operators"
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Shanghai
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# 更新bash配置文件以包含Python搜索路径
RUN echo 'export PYTHONPATH="/opt/airflow:/opt/airflow/Automation:/opt/airflow/Automation/automation:/opt/airflow/Automation/dispatcher:/opt/airflow/Automation/dispatcher/etl_sentence:/opt/airflow/Automation/dispatcher/hooks:/opt/airflow/Automation/dispatcher/operators:$PYTHONPATH"' >> ~/.bashrc

# 验证模块导入
RUN python -c "from automation.client import MaxComputerClient; print('成功导入MaxComputerClient')" || echo "无法导入MaxComputerClient，请检查模块结构"

# 创建健康检查脚本
USER root
RUN echo '#!/bin/bash\ncurl -sf http://localhost:8080/health >/dev/null || exit 1' > /healthcheck.sh && \
    chmod +x /healthcheck.sh
USER airflow

# 健康检查
HEALTHCHECK --interval=30s --timeout=30s --start-period=30s --retries=5 \
    CMD /healthcheck.sh

# 暴露端口
EXPOSE 8080

# 使用 Airflow 的默认入口点
ENTRYPOINT ["/entrypoint"]
CMD ["bash"]
