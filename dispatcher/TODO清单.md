# 数据开发工具项目 TODO 清单

## 项目概述
基于Airflow的ETL自动化调度数据开发工具项目，旨在实现现有Maxcompute ETL流程的自动化调度，并培养工程师在Docker、K8s和Airflow方面的核心能力。

## 🎯 当前项目状态 (2025-08-07)

### ✅ 已完成的重要里程碑
- **Docker环境搭建完成** - 完整的Docker Compose配置
- **Airflow服务集群** - Webserver、Scheduler、Worker、Triggerer全部正常运行
- **数据库配置** - MySQL元数据数据库配置完成
- **自定义镜像** - 包含Maxcompute SDK的Airflow镜像构建完成
- **部署脚本** - 自动化启动/停止脚本开发完成
- **文档完善** - 项目文档、部署指南、故障排查指南完成
- **从零验证** - 完整的从零开始部署流程验证通过

### 🔄 下一步重点任务
1. **Maxcompute连接配置** - 配置具体的AccessKey和项目信息
2. **第一个ETL DAG开发** - 创建和测试Maxcompute ETL任务
3. **任务依赖和调度** - 实现复杂的DAG依赖关系
4. **监控和告警** - 集成Prometheus/Grafana监控

### 📊 完成度统计
- 阶段一 (Airflow基础): **100%** ✅
- 阶段二 (高级特性): **20%** 🔄
- 阶段三 (Docker进阶): **90%** ✅
- 阶段四 (Kubernetes): **0%** ⏳

---

## 📚 Airflow DAG 开发学习教程

### 🎯 学习目标
通过系统学习 Airflow DAG 开发，掌握 ETL 自动化调度的核心技能，为后续复杂业务场景打下坚实基础。

### 📅 学习计划概览
- **第一阶段 (Day 1-2)**: Airflow 基础概念和组件
- **第二阶段 (Day 3-5)**: DAG 开发基础和核心技能
- **第三阶段 (Day 6-8)**: 高级 DAG 设计和监控告警
- **第四阶段 (Day 9-10)**: Docker 集成优化和生产准备

---

## 🚀 第一阶段：Airflow 基础概念与组件 (第1-2天)

### Day 1: 核心概念理解 ✅ **已完成**
**学习目标**：理解 Airflow 的基本架构和工作原理

**核心概念掌握**：
- [x] **DAG (Directed Acyclic Graph)**：有向无环图，定义任务执行顺序
- [x] **Task**：DAG 中的具体执行单元
- [x] **Operator**：任务的执行器类型
- [x] **Scheduler**：调度器，负责触发 DAG 执行
- [x] **Executor**：执行器，负责实际运行任务

**实践任务完成**：
- [x] 在 Airflow UI 中查看现有 DAG 的 Graph View
- [x] 理解 DAG 的三种状态：Running、Success、Failed
- [x] 熟悉 Airflow UI 的各个页面功能

**评估标准**: 能够清晰解释 Airflow 的核心概念，并在 UI 中正确识别各种状态和功能。

### Day 2: 基础组件学习 ✅ **已完成**
**学习目标**：掌握 Airflow 的核心组件和配置

**关键组件理解**：
- [x] **Webserver**：Web 界面，用于监控和管理
- [x] **Scheduler**：调度器，按时间触发 DAG
- [x] **Worker**：工作节点，执行具体任务
- [x] **Metadata Database**：元数据存储（MySQL）

**实践任务完成**：
- [x] 检查当前 Airflow 集群状态：`docker-compose ps`
- [x] 查看 Scheduler 日志：`docker logs dispatcher-airflow-scheduler-1`
- [x] 理解 CeleryExecutor 的工作原理

**评估标准**: 能够独立检查 Airflow 集群状态，理解各组件的作用和相互关系。

---

## 🛠️ 第二阶段：DAG 开发基础与核心技能 (第3-5天)

### Day 3: 第一个 DAG 开发 🔄 **进行中**
**学习目标**：创建并运行第一个简单的 DAG

**当前状态**：已有 `example_maxcompute_dag.py` 示例，需要完成配置和测试

**下一步任务**：
- [ ] **配置 Maxcompute 连接**：
  ```python
  # 在 Airflow UI 中配置 Connection
  # Connection Id: maxcompute_default
  # Connection Type: Generic
  # Host: your_maxcompute_endpoint
  # Login: your_access_key_id
  # Password: your_access_key_secret
  # Extra: {"project": "your_project_name"}
  ```

- [ ] **测试 DAG 执行**：
  - [ ] 在 Airflow UI 中启用 DAG
  - [ ] 手动触发执行
  - [ ] 检查任务日志和状态
  - [ ] 验证 Maxcompute 连接有效性

**评估标准**: DAG 文件成功上传至 Airflow，在 Airflow UI 中可见并可启用。手动触发 DAG 后，任务成功执行，且 Maxcompute 中能看到相应的查询或数据导入结果，任务日志无报错。

### Day 4: 任务依赖和调度配置
**学习目标**：掌握任务依赖关系和调度时间设置

**关键技能学习**：
```python
# 任务依赖设置
task1 >> task2 >> task3  # 顺序执行
task1 >> [task2, task3]  # 并行执行
[task1, task2] >> task3  # 等待多个任务完成

# 调度时间设置
schedule_interval='0 2 * * *'  # Cron 表达式
schedule_interval=timedelta(hours=1)  # 时间间隔
schedule_interval=None  # 手动触发
```

**实践任务**：
- [ ] **任务依赖设计**
  - [ ] 在 DAG 中添加多个任务
  - [ ] 使用 `>>` 和 `<<` 操作符设置任务依赖
  - [ ] 设计合理的任务执行顺序
  - [ ] 在 Airflow UI 中验证依赖关系

- [ ] **调度周期配置**
  - [ ] 设置 DAG 的 schedule_interval 参数
  - [ ] 使用 Cron 表达式定义调度时间
  - [ ] 测试不同调度周期下的 DAG 运行
  - [ ] 验证调度器正常工作

- [ ] **任务参数优化**
  - [ ] 配置任务重试机制
  - [ ] 设置任务超时时间
  - [ ] 优化任务执行参数

**评估标准**: DAG 中任务的上下游关系在 Airflow UI 的 Graph View 中正确显示。设置调度周期后，DAG 能按预期自动触发运行，且任务执行顺序符合依赖关系。

### Day 5: 连接和变量管理
**学习目标**：使用 Airflow Connections 和 Variables 管理配置

**关键技能学习**：
```python
# 获取连接信息
from airflow.hooks.base import BaseHook
connection = BaseHook.get_connection("maxcompute_default")

# 获取变量
from airflow.models import Variable
target_table = Variable.get("target_table", default_var="default_table")
```

**实践任务**：
- [ ] **Connections 配置**
  - [ ] 在 Airflow UI 中配置 Maxcompute 连接
  - [ ] 设置 AccessKey ID 和 AccessKey Secret
  - [ ] 配置项目名称和端点信息
  - [ ] 测试连接有效性

- [ ] **Variables 管理**
  - [ ] 创建常用参数变量
  - [ ] 设置环境相关配置
  - [ ] 管理业务参数
  - [ ] 验证变量读取功能

- [ ] **DAG 集成**
  - [ ] 修改 DAG 使用 Connection 获取连接信息
  - [ ] 使用 Variables 读取配置参数
  - [ ] 移除硬编码的敏感信息
  - [ ] 测试 DAG 正常运行

**评估标准**: Maxcompute 连接信息通过 Airflow Connection 配置，DAGs 通过 `BaseHook` 或 `Connection` 对象成功获取连接。常用参数通过 Airflow Variables 管理，DAGs 能正确读取并使用这些参数，且无硬编码敏感信息。

---

## 🚀 第三阶段：高级 DAG 设计与监控告警 (第6-8天)

### Day 6-7: 复杂 DAG 设计
**学习目标**：设计包含分支、并行、条件判断的复杂 DAG

**高级特性学习**：
```python
# 分支执行
from airflow.operators.python import BranchPythonOperator

def decide_branch(**context):
    if some_condition:
        return 'branch_a'
    else:
        return 'branch_b'

branch_task = BranchPythonOperator(
    task_id='branch_decision',
    python_callable=decide_branch,
    dag=dag
)

# 并行执行
with TaskGroup("parallel_tasks") as parallel_group:
    task_a = PythonOperator(task_id='task_a', ...)
    task_b = PythonOperator(task_id='task_b', ...)
    task_c = PythonOperator(task_id='task_c', ...)

# 条件执行
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
```

**实践任务**：
- [ ] **现有代码分析**
  - [ ] 分析现有 Maxcompute ETL 代码结构
  - [ ] 识别可封装为 Airflow 任务的逻辑单元
  - [ ] 确定任务粒度和依赖关系
  - [ ] 评估代码质量和可维护性

- [ ] **ETL 任务迁移**
  - [ ] 将现有 ETL 脚本转换为 Airflow DAG
  - [ ] 使用 PythonOperator 封装 ETL 逻辑
  - [ ] 实现数据提取任务
  - [ ] 实现数据转换任务
  - [ ] 实现数据加载任务

- [ ] **任务组织**
  - [ ] 为每个 ETL 流程创建独立 DAG
  - [ ] 使用 TaskGroup 组织复杂任务
  - [ ] 设计合理的任务命名规范
  - [ ] 实现模块化的任务结构

**评估标准**: 至少一个现有 Maxcompute ETL 脚本被成功转换为 Airflow DAG，并能通过 Airflow 调度执行。DAG 结构清晰，任务粒度合理，并能正确处理数据。

### Day 8: 监控和告警配置
**学习目标**：实现 DAG 执行监控和失败告警

**监控功能实现**：
```python
# 任务重试配置
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

# 自定义监控
def monitor_task(**context):
    # 实现自定义监控逻辑
    pass
```

**实践任务**：
- [ ] **复杂 DAG 设计**
  - [ ] 设计包含分支逻辑的 DAG
  - [ ] 实现条件判断任务
  - [ ] 配置并行执行任务
  - [ ] 设计错误处理机制

- [ ] **高级 Operators 使用**
  - [ ] 使用 BranchPythonOperator 实现分支
  - [ ] 使用 Sensor 等待外部事件
  - [ ] 使用 XCom 在任务间传递数据
  - [ ] 实现自定义 Operator（如需要）

- [ ] **测试与验证**
  - [ ] 编写单元测试
  - [ ] 进行集成测试
  - [ ] 模拟各种执行场景
  - [ ] 验证错误处理机制

**评估标准**: 设计并实现一个包含分支、并行、条件判断等复杂逻辑的 DAG。通过模拟数据或小规模真实数据进行充分测试，确保所有分支和路径都能正确执行，且错误处理机制有效。

---

## 🐳 第四阶段：Docker 集成优化与生产准备 (第9-10天)

### Day 9: 镜像优化和依赖管理 ✅ **已完成**
**学习目标**：优化 Docker 镜像和管理 Python 依赖

**已完成内容**：
- [x] **Dockerfile 设计**
  - [x] 基于官方 Airflow 镜像创建 Dockerfile
  - [x] 安装 Maxcompute Python SDK (pyodps>=0.11.0)
  - [x] 安装其他必要的 Python 依赖
  - [x] 配置环境变量和权限

- [x] **镜像优化**
  - [x] 优化 Docker 层结构
  - [x] 利用构建缓存加速构建
  - [x] 设置合适的用户权限
  - [x] 移除不必要的系统包安装

- [x] **镜像测试**
  - [x] 构建自定义镜像
  - [x] 验证 Maxcompute SDK 安装
  - [x] 测试 Python 依赖导入
  - [x] 确认镜像大小合理 (~2.15GB)

- [x] **部署更新**
  - [x] 更新 docker-compose.yml 使用自定义镜像
  - [x] 重启 Airflow 服务
  - [x] 验证服务正常运行
  - [x] 测试从零开始完整部署流程

**评估标准**: 成功构建自定义 Airflow Worker 镜像，镜像大小合理。通过 `docker run` 命令进入容器，验证 Maxcompute Python SDK 及其他必要依赖已正确安装并可正常调用。更新 `docker-compose.yml` 后，Airflow 服务能正常启动并使用新镜像。

### Day 10: 生产环境配置和监控集成
**学习目标**：准备生产环境部署配置和监控系统

**实践任务**：
- [ ] **生产环境配置**
  - [ ] 创建生产环境 docker-compose.yml
  - [ ] 配置生产环境变量
  - [ ] 设置资源限制和健康检查
  - [ ] 配置日志收集策略

- [ ] **监控配置**
  - [ ] 集成 Prometheus 监控（如时间允许）
  - [ ] 配置 Grafana 仪表盘
  - [ ] 设置 Airflow 指标收集
  - [ ] 配置告警规则

- [ ] **告警机制**
  - [ ] 配置邮件通知
  - [ ] 设置 Webhook 告警
  - [ ] 定义告警阈值
  - [ ] 测试告警功能

- [x] **部署脚本**
  - [x] 编写自动化部署脚本 (scripts/start.sh)
  - [x] 创建环境检查脚本 (scripts/check_config.sh)
  - [x] 编写服务启动/停止脚本 (scripts/start.sh, scripts/stop.sh)
  - [x] 配置备份和恢复策略 (Docker 卷管理)

**评估标准**: 编写了可用于生产环境部署的 `docker-compose.yml` 或其他部署脚本，并进行了初步测试。如果时间允许，配置了 Airflow 的监控（如 Prometheus/Grafana 仪表盘）或告警机制（如邮件通知），并验证其功能。

---

## ☸️ 第五阶段：Kubernetes 基础与 Airflow 未来展望 (第16-20天)

### Day 15-16: 学习 K8s 核心概念，并安装 Minikube
**学习目标**：掌握 Kubernetes 基础概念和本地环境搭建

**实践任务**：
- [ ] **K8s 理论学习**
  - [ ] 学习 Pod 概念和生命周期
  - [ ] 理解 Deployment 和 Service
  - [ ] 掌握 Namespace 和 Volume
  - [ ] 了解 K8s 基本架构

- [ ] **Minikube 环境搭建**
  - [ ] 安装 Minikube
  - [ ] 启动本地 K8s 集群
  - [ ] 安装 kubectl 命令行工具
  - [ ] 验证集群状态

- [ ] **基础操作练习**
  - [ ] 使用 kubectl 查看集群信息
  - [ ] 创建和管理 Pod
  - [ ] 部署简单应用
  - [ ] 查看 Pod 日志和状态

**评估标准**: 能够清晰阐述 Pod、Deployment、Service、Namespace、Volume 等 K8s 核心概念。成功安装 Minikube，并能使用 `kubectl` 命令进行基本的集群操作（如查看节点、Pod）。

### Day 17-18: 了解 Airflow KubernetesExecutor 原理，并尝试在 Minikube 上运行
**学习目标**：理解 KubernetesExecutor 工作原理并在 K8s 环境中部署 Airflow

**实践任务**：
- [ ] **KubernetesExecutor 学习**
  - [ ] 理解 KubernetesExecutor 工作原理
  - [ ] 学习 Pod 模板配置
  - [ ] 了解资源管理和调度
  - [ ] 掌握配置参数含义

- [ ] **Minikube 部署实践**
  - [ ] 在 Minikube 上部署 Airflow
  - [ ] 配置 KubernetesExecutor
  - [ ] 设置必要的 K8s 资源
  - [ ] 验证 Airflow 服务启动

- [ ] **DAG 测试**
  - [ ] 部署简单 DAG 到 K8s 环境
  - [ ] 观察任务在 Pod 中执行
  - [ ] 验证 Pod 创建和销毁
  - [ ] 检查任务执行日志

**评估标准**: 能够解释 Airflow KubernetesExecutor 的工作原理。在 Minikube 上成功部署一个简单的 Airflow 实例，并配置为使用 KubernetesExecutor。运行一个简单的 DAG，验证任务能在 K8s Pod 中执行，且 Pod 能正常创建和销毁。

### Day 19: 完善项目文档和总结
**学习目标**：整理项目成果，完善技术文档

**实践任务**：
- [ ] **技术文档完善**
  - [ ] 更新技术架构文档
  - [ ] 编写部署指南
  - [ ] 创建 DAG 开发规范
  - [ ] 整理配置说明文档

- [ ] **学习总结**
  - [ ] 整理学习笔记
  - [ ] 记录遇到的问题和解决方案
  - [ ] 总结最佳实践
  - [ ] 编写经验分享文档

- [ ] **代码整理**
  - [ ] 代码审查和优化
  - [ ] 添加必要的注释
  - [ ] 整理项目结构
  - [ ] 准备代码交付

**评估标准**: 项目规划文档内容完整、准确，涵盖技术架构、部署指南、DAG 开发规范等。整理了项目期间的学习笔记、遇到的问题及解决方案，形成经验总结文档。

### Day 20: 完成项目交付与成果展示
**学习目标**：展示项目成果，完成项目交付

**实践任务**：
- [ ] **系统演示准备**
  - [ ] 准备演示环境
  - [ ] 整理演示脚本
  - [ ] 准备演示数据
  - [ ] 测试演示流程

- [ ] **成果展示**
  - [ ] 展示 ETL 自动化调度系统
  - [ ] 演示 Airflow DAGs 运行
  - [ ] 展示监控和日志功能
  - [ ] 分享技术选型理由

- [ ] **项目交付**
  - [ ] 交付项目代码
  - [ ] 交付技术文档
  - [ ] 交付部署脚本
  - [ ] 完成项目总结

**评估标准**: 向团队成功展示 ETL 自动化调度系统，演示 Airflow DAGs 的运行、监控和日志查看。清晰地分享项目成果、技术选型理由、遇到的挑战及解决方案，并回答团队提问。项目代码和文档已归档。

---

## 🎯 本周重点任务清单 (Day 3-5)

### **立即执行 (今天)**
- [ ] **配置 Maxcompute 连接**
  - [ ] 获取 AccessKey ID 和 Secret
  - [ ] 在 Airflow UI 中创建 Connection
  - [ ] 测试连接有效性

- [ ] **测试现有 DAG**
  - [ ] 启用 `example_maxcompute_dag`
  - [ ] 手动触发执行
  - [ ] 检查任务日志和状态

### **明天 (Day 4)**
- [ ] **扩展 DAG 功能**
  - [ ] 添加数据质量检查任务
  - [ ] 实现任务重试机制
  - [ ] 测试不同的调度配置

- [ ] **学习任务依赖**
  - [ ] 理解 `>>` 和 `<<` 操作符
  - [ ] 设计并行执行任务
  - [ ] 在 UI 中验证依赖关系

### **后天 (Day 5)**
- [ ] **配置管理优化**
  - [ ] 创建常用业务变量
  - [ ] 移除硬编码配置
  - [ ] 测试配置读取功能

---

## 🔄 下周任务规划 (Day 6-10)

### **Day 6-7: 复杂 DAG 开发**
- [ ] 设计包含分支逻辑的 ETL 流程
- [ ] 实现条件判断和并行执行
- [ ] 使用 TaskGroup 组织任务

### **Day 8: 监控告警**
- [ ] 配置邮件通知
- [ ] 实现自定义监控
- [ ] 设置任务超时

### **Day 9-10: 生产准备**
- [ ] 镜像优化
- [ ] 生产环境配置
- [ ] 性能测试

---

## 🛠️ 学习资源与最佳实践

### **官方文档**
- [Airflow 官方文档](https://airflow.apache.org/docs/)
- [DAG 编写指南](https://airflow.apache.org/docs/apache-airflow/stable/howto/index.html)
- [Maxcompute Python SDK 文档](https://pyodps.readthedocs.io/)

### **实践练习建议**
- [ ] 每天至少开发/修改一个 DAG
- [ ] 尝试不同的 Operator 类型
- [ ] 测试各种错误场景和恢复机制
- [ ] 记录遇到的问题和解决方案

### **代码质量要求**
- [ ] 定期检查 DAG 代码质量
- [ ] 优化任务执行效率
- [ ] 完善错误处理和日志记录
- [ ] 遵循 Python 编码规范

---

## 📊 项目检查清单

### 环境检查
- [x] Docker 和 Docker Compose 已安装
- [x] Python 3.10+ 环境已配置 (Docker 镜像中)
- [x] 网络连接正常
- [x] 磁盘空间充足

### 配置检查
- [x] MySQL 连接信息已获取 (Docker Compose 配置)
- [ ] Maxcompute AccessKey 已配置 (待具体业务需求)
- [x] Airflow 配置文件已正确设置 (docker-compose.yml)
- [x] 环境变量已配置 (.env 文件)

### 代码检查
- [ ] DAG 文件语法正确
- [ ] 任务依赖关系合理
- [ ] 错误处理机制完善
- [ ] 日志记录充分

### 测试检查
- [ ] 单元测试已编写
- [ ] 集成测试已通过
- [ ] 性能测试已执行
- [ ] 安全测试已进行

### 文档检查
- [x] 技术文档完整 (README.md, docs 目录)
- [x] 部署指南清晰 (README.md 中的安装步骤)
- [x] 用户手册已编写 (故障排查指南)
- [x] API 文档已更新 (项目结构说明)

---

## 📝 学习进度跟踪

### **每日检查点**
- [ ] 完成当天的学习目标
- [ ] 记录遇到的问题和解决方案
- [ ] 更新 TODO 清单状态
- [ ] 提交代码到版本控制

### **每周回顾**
- [ ] 总结学习成果
- [ ] 调整下周计划
- [ ] 识别学习瓶颈
- [ ] 分享经验教训

### **里程碑检查**
- [ ] 阶段一完成后进行技能评估
- [ ] 阶段二完成后进行 DAG 开发能力评估
- [ ] 阶段三完成后进行 Docker 技能评估
- [ ] 阶段四完成后进行 K8s 基础评估

---

## ⚠️ 注意事项

1. **每日进度跟踪**: 每天结束时检查当天 TODO 完成情况，及时调整计划
2. **问题记录**: 遇到问题时及时记录，避免重复踩坑
3. **代码备份**: 定期提交代码到版本控制系统
4. **团队沟通**: 定期与团队成员沟通进度和问题
5. **质量保证**: 每个阶段完成后进行质量检查，确保符合评估标准
6. **实践为主**: 理论学习要与实际开发相结合，多动手实践
7. **文档更新**: 及时更新技术文档，记录学习心得和最佳实践

---

## 🎯 成功标准

- [ ] 所有 ETL 任务成功迁移到 Airflow
- [ ] 系统稳定运行，无重大故障
- [ ] 工程师掌握 Docker、K8s、Airflow 核心技能
- [ ] 项目文档完整，便于后续维护
- [ ] 为未来分布式部署打下基础
- [ ] 形成可复用的 DAG 开发模式和最佳实践
- [ ] 建立完善的监控和告警机制
- [ ] 具备生产环境部署和维护能力 