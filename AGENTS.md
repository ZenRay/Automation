# Automation 项目 AI Agent 指南

> 本文档为 AI Agent 提供项目上下文和开发规范。遵循 AGENTS.md 标准。
>
> **响应语言：请始终使用简体中文回复用户的问题。**

---

## 🚀 快速开始

### 环境设置（3步）

```bash
# 1. 设置 Python 版本
pyenv local 3.12.13

# 2. 创建并激活虚拟环境
uv venv .venv --python 3.12 && source .venv/bin/activate

# 3. 安装依赖
uv pip install -e .
```

### 常用命令

| 任务 | 命令 |
|------|------|
| 运行 OKR 管道 | `python -m workers.okr.main` |
| 验证环境 | `python test_env.py` |
| 测试 MaxCompute | `python test_maxcompute.py` |
| 激活环境 | `source .venv/bin/activate` |
| 退出环境 | `deactivate` |

---

## 📋 项目概述

Automation 是一个数据自动化处理项目，专注于飞书和 MaxCompute 的数据集成与处理。

**技术栈**: Python 3.12.13, pandas, numpy, pyodps, requests

**核心功能**:
1. 飞书 API 集成（多维表格读写）
2. MaxCompute 集成（SQL 查询）
3. 数据管道（多源融合、转换、路由）
4. OKR 数据处理（聚合和指标计算）

---

## 🛠️ 开发环境

```bash
# 激活环境
source .venv/bin/activate

# 退出环境
deactivate

# 安装新包
uv pip install <package>

# 锁定依赖
uv pip freeze > requirements.lock.txt
```

---

## 🔨 构建与测试

```bash
# 运行 OKR 数据管道
python -m workers.okr.main

# 验证环境
python test_env.py

# 测试 MaxCompute
python test_maxcompute.py
```

**测试流程**: 激活环境 → 运行测试 → 检查输出 → 运行管道

---

## 📝 代码规范

**命名**: 文件 `snake_case` | 类 `PascalCase` | 函数/变量 `snake_case` | 常量 `UPPER_SNAKE_CASE`

**导入顺序**:
```python
# 1. 标准库
import json
from pathlib import Path

# 2. 第三方库
import pandas as pd
from odps import ODPS

# 3. 本地模块
from automation.conf import lark as lark_config
```

---

## 🏗️ 架构

```
workers/
├── lib/              # 通用框架层
│   ├── lark_extractor.py    # 飞书数据提取
│   ├── mc_extractor.py      # MaxCompute 提取
│   ├── lark_loader.py       # 飞书加载
│   ├── transformer.py       # 转换
│   ├── router.py            # 路由
│   └── models.py            # 配置模型
└── okr/              # OKR 业务层
    ├── config.py     # 业务配置（唯一注入点）
    ├── main.py       # 主流程
    └── sql/          # SQL 文件
```

**数据流**: 提取 → 转换 → 路由 → 加载

---

## ⚠️ 常见陷阱

### 1. 配置读取
```python
# ❌ 错误
from automation.conf import LarkConfig

# ✅ 正确
from automation.conf import lark as lark_config
app_id = lark_config.get("prod", "APP_ID")
```

### 2. 日期处理
```python
# ✅ 统一日期类型
df['日期'] = pd.to_datetime(df['日期']).dt.date
```

### 3. API 调用
```python
# ✅ field_names 需要 JSON 序列化
field_names = json.dumps(["日期", "商品id"])
```

### 4. Excel 序列号
```python
# ✅ 使用 pd.isna 而非 np.isnat
pd.isna(pd.to_datetime(45753.0, origin='1899-12-30', unit='D'))
```

---

## 🔒 安全

- ❌ 不要硬编码凭据
- ❌ 不要展示真实密钥
- ✅ 使用配置文件
- ✅ 添加到 `.gitignore`

```python
# ✅ 正确
from automation.conf import lark as lark_config
API_KEY = lark_config.get("prod", "APP_SECRET")
```

---

## 📤 提交

**提交前检查**:
```bash
python test_env.py          # 运行测试
black --check .             # 检查格式
python -m workers.okr.main  # 验证功能
```

**提交信息格式**: `<type>(<scope>): <subject>`

类型: `feat` | `fix` | `docs` | `style` | `refactor` | `test` | `chore`

---

## 📚 参考

- [项目启动规划.md](项目启动规划.md)
- [ENV_TEST_REPORT.md](ENV_TEST_REPORT.md)
- [AGENTS.md 官方标准](https://agents.md/)

---

**开始开发前，请确保已完成环境验证！** 🎉
