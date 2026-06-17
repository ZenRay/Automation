# workers 模块 AI Agent 指南

> 本指南为 AI Agent 提供 workers 模块的职责边界和开发规范。

---

## 📋 模块定位

**workers** 是应用层的数据处理模块，专注于数据管道任务的实现。

**核心职责**: 实现业务逻辑，使用框架层提供的通用组件。

---

## ✅ workers 应该做什么

### 1. 数据管道任务
- 实现标准的 ETL 流程：**提取 → 转换 → 路由 → 加载**
- 使用 `workers/lib/` 提供的通用组件
- 遵循数据流规范

### 2. 业务逻辑实现
- OKR 指标计算和聚合
- 数据转换规则
- 字段类型映射
- 日期格式处理

### 3. 配置驱动
- 通过 `config.py` 定义数据源、SQL 查询、目标表
- 使用命名常量而非硬编码
- 集中管理业务配置

### 4. 使用框架组件
- 复用 `workers/lib/` 的提取器、加载器、转换器
- 遵循框架的接口规范
- 通过配置扩展功能

---

## ❌ workers 不应该做什么

### 1. 不修改框架层
- ❌ **严禁修改 `automation/` 目录下的任何代码文件**（包括 `automation/client/`、`automation/conf/`、`automation/utils/` 等）
- ❌ 未经用户明确确认，不得对 `automation/` 层进行任何改动
- ❌ 不要在业务层添加通用逻辑
- ✅ 如需修改 `automation/` 层，必须先向用户说明原因并获得明确确认
- ✅ 应该通过配置或扩展点来实现功能

### 2. 不处理底层 API
- ❌ 不要直接调用 Lark API
- ❌ 不要直接调用 MaxCompute API
- ✅ 应该使用 `automation/client/` 提供的客户端

### 3. 不包含基础设施代码
- ❌ 不要处理认证、连接池、重试逻辑
- ❌ 不要处理配置文件解析（使用 `automation/conf/`）
- ✅ 这些都在 `automation/` 层处理

### 4. 不硬编码业务规则
- ❌ 不要在代码中硬编码字段名、表名
- ❌ 不要硬编码 SQL 查询
- ✅ 应该在 `config.py` 中定义

### 5. 不跨业务域混合
- ❌ 不要在 OKR 管道中混入其他业务逻辑
- ✅ 每个业务域应该是独立的模块（如 `workers/okr/`、`workers/finance/`）

---

## 🏗️ 目录结构

```
workers/
├── lib/              # 通用框架层（可复用组件）
│   ├── lark_extractor.py    # 飞书数据提取
│   ├── mc_extractor.py      # MaxCompute 提取
│   ├── lark_loader.py       # 飞书加载
│   ├── transformer.py       # 数据转换
│   ├── router.py            # 数据路由
│   ├── validator.py         # 数据校验
│   ├── type_coercer.py      # 类型转换
│   └── models.py            # 配置模型
│
└── okr/              # OKR 业务层（具体实现）
    ├── config.py     # 业务配置（唯一注入点）
    ├── main.py       # 主流程入口
    ├── transformer.py # OKR 转换逻辑
    └── sql/          # SQL 文件
```

---

## 🚀 运行命令

```bash
# 运行 OKR 数据管道
python -m workers.okr.main

# 验证环境
python test_env.py

# 测试 MaxCompute
python test_maxcompute.py
```

---

## 📝 开发规范

### 配置示例

```python
# workers/okr/config.py

# ✅ 正确：使用命名常量
LARK_SOURCE_CONFIG = {
    "app_token": "app_token_value",
    "table_id": "table_id_value",
    "view_id": "view_id_value",
}

# ❌ 错误：硬编码
# table_id = "tblF4i1Ps0aFo8jO"  # 不要这样做
```

### 数据流示例

```python
# workers/okr/main.py

from workers.lib import LarkExtractor, Transformer, Router, LarkLoader

# 1. 提取
extractor = LarkExtractor(config)
data = extractor.extract()

# 2. 转换
transformer = Transformer()
transformed = transformer.transform(data)

# 3. 路由
router = Router(routes)
routed = router.route(transformed)

# 4. 加载
loader = LarkLoader(target_config)
loader.load(routed)
```

---

## 🔍 关键原则

1. **单一职责**: 每个业务域只处理自己的逻辑
2. **配置驱动**: 通过配置控制行为，不硬编码
3. **复用框架**: 优先使用 `workers/lib/` 的组件
4. **关注点分离**: 业务逻辑 vs 基础设施
5. **可扩展性**: 通过配置扩展，不修改框架

---

**开始开发前，请确保理解 workers 模块的职责边界！** 🎯
