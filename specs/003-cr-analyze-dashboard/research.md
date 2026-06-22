# Research: cr_analyze Module

**Date**: 2026-06-22 | **Feature**: [spec.md](spec.md)

## R-001: Streamlit 图表库选择

**Decision**: 使用 `plotly` 作为主要图表库

**Rationale**:
- Streamlit 对 plotly 有原生支持 (`st.plotly_chart`)，交互性最佳
- plotly 支持 hover、zoom、pan，适合数据探索场景
- 看板需要交互式图表（排序、筛选联动），plotly 比 matplotlib 更适合

**Alternatives considered**:
- `matplotlib`: 更轻量，但交互性差，需要额外代码实现联动
- `altair`: Streamlit 也支持，但生态不如 plotly 丰富
- `streamlit-aggrid`: 用于数据表格展示，可与 plotly 互补

## R-002: SQLite 操作封装方式

**Decision**: 自建轻量封装 (`sqlite_store.py`)，不引入 ORM

**Rationale**:
- 需求简单：写表（覆盖）、读表、列表；不需要 ORM 的关系映射
- `pandas.DataFrame.to_sql()` + `pd.read_sql()` 直接满足需求
- `if_exists="replace"` 天然支持全表覆盖 (FR-009)

**Alternatives considered**:
- `SQLAlchemy`: 过重，本模块不需要关系映射或事务管理
- `dataset` 库：额外依赖，功能与 pandas 内置能力重复

## R-003: 功效分析 CLI 架构

**Decision**: 作为 `main.py` 的 `--power` 标志位，而非独立入口

**Rationale**:
- 功效分析需要先运行管道提取数据到 SQLite，作为管道的一部分更自然
- 复用 main.py 的 client 初始化和 CLI 解析逻辑
- 看板读取 SQLite 中的功效分析结果表，解耦计算和展示

**Alternatives considered**:
- 独立 `power.py` 入口：增加入口数量，但逻辑高度复用 main.py
- Jupyter notebook：不适合自动化和 CI 集成

## R-004: 聚合宽表计算位置

**Decision**: 在 pandas 中本地计算（Phase 1 transformer.py）

**Rationale**:
- SQL 已返回明细级数据，聚合逻辑涉及多表 join（conf_试验分组配置 → city_unit、conf_试验周期抽佣率 → stage）
- pandas 中更易调试和验证中间步骤
- 数据量小（< 10 万行），pandas 性能充裕

**Alternatives considered**:
- 在 MaxCompute SQL 中完成聚合：需将配置表上传到 MC，增加复杂度
- 分步：SQL 做基础聚合，pandas 做 stage_week — 增加 SQL 维护成本

## R-005: Dashboard 状态管理

**Decision**: 使用 `st.session_state` 管理筛选器状态 + `@st.cache_data` 缓存 SQLite 读取

**Rationale**:
- Streamlit 的标准状态管理模式
- `@st.cache_data` 避免每次交互都重新读取 SQLite
- 筛选器值存 session_state，各 tab 共享

**Alternatives considered**:
- 外部状态管理 (如 Streamlit 的 `query_params`)：适合分享 URL 场景，但本模块是内部使用
- 每次重新读取：性能浪费，尤其是大表

## R-006: 测试 Mock 策略

**Decision**: Mock Lark/MC client 层，使用预构造 DataFrame fixtures

**Rationale**:
- 项目已有 `tests/conftest.py` 提供 `client` fixture 和 `@pytest.mark.integration`
- E2E 测试 mock 提取层，验证从 DataFrame → 聚合 → SQLite 的全链路
- 单元测试直接测试 transformer 纯函数，不需要 mock

**Alternatives considered**:
- Mock HTTP 层：太底层，维护成本高
- 使用真实 API (integration test)：仅用于手动验证，不纳入自动测试

## R-007: Streamlit 数据表格组件

**Decision**: 使用 `st.dataframe` (内置) + `streamlit-aggrid` (高级交互)

**Rationale**:
- `st.dataframe` 支持排序和搜索，适合配置表浏览 (Tab 2 H-2)
- `streamlit-aggrid` 提供更丰富的列配置、条件格式（红色/黄色告警）
- 告警状态表 (Tab 5) 需要条件着色，aggrid 支持 cellStyle

**Alternatives considered**:
- 仅用 `st.dataframe`：不支持条件格式
- `st.table`：静态，不支持排序和搜索

**依赖说明**: `streamlit-aggrid` 需要在 `pyproject.toml` 的 dependencies 中添加。同时需添加 `streamlit` 和 `plotly`。

## R-008: Wiki-hosted Lark Base 兼容性

**Decision**: LarkExtractor 已支持 wiki URL 格式，无需额外适配

**Rationale**:
- `LarkExtractor.extract_single_source()` 解析 URL 时支持 wiki 格式（`/wiki/{token}?table={table_id}&view={view_id}`）
- cr_trail_pricing 模块已成功使用相同机制读取 wiki-hosted 配置表
- `LarkSourceConfig.url` 直接使用 wiki URL 即可，框架自动提取 app_token 和 table_id

**Alternatives considered**:
- 将 wiki URL 手动转换为 app_token + table_id：增加配置复杂度，且容易出错
- 修改 LarkExtractor 添加 wiki 专用解析器：不必要的改动，现有解析器已覆盖

**验证方式**: Phase 1 Task 1.1 的 test_config.py 中包含 URL 解析验证测试
