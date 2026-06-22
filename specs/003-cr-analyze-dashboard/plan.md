# Implementation Plan: cr_analyze — Commission Rate Trial Analysis Dashboard

**Branch**: `workers` | **Date**: 2026-06-22 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/003-cr-analyze-dashboard/spec.md`

## Summary

为 `workers/cr_analyze` 模块构建完整的数据管道 + Streamlit 看板。管道从 6 张飞书配置表和 1 个 MaxCompute SQL 查询提取数据到 SQLite，本地计算核心聚合宽表，并提供功效分析 CLI。Streamlit 看板包含 5 个 Tab（试验总览、配置核查、归一化进度、效应分析、护栏预警）+ 功效分析页面。分 3 个 Phase 交付，每个 Phase 包含单元测试；Phase 3 专注 E2E 测试。

## Technical Context

**Language/Version**: Python 3.12

**Primary Dependencies**: pandas, numpy, pyodps, requests, streamlit, matplotlib/plotly

**Storage**: SQLite (本地数据库), MaxCompute (远程 SQL), Lark API (飞书多维表格)

**Testing**: pytest >= 7.0 (class-style + fixtures + `@pytest.mark.integration`)

**Target Platform**: Linux server (本地运行 + Streamlit 看板)

**Project Type**: CLI + Web Dashboard

**Performance Goals**: 管道 5 分钟内完成；看板 5 秒内加载首页

**Constraints**: 单用户单进程运行；SQLite 不支持并发写入

**Scale/Scope**: 8 城市 × 3 SKU × 数周数据；典型数据量 < 10 万行

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Layer Isolation | ✅ PASS | 使用 workers/lib 框架层，不修改 automation/ |
| II. Configuration-Driven | ✅ PASS | 所有业务参数集中在 config.py |
| III. Date Type Unification | ✅ PASS | 提取阶段统一 datetime.date |
| IV. Data Pipeline Integrity | ⚠️ DEVIATION | 输出到 SQLite 而非飞书；已在 spec Assumptions 中声明 |
| V. Credential Security | ✅ PASS | 使用 automation.conf 读取凭据 |
| VI. Containerization Standards | ✅ N/A | 本阶段不涉及容器化 |
| VII. CI/CD Pipeline | ✅ N/A | 本阶段不涉及 CI/CD |

**Deviation Justification (Principle IV)**: 本模块是监控分析工具而非数据产出 ETL。"Load" 阶段写入 SQLite 而非飞书目标表，Streamlit 看板直接消费 SQLite。这是有意的设计决策。

## Project Structure

### Documentation (this feature)

```text
specs/003-cr-analyze-dashboard/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   └── cli-interface.md # CLI contract
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
workers/cr_analyze/
├── __init__.py              # re-export run_cr_analyze_pipeline
├── config.py                # 业务配置唯一注入点
│   ├── LARK_SOURCES         # 6 张 LarkSourceConfig
│   ├── SQL_QUERIES          # SQLQueryConfig 列表
│   ├── SQL_BASE_DIR         # Path(__file__).parent / "sql"
│   ├── FIELD_MAPPING        # SQL 输出列 → 宽表字段映射
│   ├── TRIAL_PHASE_CONFIG   # 预备期参数、端午节日期、历史基线范围
│   └── ALERT_THRESHOLDS     # 护栏预警阈值配置
├── main.py                  # 管道编排 + CLI 入口
├── transformer.py           # 聚合宽表计算 + 功效分析逻辑
├── sqlite_store.py          # SQLite 读写封装
├── sql/
│   └── order_fact_whole.sql # 已有，交易事实表查询
└── data/                    # 默认 SQLite 输出目录
    └── .gitkeep

workers/cr_analyze/dashboard/
├── __init__.py
├── app.py                   # Streamlit 入口 (streamlit run 此文件)
├── tab1_overview.py         # 试验总览
├── tab2_config_audit.py     # 配置核查 (H-1 + H-2)
├── tab3_normalization.py    # 归一化进度
├── tab4_effect.py           # 效应分析 (B/C/D/E 子视图)
├── tab5_guardrail.py        # 护栏预警
├── tab_power.py             # 功效分析页面
└── components.py            # 共享 UI 组件 (筛选器、图表辅助)

tests/cr_analyze/
├── __init__.py
├── test_config.py           # 配置完整性单元测试
├── test_transformer.py      # 聚合逻辑单元测试
├── test_sqlite_store.py     # SQLite 存储单元测试
├── test_power_analysis.py   # 功效分析单元测试
├── test_pipeline_e2e.py     # 端到端管道测试 (mock 数据)
├── test_dashboard_e2e.py    # 看板冒烟 E2E 测试
└── conftest.py              # 共享 fixtures (sample DataFrames)
```

**Structure Decision**: 遵循项目 worker 模式（config.py + main.py + transformer.py），新增 `sqlite_store.py` 封装 SQLite 操作，`dashboard/` 子包隔离 Streamlit 代码。测试放 `tests/cr_analyze/` 子包，与 `tests/cr_trail_pricing/` 保持一致。

## Implementation Phases

### Phase 1: Data Pipeline Core + Unit Tests

**目标**: 完成数据提取 → SQLite 存储 → 聚合宽表计算，附带单元测试

#### Task 1.1: 依赖 + 配置 + 存储层

**交付物**:
- `pyproject.toml` — 添加 `streamlit`, `plotly`, `streamlit-aggrid` 到 dependencies
- `workers/cr_analyze/__init__.py`
- `workers/cr_analyze/config.py` — 6 个 LarkSourceConfig + SQLQueryConfig + 字段映射 + 试验配置
- `workers/cr_analyze/sqlite_store.py` — `write_tables(db_path, data_dict)` / `read_table(db_path, table_name)` 封装
- `workers/cr_analyze/data/.gitkeep`
- `tests/cr_analyze/__init__.py`
- `tests/cr_analyze/conftest.py` — 共享 sample DataFrames fixtures
- `tests/cr_analyze/test_config.py` — 配置完整性验证
- `tests/cr_analyze/test_sqlite_store.py` — SQLite 读写测试

#### Task 1.2: 编排 + 聚合逻辑

**交付物**:
- `workers/cr_analyze/main.py` — 管道编排 (初始化 Lark + MC 客户端 → 提取 → 聚合 → 存 SQLite) + CLI (`--date`, `--db-path`, `--power`)
- `workers/cr_analyze/transformer.py` — 聚合宽表计算 (stage/stage_week/city_unit/trial_group 关联)
- `tests/cr_analyze/test_transformer.py` — 聚合逻辑测试 (stage 推导、city_unit 归并、公共过滤)

**关键设计决策**:
1. **客户端初始化**: main.py 同时初始化 `LarkMultiDimTable` (飞书) 和 `MaxComputerClient` (MaxCompute)，参考 `okr/main.py` 的 `_init_lark_client()` / `_init_mc_client()` 模式
2. `sqlite_store.py` 使用 `if_exists="replace"` 实现全表覆盖 (FR-009)
3. 聚合逻辑在 `transformer.py` 中以纯函数实现，接收 lark_data dict + mc_data dict，返回宽表 DataFrame
4. CLI 复用 `argparse` + `date.fromisoformat` 模式（参考 okr/main.py）；`--power` 为 `store_true` 标志位，非子命令
5. config.py 中 `TRIAL_PHASE_CONFIG` dict 管理端午节日期 `[date(2026,6,19), date(2026,6,20), date(2026,6,21)]` 和历史基线范围

### Phase 2: Streamlit Dashboard + Power Analysis

**目标**: 完成 5 Tab 看板 + 功效分析 CLI + 功效分析页面

**交付物**:
- `workers/cr_analyze/dashboard/app.py` — Streamlit 主入口 (st.tabs 布局)
- `workers/cr_analyze/dashboard/tab1_overview.py` — 试验总览 (视图 G)
- `workers/cr_analyze/dashboard/tab2_config_audit.py` — 配置核查 (H-1 + H-2)
- `workers/cr_analyze/dashboard/tab3_normalization.py` — 归一化进度 (视图 A)
- `workers/cr_analyze/dashboard/tab4_effect.py` — 效应分析 (视图 B/C/D/E)
- `workers/cr_analyze/dashboard/tab5_guardrail.py` — 护栏预警 (视图 F)
- `workers/cr_analyze/dashboard/tab_power.py` — 功效分析展示页
- `workers/cr_analyze/dashboard/components.py` — 共享筛选器、图表辅助组件
- `workers/cr_analyze/transformer.py` (扩展) — 添加 `compute_power_analysis()` 函数
- `workers/cr_analyze/main.py` (扩展) — 添加 `--power` 标志位处理逻辑
- `tests/cr_analyze/test_power_analysis.py` — σ/ρ 计算和功效验证单元测试

**关键设计决策**:
1. Streamlit 使用 `st.tabs` 实现 5+1 Tab 布局 (Tab 6 = 功效分析)
2. 图表库选择: `plotly` (交互式，Streamlit 原生支持) 或 `matplotlib` (静态，更轻量) — 优先 plotly
3. 功效分析 CLI 作为 `main.py` 的 `--power` 标志位 (`store_true`): `python -m workers.cr_analyze.main --power`
4. 功效分析结果写入 SQLite `power_analysis` 表，看板读取展示
5. 每个 tab 模块暴露 `render(db_path)` 函数，app.py 统一调用

### Phase 3: E2E Testing + Integration Validation

**目标**: 端到端验证完整管道 + 看板，覆盖边界场景

**交付物**:
- `tests/cr_analyze/test_pipeline_e2e.py` — 全管道 E2E 测试 (mock Lark/MC client → 真实 SQLite → 验证宽表)
- `tests/cr_analyze/test_dashboard_e2e.py` — 看板冒烟测试 (验证各 tab render 不报错)
- 边界场景覆盖: 空数据、残缺周、端午节排除、阶段未配置

**E2E 测试策略**:
1. **Mock 层**: 使用 `unittest.mock.patch` 替换 `extract_all_lark_sources` 和 `execute_all_queries`，注入预构造的 sample DataFrames
2. **验证链**: mock 提取 → 真实聚合 → 真实 SQLite 写入 → 读取验证行数/字段/聚合正确性
3. **看板测试**: 使用 Streamlit 的 `AppTest` (如可用) 或简单的 import-and-call 验证各 tab render 无异常
4. **标记**: E2E 测试使用 `@pytest.mark.integration` 便于选择性运行

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Principle IV: 输出到 SQLite 而非飞书 | 本模块是分析看板，不需要写回飞书 | 飞书写回无业务价值，增加不必要的复杂度 |
