# Quickstart: cr_analyze Module

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md)

## Prerequisites

```bash
source .venv/bin/activate
pip install -e .  # 安装项目依赖（含 streamlit, plotly）
```

确保 `automation/conf/lark.ini` 和 `automation/conf/maxcomputer.ini` 配置正确。

## Phase 1 Validation: Data Pipeline

### 1.1 Unit Tests (无需外部依赖)

```bash
# 配置完整性
python -m pytest tests/cr_analyze/test_config.py -v

# 聚合逻辑
python -m pytest tests/cr_analyze/test_transformer.py -v

# SQLite 存储
python -m pytest tests/cr_analyze/test_sqlite_store.py -v
```

**Expected**: All tests pass. Key assertions:
- `test_config.py`: 6 LarkSourceConfig entries, field_names non-empty, SQL file exists
- `test_transformer.py`: stage derivation correct for known date ranges; city_unit merging works; public filters applied
- `test_sqlite_store.py`: write+read roundtrip; table overwrite behavior

### 1.2 Pipeline Smoke Test (需要 Lark/MC credentials)

```bash
# 运行完整管道
python -m workers.cr_analyze.main --date 2026-06-20

# 检查 SQLite 输出
python -c "
import sqlite3
db = sqlite3.connect('workers/cr_analyze/data/cr_analyze.db')
for t in db.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall():
    count = db.execute(f'SELECT COUNT(*) FROM {t[0]}').fetchone()[0]
    print(f'{t[0]}: {count} rows')
"
```

**Expected**: SQLite 包含 6 张 Lark 表 + 1 张事实表 + 1 张宽表，均有数据。

## Phase 2 Validation: Dashboard

### 2.1 Launch Dashboard

```bash
# 先确保管道已运行（SQLite 存在）
python -m workers.cr_analyze.main --date 2026-06-22

# 启动看板
streamlit run workers/cr_analyze/dashboard/app.py
```

**Expected**: 浏览器打开，5 个 Tab + 功效分析 Tab 均可访问。

### 2.2 Tab Validation Checklist

| Tab | Check |
|-----|-------|
| Tab 1 试验总览 | 8 城市分组表显示；当前阶段卡片正确 |
| Tab 2 配置核查 | 抽佣率偏差表有数据；供货价趋势图渲染 |
| Tab 3 归一化进度 | r₀ 折线图有数据；目标线 (7.5%/4.6%) 显示 |
| Tab 4 效应分析 | 4 个子视图可切换；摸底期基准点显示 |
| Tab 5 护栏预警 | 告警状态表有三色标注；阈值参考表显示 |
| 功效分析 | σ/ρ 表格显示；功效结论文字可见 |

### 2.3 Power Analysis

```bash
# 运行功效分析
python -m workers.cr_analyze.main --power

# 检查结果
python -c "
import sqlite3, pandas as pd
db = sqlite3.connect('workers/cr_analyze/data/cr_analyze.db')
df = pd.read_sql('SELECT * FROM power_analysis', db)
print(df.to_string())
"
```

**Expected**: 3 行结果（每 SKU 一行），σ_raw for 10184690 接近 0.194。

## Phase 3 Validation: E2E Tests

```bash
# 运行全部 E2E 测试
python -m pytest tests/cr_analyze/test_pipeline_e2e.py -v

# 运行全部 cr_analyze 测试
python -m pytest tests/cr_analyze/ -v

# 排除集成测试（仅单元测试）
python -m pytest tests/cr_analyze/ -v -m "not integration"
```

**Expected**: All tests pass. E2E tests validate full pipeline with mock data.

## Common Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: workers.cr_analyze` | 未安装项目 | `pip install -e .` |
| SQLite database not found | 未运行管道 | 先运行 `python -m workers.cr_analyze.main` |
| Lark API timeout | 网络或凭据问题 | 检查 `automation/conf/lark.ini` |
| Streamlit tab renders blank | SQLite 表为空 | 检查管道日志，确认数据源有数据 |
