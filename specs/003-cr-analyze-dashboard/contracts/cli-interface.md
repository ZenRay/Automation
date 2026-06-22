# CLI Contract: cr_analyze Module

**Feature**: [spec.md](spec.md) | **Data Model**: [data-model.md](data-model.md)

## 1. Data Pipeline Command

```
python -m workers.cr_analyze.main [--date DATE] [--db-path PATH]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--date` | str (YYYY-MM-DD) | today | 目标日期；用于 conf_区县信息 日期过滤和试验阶段推导 |
| `--db-path` | str (file path) | `workers/cr_analyze/data/cr_analyze.db` | SQLite 输出文件路径 |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | 管道成功完成 |
| 1 | 管道失败（Lark API 错误、MC 查询错误、或未知异常） |

### stdout Output

管道运行时向 stdout 输出每个阶段的日志：

```
[INFO] Extracting 6 Lark sources...
[INFO]   conf_product_info: 45 rows
[INFO]   conf_county_info: 5200 rows
[INFO]   conf_commission_adjustment: 1200 rows
[INFO]   conf_trial_region_price: 900 rows
[INFO]   conf_trial_group: 16 rows
[INFO]   conf_trial_period_rate: 24 rows
[INFO] Executing MaxCompute queries...
[INFO]   fact_order_item: 85000 rows
[INFO] Computing aggregation wide table...
[INFO]   agg_wide_table: 2400 rows
[INFO] Writing to SQLite: workers/cr_analyze/data/cr_analyze.db
[INFO] Pipeline complete. 8 tables written.
```

### SQLite Output

写入 8 张表到 SQLite 文件（`--power` 模式下额外写入 power_analysis，共 9 张）：

| Table Name | Source |
|------------|--------|
| conf_product_info | Lark conf_商品信息 |
| conf_county_info | Lark conf_区县信息 |
| conf_commission_adjustment | Lark conf_线上商品区域抽佣率调整 |
| conf_trial_region_price | Lark conf_线上商品试验区域价格 |
| conf_trial_group | Lark conf_试验分组配置 |
| conf_trial_period_rate | Lark conf_试验周期抽佣率 |
| fact_order_item | MaxCompute order_fact_whole.sql |
| agg_wide_table | Computed (see data-model.md §3) |
| power_analysis | Computed (see data-model.md §4, only with --power) |

## 2. Power Analysis Command

```
python -m workers.cr_analyze.main --power [--db-path PATH]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--power` | flag | - | 启用功效分析模式 |
| `--db-path` | str (file path) | `workers/cr_analyze/data/cr_analyze.db` | SQLite 路径（读取事实数据 + 写入结果） |

### Behavior

1. 从 SQLite 读取 `fact_order_item` 表
2. 按历史基线范围（config.py 中配置）筛选数据
3. 计算每 SKU 的 σ_raw, σ_adjusted, ρ_pre, ρ_post, ρ_main
4. 计算功效验证 n_required
5. 计算 SKU 间相关性
6. 将结果写入 `power_analysis` 表
7. 向 stdout 输出结果摘要

### stdout Output (示例)

```
[INFO] Power analysis mode
[INFO] Historical baseline: 2026-04-13 ~ 04-26, 2026-05-11 ~ 05-24
[INFO] Computing σ per SKU...
[INFO]   SKU 10184690: σ_raw=0.194, σ_adjusted=0.290
[INFO]   SKU 20519020: σ_raw=0.215, σ_adjusted=0.322
[INFO]   SKU 20588413: σ_raw=0.188, σ_adjusted=0.282
[INFO] Computing ρ per SKU...
[INFO]   SKU 10184690: ρ_pre=0.995, ρ_post=0.991, ρ_main=0.991
[INFO] Power verification:
[INFO]   SKU 10184690: n_required=1.2, n_actual=2 → 功效充足 ✅
[INFO]   SKU 20519020: n_required=1.8, n_actual=2 → 功效充足 ✅
[INFO]   SKU 20588413: n_required=1.1, n_actual=2 → 功效充足 ✅
[INFO] Results written to power_analysis table
```

## 3. Streamlit Dashboard

```
streamlit run workers/cr_analyze/dashboard/app.py [-- --db-path PATH]
```

### Arguments (passed via Streamlit)

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--db-path` | str | `workers/cr_analyze/data/cr_analyze.db` | SQLite 数据源路径 |

### Tabs

| # | Title | Data Source |
|---|-------|-------------|
| 1 | 试验总览 | conf_trial_group, conf_trial_period_rate |
| 2 | 配置核查 | conf_commission_adjustment, conf_trial_period_rate, conf_product_info |
| 3 | 归一化进度 | agg_wide_table (归一化预备期 granularity) |
| 4 | 效应分析 | agg_wide_table (摸底期 + 生效期) |
| 5 | 护栏预警 | agg_wide_table (生效期 stage_week) |
| 6 | 功效分析 | power_analysis |

### Error States

| Condition | Dashboard Behavior |
|-----------|-------------------|
| SQLite file missing | 全页错误提示："请先运行数据管道: `python -m workers.cr_analyze.main`" |
| Table empty | 对应 Tab 显示 "暂无数据" |
| power_analysis table missing | Tab 6 显示 "请先运行功效分析: `python -m workers.cr_analyze.main --power`" |
