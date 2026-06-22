# Data Model: cr_analyze Module

**Date**: 2026-06-22 | **Feature**: [spec.md](spec.md)

> **数据来源**: 字段清单基于 2026-06-22 从飞书 API 实际拉取验证（`tests/verify_lark_schema.py`）

## 1. Lark Configuration Tables (SQLite)

### 1.1 conf_product_info (conf_商品信息)

**Table ID**: `tblevDYqsTdwu8fo` | **实际字段数**: 26

| Field | Type ID | Lark Type | Nullable | Description |
|-------|---------|-----------|----------|-------------|
| 日期 | 5 | 日期 | NO | 配置日期 |
| 商城id | 2 | 数字 | YES | |
| 商品id | 2 | 数字 | NO | SKU 标识 |
| 商品编码 | 1 | 文本 | YES | |
| 商品名称 | 1 | 文本 | YES | SKU 名称 |
| 商品等级 | 1 | 文本 | YES | A级/B级 等 |
| 产地 | 1 | 文本 | YES | 云南 / 广西 / 海南 |
| 包装类型 | 1 | 文本 | YES | 泡沫箱/纸箱 |
| 单果大小 | 1 | 文本 | YES | 中果/大果 |
| 色号 | 1 | 文本 | YES | 映射为 sku_grade (如 5号色) |
| 商品头数 | 1 | 文本 | YES | |
| 商家id | 2 | 数字 | YES | |
| 商家名称 | 1 | 文本 | YES | |
| 商家类型 | 1 | 文本 | YES | |
| 后台类目id | 2 | 数字 | YES | |
| 后台类目名称 | 1 | 文本 | YES | 水仙芒 |
| 净重 | 2 | 数字 | YES | |
| 毛重 | 2 | 数字 | YES | |
| 非试验区域平台销售斤单价 | 2 | 数字 | YES | |
| 非试验区域平台销售件单价 | 2 | 数字 | YES | |
| 非试验区域抽佣率 | 2 | 数字 | YES | 小数格式 (如 0.102) |
| 非试验区域商家供货斤单价 | 2 | 数字 | YES | |
| 非试验区域商家供货件单价 | 2 | 数字 | YES | |
| 是否当日上架 | 2 | 数字 | YES | 0/1 |
| 是否试验周期 | 2 | 数字 | YES | 0/1 |
| 是否试验商品 | 2 | 数字 | YES | 0/1 |

**Date filter**: None (extract all)

### 1.2 conf_county_info (conf_区县信息)

**Table ID**: `tblBgJYpBRT18Uvp` | **实际字段数**: 22

| Field | Type ID | Lark Type | Nullable | Description |
|-------|---------|-----------|----------|-------------|
| 日期 | 5 | 日期 | NO | |
| 试验区域id | 2 | 数字 | YES | |
| 试验区域名称 | 1 | 文本 | YES | |
| 修正区域名称 | 1 | 文本 | YES | |
| 区域别名 | 1 | 文本 | YES | |
| 地址id | 2 | 数字 | YES | |
| 地址名称 | 1 | 文本 | YES | |
| 街道id | 2 | 数字 | YES | |
| 街道名称 | 1 | 文本 | YES | |
| 区县id | 2 | 数字 | NO | |
| 区县名称 | 1 | 文本 | YES | |
| 市id | 2 | 数字 | YES | |
| 市名称 | 1 | 文本 | YES | |
| 省id | 2 | 数字 | YES | |
| 省名称 | 1 | 文本 | YES | |
| 经度 | 1 | 文本 | YES | |
| 纬度 | 1 | 文本 | YES | |
| 父级区域id | 2 | 数字 | YES | |
| 是否有效 | 2 | 数字 | YES | 0/1 |
| 区域等级 | 2 | 数字 | YES | |
| 试验区域类型 | 1 | 文本 | YES | COUNTY/CITY |
| 运营类型 | 1 | 文本 | YES | 自营区域 / 代理人区域 |

**Date filter**: 日期 = target_date

### 1.3 conf_commission_adjustment (conf_线上商品区域抽佣率调整)

**Table ID**: `tbl1Wa88og2jX26R` | **实际字段数**: 14

| Field | Type ID | Lark Type | Nullable | Description |
|-------|---------|-----------|----------|-------------|
| 日期 | 5 | 日期 | NO | |
| 商城id | 2 | 数字 | YES | |
| 商品id | 2 | 数字 | NO | |
| 后台类目id | 2 | 数字 | YES | |
| 区县id | 2 | 数字 | YES | |
| 区县名称 | 1 | 文本 | YES | |
| 区域全称 | 1 | 文本 | YES | 省-市-区 全称 |
| 调整方向 | 1 | 文本 | YES | 涨价/降价/不变 |
| 调整系数 | 2 | 数字 | YES | |
| 调整幅度 | 2 | 数字 | YES | |
| 固定抽佣率调整 | 2 | 数字 | YES | |
| 固定抽佣金额调整 | 2 | 数字 | YES | |
| 快照版本 | 1 | 文本 | YES | |
| 参与试验类型 | 20 | **公式** | YES | 返回 `[试验区域]` 或 `[非试验区域]`（带方括号） |

**Date filter**: 日期 >= 2026-06-19

> **注意**: `参与试验类型` 是公式字段 (type=20)，返回值带方括号。过滤时需使用 `"非试验区域" not in value` 而非精确匹配。

### 1.4 conf_trial_region_price (conf_线上商品试验区域价格)

**Table ID**: `tbl4nwTsRUZSubLF` | **实际字段数**: 16

| Field | Type ID | Lark Type | Nullable | Description |
|-------|---------|-----------|----------|-------------|
| 日期 | 5 | 日期 | NO | |
| 商城id | 2 | 数字 | YES | |
| 商品id | 2 | 数字 | NO | |
| 商品名称 | 1 | 文本 | YES | |
| 商家id | 2 | 数字 | YES | |
| 商家名称 | 1 | 文本 | YES | |
| 后台类目id | 2 | 数字 | YES | |
| 后台类目名称 | 1 | 文本 | YES | |
| 区域id | 2 | 数字 | YES | |
| 区域名称 | 1 | 文本 | YES | |
| 区域全称 | 1 | 文本 | YES | |
| 试验区域平台销售斤单价 | 2 | 数字 | YES | |
| 试验区域平台销售件单价 | 2 | 数字 | YES | |
| 试验区域商家供货斤单价 | 2 | 数字 | YES | |
| 试验区域商家供货件单价 | 2 | 数字 | YES | |
| 抽佣率 | 2 | 数字 | YES | 可为 None |

**Date filter**: 日期 >= 2026-06-19

### 1.5 conf_trial_group (conf_试验分组配置)

**Table ID**: `tbl2hCVkpjtMt16J` | **实际字段数**: 7

| Field | Type ID | Lark Type | Nullable | Description |
|-------|---------|-----------|----------|-------------|
| 区域id | 2 | 数字 | NO | city_id 或 county_id |
| 区域名称 | 1 | 文本 | YES | 如 "邵阳市", "萍乡市2" |
| 市名称 | 1 | 文本 | YES | 用于 city_unit 归并 (如 "萍乡市") |
| 区域类型 | 1 | 文本 | YES | CITY / COUNTY |
| 试验分组 | 3 | **单选** | YES | 对照组 / 试验组一 / 试验组二 / 试验组三 |
| 试验起始日期 | 5 | 日期 | YES | |
| 试验结束日期 | 5 | 日期 | YES | |

**Date filter**: None

> **注意**: `试验分组` 是单选字段 (type=3)，值为中文（对照组/试验组一/试验组二/试验组三），非 G0~G3 编码。`市名称` 可直接用于 city_unit 归并。

### 1.6 conf_trial_period_rate (conf_试验周期抽佣率)

**Table ID**: `tblXeBNiHArKWmXm` | **实际字段数**: 7

| Field | Type ID | Lark Type | Nullable | Description |
|-------|---------|-----------|----------|-------------|
| 试验阶段 | 1 | 文本 | NO | 归一化预备期 / 摸底期 / 生效期 |
| 运营类型 | 3 | **单选** | YES | 自营区域 / 代理人区域 |
| 抽佣率 | 2 | 数字 | YES | 目标 r₀（小数，如 0.075） |
| 试验分组 | 3 | **单选** | YES | 对照组 / 试验组一 / 试验组二 / 试验组三 |
| 试验起始日期 | 5 | 日期 | YES | |
| 试验结束日期 | 5 | 日期 | YES | |
| 备注 | 1 | 文本 | YES | 计算说明 |

**Date filter**: None

## 2. MaxCompute Fact Table (SQLite)

### 2.1 fact_order_item (from order_fact_whole.sql)

| Field | Type | Description | Wide Table Mapping |
|-------|------|-------------|-------------------|
| 日期 | date | 交易日期 | → stage derivation |
| 订单id | str | 订单标识 | |
| 明细订单id | str | 明细行标识 | → COUNT DISTINCT → order_count |
| 商品id | str | SKU 标识 | → sku_id |
| 商品名称 | str | | |
| 后台类目名称 | str | 水仙芒 | |
| 商家名称 | str | | |
| 商家类型 | str | | |
| 结算类型 | str | | |
| 净重 | float | | |
| 毛重 | float | | |
| 实际抽佣率 | float | 已归一化为小数 | |
| 商家供货件单价 | float | SQL 已计算 | |
| 商家供货斤单价 | float | SQL 已计算 | → AVG → supply_price |
| 活动价格 | float | | |
| 平台销售件单价 | float | | → price analysis |
| 平台销售斤单价 | float | | → price analysis |
| 平台服务费单价 | float | | |
| 店铺id | str | | → COUNT DISTINCT → active_store_count |
| 省id | str | | |
| 省名称 | str | | |
| 市id | str | | → city_unit merge |
| 市名称 | str | | → city_unit merge |
| 区县id | str | | → city_unit merge |
| 区县名称 | str | | → city_unit merge |
| 网格id | str | | |
| 网格名称 | str | | |
| 下单数量 | int | | → stockout_num calc |
| 下单金额 | float | | |
| 下单重量 | float | | |
| 送达金额 | float | | → SUM → gmv |
| 送达数量 | int | | → stockout_num calc |
| 送达重量 | float | | |
| 送达运费 | float | | |
| 送达抽佣金额 | float | | → SUM → commission_amount |
| 是否有效订单 | int | 0/1 | → public filter (=1) |

## 3. Aggregation Wide Table (SQLite)

### 3.1 agg_wide_table

**Granularity varies by stage**:
- 归一化预备期: stage × 日期 × city_unit × region_type
- 摸底期: stage × city_unit × sku_id (整体聚合)
- 生效期: stage_week × city_unit × sku_id

| Field | Type | Computation |
|-------|------|-------------|
| stage | str | Derived from conf_trial_period_rate date ranges |
| stage_week | str/null | 生效期_W{N} or NULL |
| is_complete_week | bool/null | 7-day coverage check |
| trading_days | int | COUNT DISTINCT 日期 within period |
| city_unit | str | Merged city name via conf_trial_group.市名称 |
| region_type | str | 自营区域 / 代理人区域 |
| trial_group | str | 对照组 / 试验组一 / 试验组二 / 试验组三 |
| sku_id | str | 商品id |
| sku_origin | str | from conf_product_info.产地 |
| sku_grade | str | from conf_product_info.色号 |
| sku_weight_spec | str | from conf_product_info.包装类型 |
| order_count | int | COUNT DISTINCT 明细订单id |
| active_store_count | int | COUNT DISTINCT 店铺id (cross-SKU dedup) |
| gmv | float | SUM(送达金额) |
| commission_amount | float | SUM(送达抽佣金额) |
| stockout_num | int | SUM(MAX(下单数量-送达数量, 0)) |
| commission_rate | float | commission_amount / gmv |
| supply_price | float | AVG(商家供货斤单价) from SQL output |
| target_r0 | float | from conf_trial_period_rate |

## 4. Power Analysis Result (SQLite)

### 4.1 power_analysis

| Field | Type | Description |
|-------|------|-------------|
| sku_id | str | SKU 标识 |
| sigma_raw | float | 8 城市 CV 均值 |
| sigma_adjusted | float | sigma_raw × 1.5 |
| rho_pre | float | Pearson(W1, W2) |
| rho_post | float | Pearson(W3, W4) |
| rho_main | float | min(rho_pre, rho_post) |
| n_required | float | Power formula result |
| n_actual | int | 2 (fixed) |
| power_sufficient | bool | n_required <= n_actual |
| sku_cross_corr_json | str | JSON: 3 pair correlations |

## 5. Entity Relationships

```
conf_trial_group ──┐  (city_unit via 市名称, trial_group, region_type)
                   ├──→ agg_wide_table ←── fact_order_item
conf_trial_period_rate ──┘  (stage, target_r0)  ↑
                                                 │
conf_product_info ───────────────────────────────┘
      (sku_origin, sku_grade, sku_weight_spec)

conf_commission_adjustment ──→ Tab 2 H-1 (config audit, 参与试验类型 filter)
conf_trial_region_price ──→ Tab 2 H-2 (price comparison)
conf_county_info ──→ county dimension reference
```

## 6. Public Filters (applied during aggregation)

| Filter | Source | Applied At |
|--------|--------|-----------|
| 是否有效订单 = 1 | fact_order_item | Pandas filter during aggregation (SQL outputs column but does not filter) |
| 商品id IN (10184690, 20519020, 20588413) | config.py | Pandas filter during aggregation (SQL filters by sku_name REGEXP, not by ID) |
| 参与试验类型 contains "试验区域" (not "非试验区域") | conf_commission_adjustment (formula field, type=20) | Pandas join during aggregation; value format is `[试验区域]` with brackets |
