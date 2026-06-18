# Data Model: Trial Region Commission Pricing Generator

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)
**Created**: 2026-06-18

## Source Entities (Lark Tables)

### conf_区县信息 (County Info)

Filtered to `日期 == target_date` at extraction.

| Field | Type | Description |
| ----- | ---- | ----------- |
| 试验区域id | number | Trial region identifier |
| 试验区域名称 | text | Trial region name |
| city_id | number | City identifier (used for join with trial group config) |
| 市id | number | City ID (administrative) |
| 省id | number | Province ID |
| 区县id | number | County/district ID |
| 区县名称 | text | County/district name |

### conf_商品信息 (Product Info)

Filtered to `日期 == target_date - 1` (previous day) at extraction.

| Field | Type | Description |
| ----- | ---- | ----------- |
| 日期 | date | Record date (filtered to yesterday) |
| 商品id | number | Product identifier (join key) |
| 商品编码 | text | Product SKU code |
| 商品名称 | text | Product name |
| 后台类目名称 | text | Backend category name |
| 非试验区域抽佣率 | number | Non-trial commission rate (NOT used; overridden by conf_试验商品信息) |
| 毛重 | number | Gross weight |

### conf_试验分组配置 (Trial Group Config)

Filtered in pandas: `区域类型 == "CITY"` AND `target_date` within `[试验起始日期, 试验结束日期]`.

| Field | Type | Description |
| ----- | ---- | ----------- |
| 区域id | number | Region ID (matches 市id in county info) |
| 区域名称 | text | Region name |
| 区域类型 | text | Area type — only "CITY" records are used |
| 试验分组 | single_select | Trial group identifier (e.g., 对照组, 试验组一/二/三) |
| 试验起始日期 | date | Trial start date |
| 试验结束日期 | date | Trial end date |

### conf_试验周期抽佣率 (Trial Period Commission Rate)

Filtered in pandas: `target_date` within trial date range.

| Field | Type | Description |
| ----- | ---- | ----------- |
| 试验阶段 | text | Trial phase (e.g., 摸底期) |
| 运营类型 | single_select | Operation type: "自营区域" or "代理人区域" (join key with region data) |
| 试验分组 | single_select | Trial group (join key with trial group config) |
| 抽佣率 | number | Trial commission rate (decimal) |
| 试验起始日期 | date | Rate effective start date |
| 试验结束日期 | date | Rate effective end date |
| 备注 | text | Remarks (not used) |

### conf_试验商品信息 (Trial Product Info)

Filtered to `日期 == target_date` at extraction.

| Field | Type | Description |
| ----- | ---- | ----------- |
| 商品id | number | Product identifier (join key with conf_商品信息) |
| 日期 | date | Record date (filtered to today) |
| 非试验区域抽佣率 | number | Non-trial commission rate — authoritative value used in pricing calculations |

### conf_线上隐形物流费 (Online Hidden Logistics Fee)

Filtered to `日期 == target_date` at extraction.

| Field | Type | Description |
| ----- | ---- | ----------- |
| 日期 | date | Record date |
| 市id | number | City ID (join key with region data) |
| 费率 | number | City-level fee rate |
| 区县费率映射 | text | JSON object: `{"区县id": rate, ...}` for county overrides |

## Pipeline Intermediate Schemas

### Stage 2 Output: Filtered Products

| Field | Type | Source |
| ----- | ---- | ------ |
| 商品id | number | conf_商品信息 (JOIN key) |
| 商品编码 | text | conf_商品信息 |
| 商品名称 | text | conf_商品信息 |
| 后台类目名称 | text | conf_商品信息 |
| 非试验区域抽佣率 | number | conf_试验商品信息 (NOT conf_商品信息) |
| 毛重 | number | conf_商品信息 |
| 是否试验区域 | number | Derived (always 1) |

### Stage 3 Output: Marked Regions

All fields from conf_区县信息 plus:

| Field | Type | Source |
| ----- | ---- | ------ |
| 是否试验区域 | number | Derived (1 = matched trial group, 0 = not) |
| 试验分组 | single_select | conf_试验分组配置 (empty string for non-trial) |
| 运营类型 | text | conf_区县信息 (carried through for Stage 4 join) |

### Stage 4 Output: Regions with Commission

| Field | Type | Source |
| ----- | ---- | ------ |
| 日期 | date | target_date |
| 试验区域id | number | Stage 3 |
| 试验区域名称 | text | Stage 3 |
| 市id | number | Stage 3 (from conf_区县信息) |
| 省id | number | Stage 3 (from conf_区县信息) |
| 区县id | number | Stage 3 (from conf_区县信息) |
| 是否试验区域 | number | Stage 3 |
| 试验分组 | single_select | Stage 3 |
| 运营类型 | text | Stage 3 |
| 抽佣率 | number | conf_试验周期抽佣率 (0 for non-trial) |

### Stage 5 Output: Regions with Logistics Fee

All fields from Stage 4 plus:

| Field | Type | Source |
| ----- | ---- | ------ |
| 隐形物流费率 | number | Derived from conf_线上隐形物流费 |

### Stage 6 Output: Pricing Plan (Cartesian Product)

| Field | Type | Source |
| ----- | ---- | ------ |
| 日期 | date | Stage 5 |
| 试验区域id | number | Stage 5 |
| 试验区域名称 | text | Stage 5 |
| 市id | number | Stage 5 |
| 省id | number | Stage 5 |
| 区县id | number | Stage 5 |
| 是否试验区域 | number | Stage 5 |
| 抽佣率 | number | Stage 5 |
| 隐形物流费率 | number | Stage 5 |
| 商品id | number | Stage 2 |
| 商品编码 | text | Stage 2 |
| 商品名称 | text | Stage 2 |
| 后台类目名称 | text | Stage 2 |
| 非试验区域抽佣率 | number | Stage 2 (from conf_试验商品信息) |
| 毛重 | number | Stage 2 (from conf_商品信息) |
| 固定抽佣比例 | number | Computed: abs(非试验区域抽佣率 - 抽佣率) for trial; NaN for non-trial |
| 固定抽佣货值 | number | Computed: 隐形物流费率 × 毛重 for non-trial; NaN for trial |
| 调价方向 | text | Computed: "涨价" / "降价" / "不变" |
| 调价幅度 | NaN | Always empty |
| 设置状态 | text | Always "启用" |

### Stage 7 Output: Final Excel

| Output Column | Source Column |
| ------------- | ------------- |
| 区域id | 试验区域id |
| 区域名称 | 试验区域名称 |
| 商品SKU | 商品编码 |
| 商品名称 | 商品名称 |
| 调价方向 | 调价方向 |
| 调价幅度 | 调价幅度 (empty) |
| 设置状态 | 设置状态 ("启用") |
| 固定抽佣比例 | 固定抽佣比例 |
| 固定抽佣货值 | 固定抽佣货值 |
| 商品id | 商品id |
| 后台类目名称 | 后台类目名称 |

## Entity Relationships

```
conf_试验商品信息(日期=today) ──┐
                                ├──[INNER JOIN on 商品id]──→ Filtered Products (Stage 2)
conf_商品信息(日期=today-1) ────┘

conf_区县信息 ──┐
                ├──[LEFT JOIN on 市id = 区域id]──→ Marked Regions (Stage 3)
conf_试验分组配置┘

Marked Regions ─┐
                ├──[LEFT JOIN on 试验分组 + 运营类型]──→ Regions + Commission (Stage 4)
conf_试验周期抽佣率┘

Stage 4 ────────┐
                ├──[LEFT JOIN on 市id]──→ Regions + Logistics Fee (Stage 5)
conf_线上隐形物流费┘

Filtered Products × Regions + Logistics Fee ──→ Pricing Plan (Stage 6)
```
