# Implementation Plan: Trial Region Commission Pricing Generator

**Feature**: [spec.md](./spec.md)
**Branch**: `worktem`
**Created**: 2026-06-18

## Technical Context

| Item | Value |
| ---- | ----- |
| Language | Python 3.12 |
| Framework | pandas (sole DataFrame library) |
| Data Source | Lark multi-dimensional tables (6 tables, single app_token) |
| Output | Excel file (.xlsx) |
| Module Path | `workers/cr_trail_pricing/` |
| Framework Deps | `workers.lib` (LarkSourceConfig, extract_all_lark_sources) |
| Data Volume | ~10 products x ~5,000 regions = ~50K rows |
| CLI Interface | `python -m workers.cr_trail_pricing.main [--date YYYY-MM-DD] [--output path.xlsx]` |

## Constitution Check

| Principle | Status | Notes |
| --------- | ------ | ----- |
| I. Layer Isolation | PASS | New independent module under `workers/cr_trail_pricing/`; no cross-domain coupling; uses `automation/client/` via `workers.lib` |
| II. Configuration-Driven | PASS | All table IDs, field names, column mappings centralized in `config.py` |
| III. Date Type Unification | PASS | All date columns converted to `datetime.date` at extract stage; `date.today()` for cutoffs; UTC midnight for Lark ExactDate |
| IV. Data Pipeline Integrity | PARTIAL | Output is Excel (not Lark write-back) — no DataRoute/LarkTargetConfig needed. Extract → Transform → Export pattern. Justified: spec explicitly requires Excel output (FR-012), not Lark write-back |
| V. Credential Security | PASS | API credentials read from `automation.conf.lark`; no hardcoding |
| VI. Containerization | N/A | No container changes in this feature |
| VII. CI/CD | N/A | No CI/CD changes in this feature |

### Gate Evaluation

- **Principle IV deviation**: The standard ETL flow is Extract → Transform → Route → Load (to Lark). This module outputs Excel instead. No DataRoute/LarkTargetConfig/CleanupCondition needed. This is an explicit spec requirement (FR-012), not a design shortcut. The module still uses `extract_all_lark_sources()` for extraction and follows the config-driven pattern.

## Task Breakdown (3 Tasks)

### Task 1: Module Configuration (`config.py`)

**Scope**: Create `workers/cr_trail_pricing/__init__.py` and `config.py`

**Files**:
- `workers/cr_trail_pricing/__init__.py` — re-exports `run_cr_trail_pricing_pipeline`
- `workers/cr_trail_pricing/config.py` — all business configuration

**Deliverables**:
- `APP_TOKEN` constant: `GmWEbfgCmatLAnsSyaocBisJnoe`
- 6 `LarkSourceConfig` entries in `LARK_SOURCES`:
  - `conf_county` (conf_区县信息) — date filter on 日期 field, exact date = target date (API-level)
  - `conf_goods` (conf_商品信息) — date filter on 日期 field, exact date = **target_date - 1** (yesterday)
  - `conf_trial_group` (conf_试验分组配置) — no date filter (filtered in pandas by date range)
  - `conf_trial_commission` (conf_试验周期抽佣率) — no date filter (filtered in pandas by date range)
  - `conf_trial_goods` (conf_试验商品信息) — date filter on 日期 field, exact date = target date (API-level)
  - `conf_hidden_logistics` (conf_线上隐形物流费) — date filter on 日期 field, exact date = target date
- `PRODUCT_KEEP_FIELDS`: list of fields to retain after Stage 2 (includes `是否试验区域`)
- `REGION_OUTPUT_FIELDS`: list of fields for Stage 4 output (includes `运营类型`)
- `COLUMN_RENAME_MAP`: Stage 7 column renaming dictionary
- `OUTPUT_COLUMNS`: final Excel column order
- `TABLE_IDS`: dictionary mapping source names to table_ids (4 tables need table_ids discovered during implementation)

**Acceptance**:
- All constants defined with named values, no magic strings
- LarkSourceConfig entries follow the same pattern as `workers/okr/config.py`
- Date filter fields correctly configured for sources that support API-level filtering

### Task 2: Pipeline Implementation (`transformer.py` + `main.py`)

**Scope**: Create the 7-stage pipeline and CLI entry point

**Files**:
- `workers/cr_trail_pricing/transformer.py` — 7 stage functions
- `workers/cr_trail_pricing/main.py` — pipeline orchestration + argparse

**Stage Functions** (each takes DataFrame(s) + date → returns DataFrame):

| Stage | Function | Input | Output |
| ----- | -------- | ----- | ------ |
| 1 | `extract_sources()` | lark_client, sources | `dict[str, DataFrame]` |
| 2 | `filter_trial_products()` | conf_goods, conf_trial_goods, date | products DataFrame |
| 3 | `mark_trial_regions()` | conf_county, conf_trial_group, date | regions with 是否试验区域 flag |
| 4 | `associate_commission()` | Stage 3 result, conf_trial_commission, date | regions with 抽佣率 |
| 5 | `associate_logistics_fee()` | Stage 4 result, conf_hidden_logistics, date | regions with 隐形物流费率 |
| 6 | `compute_pricing()` | Stage 2 products × Stage 5 regions | pricing plan DataFrame |
| 7 | `export_excel()` | Stage 6 result, output_path | Excel file |

**main.py**:
- `run_cr_trail_pricing_pipeline(target_date, output_path)` — orchestrates all stages
- Stage-level logging: stage name, row count, elapsed time (FR-019)
- Fail-fast on Lark API errors with table identification (FR-018)
- CLI: `--date` (default today), `--output` (default `cr_trail_pricing_{date}.xlsx`)
- Empty result handling: output Excel with headers only + warning log

**Key Implementation Details**:

Stage 2 (Product Filtering):
- `conf_试验商品信息` already filtered to `日期=target_date` by API-level filter
- `conf_商品信息` already filtered to `日期=target_date-1` by API-level filter (yesterday)
- INNER JOIN on `商品id` only (dates differ between tables, no date-based join)
- Use `suffixes=("_goods", "_trial")` for conflicting column names
- `非试验区域抽佣率` sourced from conf_试验商品信息 (NOT conf_商品信息)
- Add `是否试验区域 = 1` for all matched products
- Keep: 商品id, 商品编码, 商品名称, 后台类目名称, 非试验区域抽佣率, 毛重, 是否试验区域

Stage 3 (Region Marking):
- Filter `conf_trial_group` to `区域类型 == "CITY"` AND `target_date` within trial date range
- LEFT JOIN `conf_county` on `city_id == 区域id`
- Set `是否试验区域 = 1` for matched rows, `0` for unmatched
- Preserve trial group attributes (试验类型, 试验分组, 试验区域id, 试验区域名称) from matched config

Stage 4 (Commission Association):
- Filter `conf_trial_commission` to `target_date` within trial date range
- LEFT JOIN Stage 3 result on `试验分组` + `运营类型` match AND date in range
- Fill missing 抽佣率 with 0 for non-trial regions
- Keep: 日期, 试验区域id, 试验区域名称, 市id, 省id, 区县id, 是否试验区域, 试验分组, 运营类型, 抽佣率

Stage 5 (Hidden Logistics Fee):
- Filter `conf_hidden_logistics` to `日期 == target_date`
- LEFT JOIN Stage 4 result on `市id`
- Compute `隐形物流费率`:
  - No match → 0
  - `区县费率映射` empty → use `费率`
  - `区县费率映射` non-empty and `区县id` in JSON keys → use mapped value
  - `区县费率映射` non-empty and `区县id` not in JSON keys → use `费率`

Stage 6 (Cartesian Product + Calculation):
- Cross join Stage 2 products × Stage 5 regions
- For `是否试验区域 == 1`:
  - `signed_diff = 非试验区域抽佣率 - 抽佣率`
  - `固定抽佣比例 = abs(signed_diff)`
  - Direction: signed_diff < 0 → "降价", == 0 → "不变", > 0 → "涨价"
- For `是否试验区域 == 0`:
  - `固定抽佣货值 = 隐形物流费率 × 毛重`
  - Direction: always "涨价"
- `调价幅度 = NaN`, `设置状态 = "启用"`

Stage 7 (Excel Export):
- Rename columns per `COLUMN_RENAME_MAP`
- Select and order columns per `OUTPUT_COLUMNS`
- Write to Excel via `pd.DataFrame.to_excel()`

**Acceptance**:
- Each stage function is independently callable with DataFrame inputs
- Pipeline produces correct output for known test data
- Stage logging format: `[Stage N/7] {name}: {rows} rows, {elapsed}s`
- CLI accepts `--date` and `--output` parameters

### Task 3: End-to-End Test

**Scope**: Create test module with mocked Lark data to validate the full pipeline

**Files**:
- `workers/cr_trail_pricing/tests/__init__.py`
- `workers/cr_trail_pricing/tests/test_pipeline.py`

**Test Strategy**:
- Mock `extract_all_lark_sources()` to return known DataFrames
- Run the full pipeline with a fixed test date
- Validate output Excel content against expected results

**Test Cases**:
1. **test_full_pipeline**: End-to-end run with ~3 products, ~5 regions (2 trial, 3 non-trial). Verify:
   - Output row count = products × regions
   - Trial region rows have correct `固定抽佣比例` and direction
   - Non-trial region rows have correct `固定抽佣货值` and direction = "涨价"
   - Column names match `OUTPUT_COLUMNS`
   - `设置状态` = "启用" for all rows

2. **test_empty_products**: No trial products for the date → output has headers only

3. **test_hidden_logistics_fee_mapping**: Validate all 4 branches of the logistics fee logic:
   - No match → 0
   - Empty mapping → city rate
   - County in mapping → county rate
   - County not in mapping → city rate

**Acceptance**:
- All tests pass with `pytest`
- Tests run without network access (fully mocked)
- Tests validate the business logic in each stage independently

## Complexity Tracking

No constitution violations requiring justification. Principle IV partial compliance (Excel output instead of Lark write-back) is explicitly required by the spec.
