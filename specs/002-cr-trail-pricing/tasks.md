# Tasks: Trial Region Commission Pricing Generator

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)
**Created**: 2026-06-18
**Status**: Draft

## Dependencies

```
Phase 1 (Setup) → Phase 2 (Config) → Phase 3 (Pipeline) → Phase 4 (E2E Test)
```

All phases are strictly sequential — each depends on the previous phase's output.

## Phase 1: Setup

- [x] T001 Create module and test directory structure: `workers/cr_trail_pricing/__init__.py` and `tests/cr_trail_pricing/__init__.py`

## Phase 2: Module Configuration (Task 1)

**Story goal**: Define all business configuration — Lark source configs, field names, column mappings — as named constants in `config.py`. This is the sole business knowledge injection point per Constitution Principle II.

**Independent test**: `pytest tests/cr_trail_pricing/test_config.py -v` — all config constants are importable and structurally valid.

### Unit Tests

- [x] T002 [P] Create unit tests for config in `tests/cr_trail_pricing/test_config.py`
  - Test `APP_TOKEN` is the expected value `GmWEbfgCmatLAnsSyaocBisJnoe`
  - Test `LARK_SOURCES` has exactly 6 entries, each is a `LarkSourceConfig` with non-empty `name`, `url`, `table_name`, `field_names`
  - Test source names: `conf_county`, `conf_goods`, `conf_trial_group`, `conf_trial_commission`, `conf_trial_goods`, `conf_hidden_logistics`
  - Test `conf_goods` and `conf_hidden_logistics` have `date_filter_field="日期"` and `date_fields=["日期"]`
  - Test `conf_county`, `conf_trial_group`, `conf_trial_commission`, `conf_trial_goods` have `date_filter_field=None`
  - Test `PRODUCT_KEEP_FIELDS` contains exactly: 商品id, 商品编码, 商品名称, 四级类目名称, 非试验区域抽佣率, 毛重
  - Test `REGION_OUTPUT_FIELDS` contains: 日期, 试验区域id, 试验区域名称, 市id, 省id, 区县id, 是否试验区域, 抽佣率
  - Test `COLUMN_RENAME_MAP` maps: 试验区域id→区域id, 试验区域名称→区域名称, 商品编码→商品SKU
  - Test `OUTPUT_COLUMNS` has exactly 11 columns in order: 区域id, 区域名称, 商品SKU, 商品名称, 调价方向, 调价幅度, 设置状态, 固定抽佣比例, 固定抽佣货值, 商品id, 四级类目名称
  - Test all LarkSourceConfig URLs contain `GmWEbfgCmatLAnsSyaocBisJnoe`

### Implementation

- [x] T003 Create `workers/cr_trail_pricing/config.py` with all business configuration constants:
  - `APP_TOKEN = "GmWEbfgCmatLAnsSyaocBisJnoe"`
  - 6 `LarkSourceConfig` entries in `LARK_SOURCES` list (see data-model.md for field lists per table)
  - `PRODUCT_KEEP_FIELDS` list
  - `REGION_OUTPUT_FIELDS` list
  - `COLUMN_RENAME_MAP` dict
  - `OUTPUT_COLUMNS` list
  - URL format: `https://bggc.feishu.cn/base/GmWEbfgCmatLAnsSyaocBisJnoe?table={table_id}&view=default`

- [x] T004 Update `workers/cr_trail_pricing/__init__.py` to re-export `run_cr_trail_pricing_pipeline` from main (forward declaration, will be implemented in Task 2)

- [x] T005 Run `pytest tests/cr_trail_pricing/test_config.py -v` and verify all tests pass

## Phase 3: Pipeline Implementation (Task 2)

**Story goal**: Implement the 7-stage data processing pipeline and CLI entry point. Each stage is an independent function in `transformer.py`, orchestrated by `main.py` with stage-level logging.

**Independent test**: `pytest tests/cr_trail_pricing/ -v` — pipeline stages produce correct output for known inputs.

### Implementation

- [x] T006 [US1] [US2] Create `workers/cr_trail_pricing/transformer.py` with 7 stage functions:
  - `extract_sources(lark_client, sources)` → calls `extract_all_lark_sources()`
  - `filter_trial_products(conf_goods, conf_trial_goods, target_date)` → INNER JOIN on 商品id (conf_trial_goods=today, conf_goods=yesterday), 非试验区域抽佣率 from conf_trial_goods, add 是否试验区域=1
  - `mark_trial_regions(conf_county, conf_trial_group, target_date)` → LEFT JOIN on city_id==区域id, filter 区域类型=="CITY" and date range, add 是否试验区域 flag
  - `associate_commission(regions_df, conf_trial_commission, target_date)` → LEFT JOIN on 试验分组+运营类型, filter date range, keep REGION_OUTPUT_FIELDS
  - `associate_logistics_fee(regions_df, conf_hidden_logistics, target_date)` → LEFT JOIN on 市id, compute 隐形物流费率 with 4-branch JSON override logic
  - `compute_pricing(products_df, regions_df)` → Cartesian product, compute 固定抽佣比例/固定抽佣货值/调价方向/设置状态
  - `export_excel(df, output_path)` → rename columns, select OUTPUT_COLUMNS, write to_excel

- [x] T007 [US1] [US5] Create `workers/cr_trail_pricing/main.py` with:
  - `_init_lark_client()` → reads credentials from `automation.conf.lark`
  - `run_cr_trail_pricing_pipeline(target_date, output_path)` → orchestrates 7 stages with timing/logging
  - `main()` → argparse with `--date` (default today) and `--output` (default `cr_trail_pricing_{date}.xlsx`)
  - Stage logging format: `[Stage N/7] {name}: {rows} rows, {elapsed:.2f}s`
  - Fail-fast on extraction errors with table name identification

- [x] T008 Update `workers/cr_trail_pricing/__init__.py` with actual `run_cr_trail_pricing_pipeline` import

## Phase 4: End-to-End Tests (Task 3)

**Story goal**: Validate the full pipeline with mocked Lark data covering all business logic branches.

**Independent test**: `pytest tests/cr_trail_pricing/test_pipeline.py -v` — all tests pass without network access.

### Implementation

- [x] T009 [US1] [US5] Create `tests/cr_trail_pricing/test_pipeline.py` with mocked tests:
  - `test_full_pipeline`: mock extract_all_lark_sources with ~3 products, ~5 regions (2 trial, 3 non-trial), run full pipeline, verify output Excel row count, column names, and calculated values
  - `test_empty_products`: mock with no matching trial products, verify output has headers only
  - `test_hidden_logistics_fee_mapping`: validate all 4 branches — no match→0, empty mapping→city rate, county in mapping→county rate, county not in mapping→city rate
  - `test_pricing_direction_trial`: verify "涨价"/"降价"/"不变" based on signed diff
  - `test_pricing_direction_non_trial`: verify always "涨价"

- [x] T010 Run `pytest tests/cr_trail_pricing/ -v` and verify all tests pass

## Implementation Strategy

**MVP scope**: Phase 1 + Phase 2 (config + tests) — establishes the configuration contract that all downstream code depends on.

**Incremental delivery**:
1. Phase 2 first: config.py with unit tests validates the data model contract
2. Phase 3 next: pipeline implementation uses config.py constants directly
3. Phase 4 last: e2e tests validate the integrated pipeline

**Parallel opportunities**: Within Phase 2, T002 (tests) and T003 (config) can be developed in parallel since they target different files.
