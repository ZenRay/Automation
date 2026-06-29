# Tasks: cr_analyze — Commission Rate Trial Analysis Dashboard

**Input**: Design documents from `specs/003-cr-analyze-dashboard/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/cli-interface.md, quickstart.md

**Tests**: Unit tests (Phase 2/4) and E2E tests (Phase 5) are included per spec requirements.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1=Pipeline, US2~US6=Dashboard Tabs, US7=Power Analysis)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and dependency management

- [x] T001 Create module directory structure: `workers/cr_analyze/`, `workers/cr_analyze/dashboard/`, `workers/cr_analyze/data/`, `tests/cr_analyze/`
- [x] T002 [P] Create `workers/cr_analyze/__init__.py` with re-export of `run_cr_analyze_pipeline`
- [x] T003 [P] Create `workers/cr_analyze/dashboard/__init__.py` (empty)
- [x] T004 [P] Create `workers/cr_analyze/data/.gitkeep`
- [x] T005 [P] Create `tests/cr_analyze/__init__.py` (empty)
- [x] T006 Add `streamlit`, `plotly`, `streamlit-aggrid` to dependencies in `pyproject.toml`

**Checkpoint**: Module skeleton exists, dependencies installed, `source .venv/bin/activate && pip install -e .` succeeds

---

## Phase 2: Data Pipeline Core (US1 — Daily Data Refresh Pipeline) 🎯 MVP

**Goal**: Complete data extraction from 6 Lark tables + MaxCompute SQL → SQLite storage → aggregation wide table computation. Deliver with unit tests.

**Independent Test**: Run `python -m workers.cr_analyze.main --date 2026-06-20` and verify SQLite contains 8 tables (6 Lark + 1 fact + 1 wide table) with correct row counts.

### Unit Tests for Pipeline (write first, verify fail)

- [x] T007 [P] [US1] Create shared test fixtures (sample DataFrames for all 6 Lark tables + fact table) in `tests/cr_analyze/conftest.py`
- [x] T008 [P] [US1] Write config completeness tests (6 LarkSourceConfig, SQLQueryConfig, field_names non-empty, SQL file exists, TRIAL_PHASE_CONFIG keys) in `tests/cr_analyze/test_config.py`
- [x] T009 [P] [US1] Write SQLite store tests (write_tables roundtrip, table overwrite behavior, read_table with missing table) in `tests/cr_analyze/test_sqlite_store.py`
- [x] T010 [P] [US1] Write transformer tests (stage derivation from date ranges, city_unit merging, public filter application, stage_week calculation, is_complete_week logic, trading_days with Dragon Boat Festival exclusion) in `tests/cr_analyze/test_transformer.py`

### Task 2.1: Configuration + Storage Layer

- [x] T011 [US1] Implement `workers/cr_analyze/config.py` with all business configuration:
  - `LARK_SOURCES`: 6 `LarkSourceConfig` entries (conf_product_info, conf_county_info, conf_commission_adjustment, conf_trial_region_price, conf_trial_group, conf_trial_period_rate) with per-source field_names and date filters per FR-001/FR-002
  - `SQL_QUERIES`: `SQLQueryConfig` list referencing `order_fact_whole.sql`
  - `SQL_BASE_DIR`: `Path(__file__).parent / "sql"`
  - `FIELD_MAPPING`: dict mapping SQL output column names to wide table field names per FR-019a
  - `TRIAL_PHASE_CONFIG`: Dragon Boat Festival dates `[date(2026,6,19), date(2026,6,20), date(2026,6,21)]`, historical baseline ranges `(date(2026,4,13), date(2026,4,26))` and `(date(2026,5,11), date(2026,5,24))`
  - `ALERT_THRESHOLDS`: stage-specific guardrail thresholds per FR-035
  - `TARGET_R0_REFERENCE`: phase × region_type × trial_group target values per DP-003 H-1 table
- [x] T012 [US1] Implement `workers/cr_analyze/sqlite_store.py`:
  - `write_tables(db_path: str, data: dict[str, pd.DataFrame]) -> int`: write each DataFrame as a SQLite table using `to_sql(if_exists="replace")`, return table count
  - `read_table(db_path: str, table_name: str) -> pd.DataFrame`: read single table
  - `list_tables(db_path: str) -> list[str]`: list all table names
  - `table_exists(db_path: str, table_name: str) -> bool`: check existence

**Checkpoint**: `pytest tests/cr_analyze/test_config.py tests/cr_analyze/test_sqlite_store.py -v` all pass

### Task 2.2: Orchestration + Aggregation Logic

- [x] T013 [US1] Implement `workers/cr_analyze/transformer.py`:
  - `compute_wide_table(lark_data: dict, mc_data: dict, config: dict) -> pd.DataFrame`: core aggregation function
    - Join fact_order_item with conf_trial_group for city_unit merging and trial_group labeling (FR-015)
    - Join with conf_trial_period_rate for stage derivation based on date ranges (FR-012)
    - Join with conf_product_info for sku_origin, sku_grade, sku_weight_spec
    - Join with conf_commission_adjustment for 参与试验类型 filter (FR-016a)
    - Compute stage_week for 生效期 (7-day rolling from start date) and is_complete_week (FR-017)
    - Compute trading_days excluding Dragon Boat Festival dates (FR-018)
    - Aggregate by stage-specific granularity (FR-014): 预备期 daily, 摸底期 overall, 生效期 weekly
    - Compute measures: order_count, active_store_count (cross-SKU dedup, FR-019), gmv, commission_amount, stockout_num, commission_rate, supply_price (AVG 商家供货斤单价, FR-013), target_r0
  - `preprocess_lark_dates(lark_data: dict) -> dict`: normalize date fields per LarkSourceConfig.date_fields
- [x] T014 [US1] Implement `workers/cr_analyze/main.py`:
  - `_init_lark_client()`: read credentials from `automation.conf.lark` (prod section), instantiate `LarkMultiDimTable`
  - `_init_mc_client()`: read credentials from `automation.conf.maxcomputer`, instantiate `MaxComputerClient`
  - `run_cr_analyze_pipeline(date: date, db_path: str) -> int`: orchestration function
    1. Init Lark + MC clients
    2. Extract 6 Lark sources via `extract_all_lark_sources()`
    3. Execute MC queries via `execute_all_queries()`
    4. Preprocess dates, compute wide table via `transformer.compute_wide_table()`
    5. Write all tables to SQLite via `sqlite_store.write_tables()`
    6. Log summary (table names, row counts)
    7. Return 0 on success, 1 on failure
  - CLI entry `main()`: argparse with `--date` (YYYY-MM-DD, default today), `--db-path` (default `data/cr_analyze.db`), `--power` (store_true flag)
  - When `--power` flag set, call power analysis instead of (or after) main pipeline
- [x] T015 [US1] Update `workers/cr_analyze/__init__.py` to re-export `run_cr_analyze_pipeline`

**Checkpoint**: `pytest tests/cr_analyze/test_transformer.py -v` all pass; `python -m workers.cr_analyze.main --date 2026-06-20` produces valid SQLite (with real credentials)

---

## Phase 3: Streamlit Dashboard (US2~US6 — Dashboard Tabs)

**Goal**: Complete 5-tab Streamlit dashboard consuming SQLite data. Each tab renders its designated view with interactive charts and filters.

**Independent Test**: `streamlit run workers/cr_analyze/dashboard/app.py` opens browser with all 5 tabs functional.

### Shared Dashboard Infrastructure

- [x] T016 [US2] Implement `workers/cr_analyze/dashboard/components.py`:
  - `load_db(db_path: str) -> dict[str, pd.DataFrame]`: load all tables from SQLite with `@st.cache_data`
  - `render_filters(df: pd.DataFrame) -> dict`: sidebar filters (date range, product multi-select, region filter) returning filter state
  - `apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame`: apply selected filters to DataFrame
  - `render_metric_card(label: str, value, delta=None)`: metric card component
  - `render_alert_badge(level: str)`: GREEN/YELLOW/RED badge component

### Tab 1: Trial Overview (US2)

- [x] T017 [US2] Implement `workers/cr_analyze/dashboard/tab1_overview.py`:
  - `render(db_path: str)`: render function
  - Current phase status card (derived from conf_trial_period_rate date ranges vs today)
  - 8-city grouping table: city_unit, region_type, trial_group, target r₀, SKU list
  - Phase timeline with Dragon Boat Festival (2026-06-19~21) annotation
  - Data sources: conf_trial_group, conf_trial_period_rate

### Tab 2: Configuration Audit (US3)

- [x] T018 [US3] Implement `workers/cr_analyze/dashboard/tab2_config_audit.py`:
  - `render(db_path: str)`: render function
  - **H-1 section**: Commission rate deviation table
    - Join conf_commission_adjustment with conf_trial_period_rate on (trial_group, stage)
    - Build county→city→trial_group mapping using conf_county_info latest snapshot date (max 日期) + conf_trial_group mapping
    - Compute r_deviation = configured_r - target_r
    - Highlight rows where |r_deviation| > 0.5% in red (FR-021)
    - Flag: participate_type = "非试验区域" in trial cities (FR-022)
    - Flag: region_type = "代理人" with configured_r ≈ 7.5% (FR-022)
    - Compute and show 隐形物流费加价 = 调整系数 × 固定抽佣金额调整 in the H-1 main table (same table, not separate section)
    - Add 商家供货斤单价 and 商城销售斤单价 after 试验分组 in H-1 main table
    - Source H-1 price fields by participation type: 试验区域 from conf_trial_region_price (日期+商品id+城市归一化键 from 区域全称), 非试验区域 from conf_product_info (日期+商品id)
    - Reuse target reference for H-1 deviation calculation only; do not render duplicate reference table in Tab 2 (FR-023)
  - **H-2 section**: Supplier price trend chart
    - Add SKU selector for H-2
    - Restrict H-2 SKU options to trial products that are sellable on the latest product date (是否当日上架=1)
    - Display each SKU option label using the latest-date 商品名称 for that SKU
    - Chart 1: non-trial-region dual-axis trend (商家供货斤单价 + 抽佣率) from conf_product_info, with both y-axes starting at 0 (FR-024)
    - Chart 2: trial-region city-level grouped bar chart of 商家供货斤单价 by date from conf_trial_region_price (FR-024)
    - Chart 3: trial-region city-level grouped bar chart of 抽佣率 by date from conf_trial_region_price (FR-024)
    - Chart 4: trial-region city-level grouped bar chart of 平台销售斤单价 by date from conf_trial_region_price (FR-024)
    - Ensure H-2 date axis displays full YYYY-MM-DD format (FR-024a)
    - Mark 是否当日上架 = true rows as 当日可售卖标记（非新品上架语义）(FR-025)

### Tab 3: Normalization Progress (US4)

- [x] T019 [US4] Implement `workers/cr_analyze/dashboard/tab3_normalization.py`:
  - `render(db_path: str)`: render function
  - Filter agg_wide_table to 归一化预备期 granularity (stage × 日期 × city_unit × region_type)
  - Multi-line chart: daily r₀ per city × region_type, with target lines at 7.5% (自营) and 4.6% (代理人) (FR-026)
  - Deviation bar chart: commission_rate - target_r₀, red when |deviation| > 1% (FR-027)
  - Summary table: city, latest r₀, target r₀, deviation, status (达标/偏高/偏低) (FR-028)

### Tab 4: Effect Analysis (US5)

- [x] T020 [US5] Implement `workers/cr_analyze/dashboard/tab4_effect.py`:
  - `render(db_path: str)`: render function with 4 switchable sub-views (st.radio or st.tabs)
  - **Sub-view B (City Trends)**: Filter agg_wide_table; plot 摸底期 as baseline points + 生效期 by stage_week; mark incomplete weeks with ⚠️ and "N/7天" (FR-030)
  - **Sub-view C (Group Aggregation)**: 对照组/试验组一/试验组二/试验组三 trends; switchable metric (commission_amount/gmv/order_count) and aggregation mode selector (总量/均值) via st.selectbox (FR-031)
  - **Sub-view D (SKU Comparison)**: 3 SKUs side-by-side within each trial_group as grouped bar chart with numerical annotations; metric selector supports commission_amount/gmv/order_count/active_store_count/commission_rate (FR-032)
  - **Sub-view E (Origin Comparison)**: 云南 = SKU 10184690 + 20519020 sum; 广西 = SKU 20588413; compare commission_amount/commission_rate/gmv/order_count/active_store_count, with weighted commission_rate aggregation (FR-033)

### Tab 5: Guardrail Alerts (US6)

- [x] T021 [US6] Implement `workers/cr_analyze/dashboard/tab5_guardrail.py`:
  - `render(db_path: str)`: render function
  - Add metric selector (中文) and SKU selector
  - During 摸底期, show actual monitoring metrics without alert lights
  - During 生效期, compute wow (week-over-week): order_count_wow, store_count_wow per city × SKU
  - Compute WoW by trial stage-week sequence (`stage_week`) instead of natural week (FR-036)
  - Assign alert_level per FR-035 thresholds:
    - 生效期: order_count wow < -10% = YELLOW, < -15% = RED
    - active_store_count wow < -5% = YELLOW, < -10% = RED
  - Alert status table with 🟢/🟡/🔴 coloring using streamlit-aggrid cellStyle (FR-034)
  - stockout_num trend line chart (FR-037)
  - Alert threshold reference table at page bottom (FR-037)
  - Add 2025 vs 2026 comparison sub-view with city selector (`省名称-市名称` + `整体`) and attribute multi-select filters

### Dashboard Entry Point

- [x] T022 [US2] Implement `workers/cr_analyze/dashboard/app.py`:
  - Parse `--db-path` from Streamlit args (default: `workers/cr_analyze/data/cr_analyze.db`)
  - Check SQLite existence; show error if missing (FR-046)
  - `st.tabs` layout with 6 tabs: 试验总览, 配置核查, 归一化进度, 效应分析, 护栏预警, 功效分析
  - Call each tab module's `render(db_path)` function
  - Tab 6 placeholder (populated in Phase 4)

**Checkpoint**: `streamlit run workers/cr_analyze/dashboard/app.py` loads all 5 tabs without errors; each tab renders expected visualizations with populated SQLite data

---

## Phase 4: Power Analysis (US7 — Statistical Power Analysis)

**Goal**: CLI computes σ/ρ/power for 3 SKUs; results written to SQLite; dashboard Tab 6 displays results with interpretive conclusions.

**Independent Test**: `python -m workers.cr_analyze.main --power` writes 3 rows to power_analysis table; dashboard Tab 6 shows σ/ρ tables with conclusions.

### Tests for Power Analysis (write first)

- [x] T023 [P] [US7] Write power analysis unit tests (σ_raw computation with known data, ρ_pre/ρ_post Pearson correlation, power formula n_required, cross-correlation pairs, fallback behavior for insufficient data) in `tests/cr_analyze/test_power_analysis.py`

### Implementation

- [x] T024 [US7] Add `compute_power_analysis(fact_df: pd.DataFrame, config: dict) -> pd.DataFrame` to `workers/cr_analyze/transformer.py`:
  - Filter fact data to historical baseline weeks (config TRIAL_PHASE_CONFIG)
  - Per-SKU σ: per-city CV → mean of 8 CVs → × 1.5 (FR-039)
  - Per-SKU ρ: Pearson(W1,W2), Pearson(W3,W4), min (FR-040)
  - Power formula: n_required = 4 × σ² × (1-ρ) × 7.84 / 0.01 (FR-041)
  - Cross-correlation: 3 SKU pairs, flag if ρ > 0.5 (FR-042)
  - Fallback: use 预备期+摸底期 data if < 3 complete weeks (FR-044)
  - Return DataFrame with columns: sku_id, sigma_raw, sigma_adjusted, rho_pre, rho_post, rho_main, n_required, n_actual, power_sufficient
- [x] T025 [US7] Extend `workers/cr_analyze/main.py` `--power` flag handling:
  - When `--power` is set, load fact_order_item from SQLite (or extract if not yet stored)
  - Call `transformer.compute_power_analysis()`
  - Write results to `power_analysis` SQLite table
  - Print summary to stdout
- [x] T026 [US7] Implement `workers/cr_analyze/dashboard/tab_power.py`:
  - `render(db_path: str)`: render function
  - Check `power_analysis` table existence; show instruction if missing
  - Display σ/ρ result tables with reference values (σ_raw≈0.194, ρ≈0.993 for SKU 10184690)
  - Power verification table with pass/fail conclusions per SKU
  - SKU cross-correlation matrix with risk flag annotation
  - Interpretive text conclusions for each metric (FR-043)
- [x] T027 [US7] Wire Tab 6 (功效分析) into `workers/cr_analyze/dashboard/app.py` to call `tab_power.render(db_path)`

**Checkpoint**: `pytest tests/cr_analyze/test_power_analysis.py -v` all pass; `python -m workers.cr_analyze.main --power` produces correct results; dashboard Tab 6 displays conclusions

---

## Phase 5: E2E Testing + Integration Validation

**Goal**: End-to-end tests validating the full pipeline (mock extract → real aggregation → real SQLite → verify) and dashboard smoke tests.

### E2E Tests

- [x] T028 [P] Implement pipeline E2E test in `tests/cr_analyze/test_pipeline_e2e.py`:
  - `@pytest.mark.integration` marker
  - Mock `extract_all_lark_sources` and `execute_all_queries` with `unittest.mock.patch`, inject conftest sample DataFrames
  - Call `run_cr_analyze_pipeline(date, db_path)` with temp SQLite path
  - Assert: 8 tables exist in SQLite (6 Lark + 1 fact + 1 wide)
  - Assert: wide table has correct columns per data-model.md §3
  - Assert: wide table aggregation granularity correct per stage
  - Assert: public filters applied (only 3 SKUs, valid orders, trial regions)
  - Assert: Dragon Boat Festival dates excluded from trading_days
  - Edge case test: empty fact data → wide table empty, no crash
  - Edge case test: conf_trial_period_rate missing current date range → stage = None, appropriate handling
- [x] T029 [P] Implement dashboard smoke E2E test in `tests/cr_analyze/test_dashboard_e2e.py`:
  - `@pytest.mark.integration` marker
  - Create temp SQLite with sample data
  - For each tab module: import and call `render(db_path)` within a mock Streamlit context (or `st.AppTest` if available)
  - Assert: no unhandled exceptions raised
  - Assert: each tab produces output (non-empty render)
  - Edge case: missing SQLite → app.py shows error message
  - Edge case: empty power_analysis table → Tab 6 shows instruction message

### Final Validation

- [x] T030 Run full test suite: `pytest tests/cr_analyze/ -v` and verify all pass
- [x] T031 Run quickstart.md validation scenarios end-to-end (Phase 1 pipeline + Phase 2 dashboard launch + Tab checklist)
- [x] T032 Run `black --check workers/cr_analyze/ tests/cr_analyze/` for code formatting

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Pipeline (Phase 2)**: Depends on Setup — **BLOCKS** all subsequent phases
- **Dashboard (Phase 3)**: Depends on Phase 2 (needs SQLite with wide table)
- **Power Analysis (Phase 4)**: Depends on Phase 2 (needs fact data in SQLite); can run in parallel with Phase 3
- **E2E (Phase 5)**: Depends on Phases 2, 3, and 4

### User Story Dependencies

- **US1 (Pipeline)**: Foundation — all other stories depend on it
- **US2~US6 (Dashboard Tabs)**: Depend on US1; independent of each other (can parallelize)
- **US7 (Power Analysis)**: Depends on US1; independent of US2~US6

### Parallel Opportunities

- Phase 1: T002~T005 all parallel (different files)
- Phase 2: T007~T010 all parallel (different test files); T011~T012 sequential within Task 2.1
- Phase 3: T017~T021 all parallel (different tab files, once T016 components.py done)
- Phase 4: T023~T027 sequential (tests → implementation → wiring)
- Phase 5: T028~T029 parallel (different test files)

---

## Parallel Example: Dashboard Tabs

```bash
# After T016 (components.py) is done, launch all tabs together:
Task T017: tab1_overview.py
Task T018: tab2_config_audit.py
Task T019: tab3_normalization.py
Task T020: tab4_effect.py
Task T021: tab5_guardrail.py
# Then T022 (app.py) wires them together
```

---

## Implementation Strategy

### MVP First (US1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Data Pipeline Core (US1)
3. **STOP and VALIDATE**: Run pipeline, verify SQLite output
4. Deliverable: working data pipeline with unit tests

### Incremental Delivery

1. Setup + Pipeline (Phase 1+2) → Data layer complete
2. Dashboard (Phase 3) → Visual analysis available (US2~US6)
3. Power Analysis (Phase 4) → Statistical validation available (US7)
4. E2E Testing (Phase 5) → Integration confidence
5. Each phase adds value without breaking previous phases

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks
- Tests are written before implementation (TDD-style within each phase)
- Commit after each task or logical group
- Stop at any checkpoint to validate independently
- All SQL file references use `order_fact_whole.sql` (the actual filename)
- `--power` is a `store_true` flag, not a subcommand
