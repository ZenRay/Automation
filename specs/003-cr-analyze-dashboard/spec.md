# Feature Specification: Commission Rate Trial Analysis Dashboard (cr_analyze)

**Feature Branch**: `003-cr-analyze-dashboard`

**Created**: 2026-06-22

**Status**: Draft

**Input**: DP-003 数据开发需求规格 — EXP-003 动态抽佣率试验 · 监控看板数据管道 + 功效分析基础数据

## Clarifications

### Session 2026-06-22

- Q: 功效分析（σ/ρ 计算、功效验证）的交付形式？ → A: 双交付 — CLI 命令负责计算并写入 SQLite，Streamlit 看板设独立页面展示计算结果与分析结论（文字判定）。
- Q: 核心聚合宽表在哪里计算？ → A: 交易明细数据由已有 SQL（order_fact.sql）提取到 SQLite；stage、stage_week、city_unit 等维度结合飞书配置表在本地完成聚合计算。
- Q: 试验阶段日期与城市单元如何配置管理？ → A: 双源 — 飞书配置表（conf_试验分组配置 + conf_试验周期抽佣率）管理摸底期和生效期的分组与目标 r₀；config.py 管理归一化预备期参数、历史基线时间范围、端午节日期（2026-06-19 ~ 2026-06-21）等特殊日期标注。

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Daily Data Refresh Pipeline (Priority: P1)

A data analyst runs the cr_analyze pipeline each morning (or on demand) to refresh the analysis database. The pipeline performs two extraction phases: (1) extracts 5 Lark configuration tables (商品信息, 区县信息, 线上商品区域抽佣率调整, 试验分组配置, 试验周期抽佣率) with per-source date filters; (2) executes the order_fact.sql MaxCompute query returning transaction-level fact data. All raw results are written to SQLite — one table per source, fully overwritten on each run. The pipeline then computes a core aggregation wide table by joining fact data with configuration tables (city_unit merging, stage/stage_week assignment, trial_group labeling) and writes the aggregated result as an additional SQLite table. The analyst invokes via CLI with optional `--date` (defaults to today) and `--db-path` (defaults to module-local data/ directory).

**Why this priority**: Without fresh data in SQLite, the Streamlit dashboard has nothing to visualize. This is the foundational data layer all downstream analysis depends on.

**Independent Test**: Run the pipeline with `--date 2026-06-20`, then inspect the SQLite database to confirm all expected tables exist (5 Lark tables + 1 fact table + 1 aggregation wide table), contain rows, and reflect correct date-filtered data.

**Acceptance Scenarios**:

1. **Given** valid Lark credentials and MaxCompute access, **When** the pipeline runs with no arguments, **Then** a SQLite database is created (or overwritten) at the default path containing all configured source tables plus the aggregation wide table, each populated per its date rule.
2. **Given** the pipeline runs with `--date 2026-06-20`, **When** extraction begins, **Then** Lark sources apply their configured date filters; the MaxCompute SQL template receives date parameters; the aggregation wide table computes stage/stage_week/city_unit based on conf_试验周期抽佣率 date ranges and conf_试验分组配置 mappings.
3. **Given** the pipeline completes successfully, **When** the SQLite database is inspected, **Then** the aggregation wide table contains rows at the correct granularity per stage (归一化预备期: stage × city_unit × region_type by day; 摸底期: stage × city_unit × sku_id overall; 生效期: stage_week × city_unit × sku_id).
4. **Given** the SQLite database already exists from a previous run, **When** the pipeline runs again, **Then** all tables are fully overwritten — no stale rows from the prior run persist.
5. **Given** a Lark API call fails during extraction, **When** the failure occurs, **Then** the pipeline terminates immediately with an error message identifying which table failed.
6. **Given** the pipeline runs with `--db-path /tmp/custom.db`, **When** it completes, **Then** the SQLite database is written to the specified path.

---

### User Story 2 - Tab 1: Trial Overview (Priority: P1)

A trial lead opens the Streamlit dashboard and lands on Tab 1 "试验总览". The tab displays the current trial phase (归一化预备期/摸底期/生效期) with start/end dates, an 8-city grouping configuration table showing city_unit, region_type (自营/代理人), trial_group (G0~G3), current target r₀, and covered SKU list, plus a timeline of key phase milestones including the Dragon Boat Festival period (2026-06-19~21).

**Why this priority**: This is the dashboard landing page and the first thing a trial lead checks. It provides immediate situational awareness of the trial's current state.

**Independent Test**: Launch the dashboard, confirm Tab 1 loads with all 8 cities displayed, correct trial groups, and current phase highlighted.

**Acceptance Scenarios**:

1. **Given** a populated SQLite database, **When** Tab 1 loads, **Then** a phase status card shows the current trial stage (derived from conf_试验周期抽佣率 date ranges relative to today) with start and end dates.
2. **Given** the 8-city grouping configuration, **When** the configuration table renders, **Then** each row shows city_unit, region_type, trial_group, current target r₀ (by phase and region_type), and the SKU ID list (10184690, 20519020, 20588413).
3. **Given** the timeline section, **When** it renders, **Then** all phase boundaries are shown; the Dragon Boat Festival period (2026-06-19~21) is annotated within the 摸底期 timeline.

---

### User Story 3 - Tab 2: Configuration Audit (Priority: P1)

An operations analyst navigates to Tab 2 "配置核查" to verify that commission rate configurations and supplier pricing are correctly set up before each trial phase transition. The tab has two sections: H-1 checks that each region's configured commission rate matches the target r₀ for its trial group and phase (flagging deviations > 0.5% in red); H-2 checks non-trial-region supplier pricing for anomalies, computing an implied commission rate and flagging daily swings > 5%.

**Why this priority**: Configuration errors directly corrupt the trial's validity. A wrong r₀ or mislabeled participation type invalidates the experiment for affected regions. Must be caught before each phase starts.

**Independent Test**: After a pipeline run, open Tab 2 and verify the commission rate deviation table correctly flags a known misconfiguration; verify the pricing anomaly chart shows the 3 SKUs' implied rate trends.

**Acceptance Scenarios**:

1. **Given** a region where the configured commission rate (from conf_线上商品区域抽佣率调整) differs from the target r₀ (from conf_试验周期抽佣率 by trial_group and current stage) by more than 0.5%, **When** H-1 renders, **Then** that row is highlighted red with the deviation value displayed.
2. **Given** a trial city whose participation type (参与试验类型) is "非试验区域" instead of "试验区域", **When** H-1 renders, **Then** a red alert is shown for that region.
3. **Given** a region with region_type = "代理人" whose configured rate is approximately 7.5% (the 自营 target), **When** H-1 renders, **Then** a red alert flags the cross-type misconfiguration.
4. **Given** conf_商品信息 data for the 3 SKUs, **When** H-2 renders, **Then** a trend chart shows each SKU's non-trial-region implied commission rate (computed as (platform_price - supply_price) / platform_price) over time; days where the rate swings > 5% from the prior day are highlighted yellow.
5. **Given** a product row where 是否当日上架 = true, **When** H-2 renders, **Then** that date is visually highlighted as a new listing marker.

---

### User Story 4 - Tab 3: Normalization Progress (Priority: P1)

An operations analyst opens Tab 3 "归一化进度" during the 归一化预备期 and 摸底期 to monitor whether each city's actual commission rate (r₀) has converged to its target. The tab shows a daily r₀ line chart for all 8 cities (split by region_type: 自营 target 7.5%, 代理人 target 4.6%), a deviation bar chart (commission_rate - target_r₀, red when |deviation| > 1%), and a summary table showing each city's current status (达标/偏高/偏低).

**Why this priority**: r₀ normalization is a prerequisite for valid trial results. If normalization fails, the entire trial design is compromised. Daily monitoring enables timely intervention.

**Independent Test**: After a pipeline run with normalization period data, open Tab 3 and verify the r₀ line chart shows per-city trends with target overlay lines, and the deviation chart correctly flags cities exceeding ±1%.

**Acceptance Scenarios**:

1. **Given** fact data aggregated at 日期 × city_unit × region_type granularity, **When** Tab 3 renders, **Then** a multi-line chart shows daily r₀ for each city × region_type combination, with horizontal reference lines at 7.5% (自营) and 4.6% (代理人).
2. **Given** a city × region_type where |commission_rate - target_r₀| > 1%, **When** the deviation bar chart renders, **Then** that bar is colored red.
3. **Given** the summary table, **When** it renders, **Then** each city shows its latest r₀, target r₀, deviation, and a status label: 达标 (|deviation| ≤ 1%), 偏高 (> +1%), or 偏低 (< -1%).

---

### User Story 5 - Tab 4: Effect Analysis (Priority: P1)

An analyst navigates to Tab 4 "效应分析" after the 摸底期 ends to examine the trial's impact. The tab contains 4 sub-views switchable via radio buttons or sub-tabs: (B) City Unit Trends — 8 cities × 3 SKUs, showing 摸底期 as a single baseline point and 生效期 by stage_week, with incomplete weeks marked ⚠️; (C) Group Aggregation — G0/G1/G2/G3 group-level mean trends for commission_amount, gmv, and order_count; (D) SKU Comparison — 3 SKUs side-by-side within the same group; (E) Origin Comparison — 云南 (10184690+20519020 combined) vs 广西 (20588413) on commission_amount and commission_rate.

**Why this priority**: Effect analysis is the primary analytical deliverable — it answers the core business question of whether the commission rate changes had measurable impact.

**Independent Test**: After 摸底期 data is available, verify sub-view B shows baseline points; after 生效期 W1 data, verify weekly trend points appear with correct trading_days labels.

**Acceptance Scenarios**:

1. **Given** 摸底期 data (aggregated as a single block per city_unit × sku_id), **When** sub-view B renders, **Then** each city shows one baseline data point per SKU with trading_days annotation; stage_week is NULL.
2. **Given** 生效期 data with stage_week = "生效期_W1" where W1 covers only 4 days (is_complete_week = false), **When** sub-view B renders, **Then** the data point shows ⚠️ with "4/7天" annotation.
3. **Given** sub-view C (group aggregation), **When** it renders, **Then** 4 lines (G0~G3) are plotted for the selected metric (switchable between commission_amount, gmv, order_count); values are group-level means.
4. **Given** sub-view D (SKU comparison), **When** it renders, **Then** 3 SKUs are shown side-by-side within each trial group as bar charts with numerical annotations.
5. **Given** sub-view E (origin comparison), **When** it renders, **Then** 云南 origin aggregates SKU 10184690 + 20519020; 广西 origin uses SKU 20588413; commission_amount and commission_rate are compared.

---

### User Story 6 - Tab 5: Guardrail Alerts (Priority: P1)

A trial lead checks Tab 5 "护栏预警" weekly during the 生效期 to detect abnormal order volume or store count drops. The tab shows a city × SKU alert status table with three-color coding (🟢 GREEN / 🟡 YELLOW / 🔴 RED) based on week-over-week changes in order_count and active_store_count. Incomplete weeks are grayed out and excluded from alert calculations. A stockout trend line chart tracks supply disruptions. Alert thresholds are displayed at page bottom for reference.

**Why this priority**: Guardrail metrics protect against unintended harm — a severe order drop or store exodus could indicate the commission changes are damaging the business. Early detection enables rollback.

**Independent Test**: After 生效期 W2 data is available, verify the wow (week-over-week) calculations are correct and alert levels match the defined thresholds.

**Acceptance Scenarios**:

1. **Given** 生效期 data for W2 with order_count dropping 12% from W1 for a trial group city, **When** Tab 5 renders, **Then** that row shows 🔴 RED (threshold: order_count wow < -15% for trial group = RED; < -10% = YELLOW).
2. **Given** a stage_week where is_complete_week = false, **When** Tab 5 renders, **Then** that week's wow values are grayed out and no alert is triggered regardless of the magnitude.
3. **Given** active_store_count dropping 7% week-over-week, **When** Tab 5 renders, **Then** 🟡 YELLOW is shown (threshold: < -5% = YELLOW, < -10% = RED).
4. **Given** the stockout trend chart, **When** stockout_num (下单数量 - 送达数量, positive differences only) increases significantly during 摸底期, **Then** the trend line is visible and annotated.
5. **Given** the page bottom, **When** it renders, **Then** the alert threshold table is displayed showing all stage-specific thresholds from DP-003 (归一化预备期, 摸底期, 生效期 × metrics).

---

### User Story 7 - Statistical Power Analysis (Priority: P2)

A data scientist runs the power analysis CLI command to compute σ (GMV coefficient of variation) and ρ (intra-class correlation) for each of the 3 SKUs, then verify whether the trial design (2 city units per group) achieves ≥ 80% statistical power. The CLI reads historical baseline data from SQLite (4 complete natural weeks: 2026-04-13~04-26 and 2026-05-11~05-24), computes per-SKU σ_adjusted and ρ_main, applies the power formula, and writes results to SQLite. The Streamlit dashboard's dedicated analysis page reads these results and displays the σ/ρ tables, power verification conclusions, and SKU cross-correlation analysis — each with interpretive text conclusions (e.g., "功效充足 ✅" or "功效不足，建议调整 MDE").

**Why this priority**: Power analysis validates whether the trial can detect meaningful effects. Without it, the trial may produce inconclusive results. It is a prerequisite analysis but runs once (not daily), hence P2.

**Independent Test**: Run the power analysis CLI, verify SQLite contains the results table; open the dashboard's power analysis page and verify the displayed σ_raw for SKU 10184690 is close to the reference value 0.194.

**Acceptance Scenarios**:

1. **Given** historical fact data covering 4 complete natural weeks for all 3 SKUs across 8 city units, **When** the power analysis CLI runs, **Then** it computes σ_raw, σ_adjusted (= σ_raw × 1.5), ρ_pre, ρ_post, ρ_main (= min(ρ_pre, ρ_post)) per SKU, and writes results to SQLite.
2. **Given** the power formula with parameters (z_α/2=1.96, z_β=0.84, δ=0.10, n_actual=2), **When** the CLI computes n_required per SKU, **Then** results include n_required, n_actual, and a pass/fail conclusion (n_required ≤ 2 = sufficient power).
3. **Given** the SKU cross-correlation analysis, **When** any pair's Pearson correlation > 0.5, **Then** the output flags "有效样本量存在打折风险".
4. **Given** the dashboard's power analysis page, **When** it loads, **Then** it displays: σ/ρ result tables, power verification table with conclusions, SKU cross-correlation matrix, and interpretive text for each metric.
5. **Given** a SKU with insufficient historical data (< 3 complete weeks of non-zero GMV per city), **When** the power analysis runs, **Then** it falls back to using 归一化预备期 + 摸底期 combined data, and the output notes the fallback with a confidence caveat.

---

### Edge Cases

- What happens when a Lark table returns zero rows for the given date filter? The SQLite table is created but empty; the dashboard shows empty-state messaging for affected views.
- What happens when the MaxCompute SQL returns no rows for the configured date range? The fact table is empty; aggregation wide table is empty; all dashboard analysis views show "no data" states.
- What happens when two pipeline runs execute concurrently targeting the same SQLite file? SQLite file locking may cause one run to fail — concurrent runs are not supported.
- What happens when a city_unit has zero transactions in a given stage_week? The aggregation produces no row for that combination; the dashboard chart shows a gap, and trading_days reflects the actual count.
- What happens when 摸底期 data includes the Dragon Boat Festival (2026-06-19~21)? trading_days excludes these holidays, correctly reflecting the effective trading day count (~7 days out of 10).
- What happens when conf_试验周期抽佣率 has no date range covering today? The current stage cannot be determined; Tab 1 shows "阶段未配置" and Tab 3/4/5 show appropriate warnings.
- What happens when a Lark API call fails during extraction? The pipeline terminates immediately with an error identifying the failed table; SQLite is not partially updated.
- What happens when the power analysis finds a SKU with fewer than 3 complete weeks of non-zero GMV? It falls back to 预备期+摸底期 combined data and annotates the result with a confidence caveat.

## Requirements *(mandatory)*

### Functional Requirements

**Data Extraction**

- **FR-001**: System MUST extract data from 5 Lark configuration tables within a single wiki-hosted Lark base (wiki URL: https://bggc.feishu.cn/wiki/TcALwGgnciCQQYkPeHYcYf1Cnkd?table=tblevDYqsTdwu8fo&view=vewJatIahv): conf_商品信息, conf_区县信息, conf_线上商品区域抽佣率调整, conf_试验分组配置, conf_试验周期抽佣率.
- **FR-002**: System MUST apply per-source date filters: conf_区县信息 filtered to 日期=target_date; conf_线上商品区域抽佣率调整 filtered to 日期>=2026-06-19; conf_商品信息, conf_试验分组配置, conf_试验周期抽佣率 extracted without date filtering.
- **FR-003**: System MUST execute the order_fact.sql MaxCompute query from the module's sql/ directory, with parameterized date substitution.
- **FR-004**: All business parameters (table names, field names, SQL file paths, date filter rules, trial phase dates, Dragon Boat Festival dates, historical baseline ranges) MUST be centralized in config.py; no hard-coded values in processing or dashboard logic.
- **FR-005**: System MUST use existing framework components (LarkExtractor, LarkSourceConfig, SQLQueryConfig, MCExtractor) for data extraction — no direct API calls.
- **FR-006**: All date columns MUST be converted to datetime.date at the extraction stage per the project constitution.

**Data Storage**

- **FR-007**: System MUST store all extracted data (5 Lark tables + MaxCompute fact data) into a single SQLite database, one SQLite table per source.
- **FR-008**: System MUST compute a core aggregation wide table by joining fact data with configuration tables and write it as an additional SQLite table.
- **FR-009**: On each pipeline run, all SQLite tables MUST be fully overwritten (dropped and recreated) — no stale data persists.
- **FR-010**: The target date MUST be configurable via `--date` CLI parameter, defaulting to today.
- **FR-011**: The SQLite database file path MUST be configurable via `--db-path` CLI parameter, defaulting to module-local data/ directory.

**Core Aggregation Wide Table**

- **FR-012**: The wide table MUST contain the following dimension fields: stage (归一化预备期/摸底期/生效期), stage_week (生效期_W{N}, NULL for other stages), is_complete_week (BOOLEAN, 生效期 only), trading_days (INT), city_unit (merged city name), region_type (自营/代理人), trial_group (G0/G1/G2/G3), sku_id, sku_origin (产地), sku_grade, sku_weight_spec.
- **FR-013**: The wide table MUST contain the following measure fields: order_count (COUNT DISTINCT 明细订单id), active_store_count (COUNT DISTINCT 店铺id, cross-SKU deduplicated), gmv (SUM 送达金额), commission_amount (SUM 送达抽佣金额), stockout_num (SUM of positive 下单数量-送达数量 differences), commission_rate (commission_amount / gmv), supply_price (AVG of 平台销售斤单价 × 净重 × (1 - commission_rate)), target_r0 (from conf_试验周期抽佣率 by group and stage).
- **FR-014**: Aggregation granularity MUST vary by stage: 归一化预备期 = stage × city_unit × region_type (daily); 摸底期 = stage × city_unit × sku_id (overall, no weekly split); 生效期 = stage_week × city_unit × sku_id (weekly).
- **FR-015**: City unit merging MUST combine multiple same-city regions into one city_unit (e.g., 萍乡市 sub-regions merge to "萍乡市"); the merge mapping is derived from conf_试验分组配置.
- **FR-016**: Public filters MUST be applied to all fact-data-derived computations: 是否有效订单=1, 商品id IN (10184690, 20519020, 20588413), 参与试验类型="试验区域".
- **FR-017**: stage_week numbering MUST start at W1 from the 生效期 start date, rolling in 7-day increments; is_complete_week = true only when the week covers a full 7 days.
- **FR-018**: 摸底期 MUST be aggregated as a single block (no weekly split); trading_days MUST reflect actual trading days excluding Dragon Boat Festival (2026-06-19~21) holidays.
- **FR-019**: active_store_count MUST be computed with cross-SKU deduplication at the city_unit × aggregation-period level, output separately to avoid double-counting.

**Streamlit Dashboard — Tab 1: Trial Overview**

- **FR-020**: Tab 1 MUST display a current phase status card (derived from conf_试验周期抽佣率 date ranges), an 8-city grouping table (city_unit, region_type, trial_group, target r₀, SKU list), and a timeline of phase milestones including Dragon Boat Festival annotation.

**Streamlit Dashboard — Tab 2: Configuration Audit**

- **FR-021**: Tab 2 section H-1 MUST display a commission rate deviation table comparing configured rates (from conf_线上商品区域抽佣率调整) against target rates (from conf_试验周期抽佣率 by trial_group and stage), highlighting deviations > 0.5% in red.
- **FR-022**: Tab 2 H-1 MUST flag three alert conditions: commission rate deviation > 0.5% (RED), trial city with participate_type = "非试验区域" (RED), 代理人 region configured at ~7.5% rate (RED).
- **FR-023**: Tab 2 H-1 MUST display the target configuration reference table showing target r₀ for each phase × region_type × trial_group combination.
- **FR-024**: Tab 2 section H-2 MUST display a non-trial-region supplier price trend chart for 3 SKUs, computing implied commission rate ((platform_price - supply_price) / platform_price) and flagging daily swings > 5% in yellow.
- **FR-025**: Tab 2 H-2 MUST highlight dates where 是否当日上架 = true as new listing markers.

**Streamlit Dashboard — Tab 3: Normalization Progress**

- **FR-026**: Tab 3 MUST display a daily r₀ line chart for 8 cities × region_type, with horizontal target reference lines (自营 7.5%, 代理人 4.6%).
- **FR-027**: Tab 3 MUST display a deviation bar chart (commission_rate - target_r₀), coloring bars red when |deviation| > 1%.
- **FR-028**: Tab 3 MUST display a summary table with each city's latest r₀, target r₀, deviation, and status label (达标/偏高/偏低).

**Streamlit Dashboard — Tab 4: Effect Analysis**

- **FR-029**: Tab 4 MUST provide 4 switchable sub-views: City Trends (B), Group Aggregation (C), SKU Comparison (D), Origin Comparison (E).
- **FR-030**: Sub-view B MUST show 摸底期 as a single baseline point per city × SKU, and 生效期 by stage_week; incomplete weeks MUST be marked with ⚠️ and display trading_days as "N/7天".
- **FR-031**: Sub-view C MUST show G0/G1/G2/G3 group-level mean trends for commission_amount, gmv, and order_count (switchable metric selector).
- **FR-032**: Sub-view D MUST show 3 SKUs side-by-side within each trial group as bar charts with numerical annotations.
- **FR-033**: Sub-view E MUST aggregate 云南 origin as SKU 10184690 + 20519020 sum; 广西 origin as SKU 20588413; compare commission_amount and commission_rate.

**Streamlit Dashboard — Tab 5: Guardrail Alerts**

- **FR-034**: Tab 5 MUST display a city × SKU alert status table with three-color coding: 🟢 GREEN / 🟡 YELLOW / 🔴 RED based on week-over-week changes in order_count and active_store_count.
- **FR-035**: Alert thresholds MUST be stage-specific: 生效期 order_count wow < -10% = YELLOW, < -15% = RED; active_store_count wow < -5% = YELLOW, < -10% = RED (all thresholds per DP-003 视图 F).
- **FR-036**: Incomplete weeks (is_complete_week = false) MUST be grayed out and excluded from wow calculations and alert triggers.
- **FR-037**: Tab 5 MUST display a stockout_num trend line chart and a fixed alert threshold reference table at page bottom.

**Statistical Power Analysis**

- **FR-038**: System MUST provide a CLI command to compute per-SKU σ (GMV coefficient of variation) and ρ (intra-class Pearson correlation) using historical baseline data (4 natural weeks: 2026-04-13~04-26 and 2026-05-11~05-24) or fallback data (归一化预备期 + 摸底期 combined).
- **FR-039**: σ computation MUST follow: per-city CV = σ_city / μ_city across 4 weeks; σ_raw = mean of 8 city CVs; σ_adjusted = σ_raw × 1.5.
- **FR-040**: ρ computation MUST follow: ρ_pre = Pearson(W1 city GMV vector, W2 city GMV vector); ρ_post = Pearson(W3, W4); ρ_main = min(ρ_pre, ρ_post).
- **FR-041**: Power verification MUST compute n_required = 4 × σ_adjusted² × (1 - ρ_main) × 7.84 / 0.01 and compare against n_actual = 2; result is pass/fail per SKU.
- **FR-042**: System MUST compute SKU cross-correlation (3 pairs) and flag any pair with ρ > 0.5 as "有效样本量存在打折风险".
- **FR-043**: Power analysis results MUST be written to SQLite and displayed on a dedicated Streamlit dashboard page with result tables, conclusions, and interpretive text.
- **FR-044**: When a SKU has < 3 complete weeks of non-zero GMV in historical data, the system MUST fall back to 预备期+摸底期 data and annotate the result with a confidence caveat.

**General**

- **FR-045**: When any Lark API call fails during extraction, the pipeline MUST terminate immediately with an error identifying the failed table; no retry logic.
- **FR-046**: The dashboard MUST display a clear error message when the SQLite database is missing, instructing the user to run the pipeline first.
- **FR-047**: The dashboard MUST NOT write any data back to Lark or any other external system; it is read-only.
- **FR-048**: The dashboard MUST be launchable via `streamlit run` command.

### Key Entities

- **Fact Transaction (事实交易表)**: Individual order-line records from MaxCompute (order_fact.sql). Key attributes: 日期, 订单id, 明细订单id, 商品id, 商品名称, 商家名称, 实际抽佣率, 平台销售件单价, 平台销售斤单价, 商家供货件单价, 商家供货斤单价, 店铺id, 省/市/区县 id+name, 网格id+name, 下单数量/金额/重量, 送达数量/金额/重量/运费/抽佣金额, 是否有效订单.
- **Product (商品信息)**: SKU dimension from conf_商品信息. Key attributes: 商品id, 商品名称, 产地 (云南/广西), 包装类型, 单果大小, 色号, 商品头数, non-trial prices, 是否当日上架.
- **County Region (区县信息)**: Geographic area from conf_区县信息. Attributes: 区县id, 区县名称, 市id, 市名称, 省id, 省名称, 运营类型.
- **Commission Adjustment (抽佣率调整)**: Per-product per-region config from conf_线上商品区域抽佣率调整. Attributes: 日期, 商品id, 区县名称, 区域全称, 调整系数, 固定抽佣率调整, 固定抽佣金额调整, 参与试验类型.
- **Trial Group Config (试验分组配置)**: Trial design from conf_试验分组配置. Attributes: 区域id, 区域类型 (自营/代理人), 试验分组 (G0/G1/G2/G3), 试验起始日期, 试验结束日期. Used for city_unit merging and group labeling.
- **Trial Period Commission Rate (试验周期抽佣率)**: Phase-level commission targets from conf_试验周期抽佣率. Attributes: 试验阶段 (运营阶段), 运营类型, 抽佣率, 试验分组, 试验起始日期, 试验结束日期. Used for stage derivation and target_r0.
- **Aggregation Wide Table (核心聚合宽表)**: Computed table joining fact data with all configuration dimensions. Contains stage, stage_week, is_complete_week, trading_days, city_unit, region_type, trial_group, sku_id, sku_origin, sku_grade, sku_weight_spec, order_count, active_store_count, gmv, commission_amount, stockout_num, commission_rate, supply_price, target_r0.
- **Power Analysis Result**: Per-SKU statistical power metrics: σ_raw, σ_adjusted, ρ_pre, ρ_post, ρ_main, n_required, n_actual, power conclusion, SKU cross-correlation matrix.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A full pipeline run (5 Lark extractions + MaxCompute SQL + aggregation wide table computation + SQLite write) completes within 5 minutes for typical data volumes.
- **SC-002**: The SQLite database contains all configured source tables plus the aggregation wide table, with row counts matching source query results.
- **SC-003**: The dashboard loads and displays Tab 1 within 5 seconds of startup (assuming populated SQLite on local storage).
- **SC-004**: All 5 dashboard tabs render correctly with representative data — each tab's primary visualization is present and interactive.
- **SC-005**: Configuration audit (Tab 2) correctly identifies commission rate deviations > 0.5% and participation type mismatches, verified by spot-checking against source data.
- **SC-006**: Normalization progress (Tab 3) correctly displays daily r₀ trends for all 8 cities with target reference lines; deviation alerts trigger at ±1%.
- **SC-007**: Effect analysis (Tab 4) displays 摸底期 as single baseline points and 生效期 by stage_week; incomplete weeks are correctly marked and annotated.
- **SC-008**: Guardrail alerts (Tab 5) correctly compute week-over-week changes and assign alert levels per the defined thresholds; incomplete weeks are excluded from alerts.
- **SC-009**: Power analysis CLI produces σ_raw for SKU 10184690 close to the historical reference value of 0.194; ρ_main close to 0.993.
- **SC-010**: Power analysis dashboard page displays result tables with interpretive conclusions for each SKU.
- **SC-011**: All 8 city units are present in the aggregation wide table with correct trial_group assignments matching conf_试验分组配置.
- **SC-012**: Public filters (有效订单=1, 3 SKUs only, 试验区域 only) are correctly applied to all fact-data-derived computations.
- **SC-013**: The pipeline produces identical output for the same input data and target date regardless of execution environment (deterministic).

## Assumptions

- All 5 Lark configuration tables reside within a single wiki-hosted Lark base accessible via the provided wiki URL; the existing LarkExtractor framework supports wiki-hosted bases without modification.
- The order_fact.sql MaxCompute query is already developed and tested; it returns transaction-level data with all fields needed for the aggregation wide table.
- The 3 trial SKUs (10184690, 20519020, 20588413) are all "水仙芒" products with consistent field structures in both Lark config tables and MaxCompute fact data.
- City unit merging: conf_试验分组配置 contains sufficient information (区域id → city mapping) to derive city_unit; 萍乡市 sub-regions are explicitly handled.
- 8 trial cities exist with 2 region_types each (自营 + 代理人), yielding 16 city × region_type combinations for normalization monitoring.
- Trial groups G0~G3 have 2 city units each; G0 is the control group with no commission rate increase during 生效期.
- Dragon Boat Festival dates (2026-06-19 ~ 2026-06-21) are hardcoded in config.py; if holiday dates change in future years, config.py is updated.
- 摸底期 lasts 10 calendar days; effective trading days exclude Dragon Boat Festival (approximately 7 trading days).
- Historical baseline data (2026-04-13~04-26 and 2026-05-11~05-24) is available in MaxCompute for all 3 SKUs across all 8 city units. If not available for certain SKUs, the fallback power analysis method (using 预备期+摸底期 data) is used.
- Commission rate fields use consistent decimal formats (e.g., 0.075 for 7.5%) across all tables, enabling direct comparison.
- The Streamlit dashboard is used by internal analysts on local machines; no authentication is required.
- The pipeline is run by a single user at a time; concurrent runs are not supported.
- The dashboard reads from SQLite in read-only mode and does not modify the database.

## Scope Boundaries

### In Scope

- Extracting 5 Lark configuration tables with per-source date filtering
- Executing order_fact.sql MaxCompute query with date parameterization
- Computing core aggregation wide table (joining fact data with config tables, city_unit merging, stage/stage_week assignment)
- SQLite storage with full overwrite per run
- CLI with `--date` and `--db-path` parameters
- 5-tab Streamlit dashboard: Trial Overview, Configuration Audit, Normalization Progress, Effect Analysis, Guardrail Alerts
- Statistical power analysis (CLI + dashboard page with conclusions)
- Public filters (valid orders, 3 SKUs, trial regions only)
- Alert logic with stage-specific thresholds
- Dragon Boat Festival handling (hardcoded 2026-06-19~21)
- Dashboard launch via `streamlit run`

### Out of Scope

- Writing any data back to Lark, MaxCompute, or any other external system
- Scheduling or automated triggering (handled externally by cron/scheduler)
- Modification of source configuration tables in Lark
- Authentication or access control for the dashboard
- Real-time or streaming data updates (dashboard reflects last pipeline run only)
- Deployment as a hosted web service (local execution only)
- Historical trend analysis across multiple pipeline runs
- DID (Difference-in-Differences) formal statistical analysis — the dashboard provides effect preview data; formal DID is done in separate analysis scripts
- Developing or modifying the order_fact.sql query (already completed)
- conf_线上商品试验区域价格表 extraction (not required by DP-003; trial region prices come from fact data)
