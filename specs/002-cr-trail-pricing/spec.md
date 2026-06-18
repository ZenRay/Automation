# Feature Specification: Trial Region Commission Pricing Generator

**Feature Branch**: `002-cr-trail-pricing`

**Created**: 2026-06-18

**Status**: Draft

**Input**: User description: "为 Automation 项目 workers 模块开发一个「试验区域抽佣调价方案生成器」ETL 功能，从飞书多维表格读取 6 张配置表，经过 7 阶段数据流水线处理，输出 Excel 调价方案"

## Clarifications

### Session 2026-06-18

- Q: Expected data volume per run? → A: Fewer than 10 trial products x ~5,000 county-level regions, producing up to ~50,000 rows in the Cartesian product output.
- Q: Lark API failure behavior during extraction? → A: Fail immediately with a clear error message identifying which table failed; no retries.
- Q: Pipeline execution observability? → A: Log each stage completion with stage name, row count, and elapsed time to stdout.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Daily Pricing Plan Generation (Priority: P1)

A pricing operations analyst runs the pricing generator each morning to produce the day's trial region commission adjustment plan. The system reads the latest configuration from 6 Lark multi-dimensional tables (county info, product info, trial group config, trial period commission rates, trial product info, online hidden logistics fees), processes them through a 7-stage pipeline, and outputs an Excel file listing every SKU-region combination with its pricing direction, adjustment amount, and commission parameters.

**Why this priority**: This is the core deliverable — without a generated pricing plan, no trial region pricing adjustments can be applied. All other stories depend on this.

**Independent Test**: Run the generator with `--date` set to a known test date, verify the output Excel contains all expected SKU-region combinations with correctly calculated pricing fields.

**Acceptance Scenarios**:

1. **Given** 6 valid configuration tables in Lark, **When** the generator runs with a date parameter, **Then** an Excel file is produced containing one row per product-region combination with all required pricing fields.
2. **Given** the generator runs without a `--date` parameter, **When** it executes, **Then** it defaults to today's date and produces the current day's pricing plan.
3. **Given** the generator runs with an `--output` parameter specifying a file path, **When** it completes, **Then** the Excel file is written to the specified path.
4. **Given** a date where no trial is active (no matching trial period or trial products), **When** the generator runs, **Then** it produces an empty result set with appropriate headers and logs a warning.

---

### User Story 2 - Trial Product Filtering (Priority: P2)

The system identifies which products participate in the trial on the given date by joining the trial product daily snapshot (conf_试验商品信息, filtered to target_date) with the product master list (conf_商品信息, filtered to target_date-1). The join is performed on product ID only (the two tables use different dates). Products that appear in the trial product snapshot are included in the pricing plan, with their non-trial commission rate sourced from the trial product table (not the product master).

**Why this priority**: Product filtering determines which SKUs appear in the output. Incorrect filtering leads to wrong pricing being applied.

**Independent Test**: Given known product and trial product tables, verify that only products present in the trial product snapshot for the target date AND existing in the product master list (previous day) appear in the intermediate result.

**Acceptance Scenarios**:

1. **Given** a product present in conf_试验商品信息 (日期=target_date) and also existing in conf_商品信息 (日期=target_date-1) with matching product ID, **When** filtering runs, **Then** the product is included with its category (后台类目名称), non-trial commission rate (from conf_试验商品信息), gross weight (from conf_商品信息), and 是否试验区域 set to 1.
2. **Given** a product present in conf_试验商品信息 (日期=target_date) but NOT existing in conf_商品信息 (日期=target_date-1), **When** filtering runs, **Then** the product is excluded (INNER JOIN drops unmatched rows).
3. **Given** a product existing in conf_商品信息 (日期=target_date-1) but NOT present in conf_试验商品信息 (日期=target_date), **When** filtering runs, **Then** the product is excluded (not a trial product today).

---

### User Story 3 - Trial Region Identification (Priority: P2)

The system identifies which geographic regions participate in the trial by joining county-level geographic data with trial group configuration. Each region is flagged as either a trial region or non-trial region, which determines the pricing calculation logic applied in later stages.

**Why this priority**: Region identification is equally critical as product filtering — both dimensions of the Cartesian product must be correct.

**Independent Test**: Given known county and trial group tables, verify that regions are correctly flagged as trial (1) or non-trial (0) based on the join conditions.

**Acceptance Scenarios**:

1. **Given** a county whose city ID matches a trial group configuration record with area type CITY and today within the trial date range, **When** region marking runs, **Then** the region is flagged as a trial region (1).
2. **Given** a county with no matching trial group configuration, **When** region marking runs, **Then** the region is flagged as a non-trial region (0).
3. **Given** a trial group configuration with area type other than CITY, **When** region marking runs, **Then** it is not used for matching (only CITY-level configurations apply).

---

### User Story 4 - Commission Rate and Hidden Logistics Fee Association (Priority: P2)

The system associates each region with its trial commission rate (for trial regions) and hidden logistics fee rate (for all regions). The hidden logistics fee supports county-level overrides via a JSON mapping, falling back to the city-level rate when no county-specific rate exists.

**Why this priority**: Correct fee association directly affects pricing calculation accuracy.

**Independent Test**: Given known region, commission rate, and logistics fee tables, verify that each region row carries the correct commission rate and logistics fee rate after association.

**Acceptance Scenarios**:

1. **Given** a trial region with a matching trial period commission rate record (matching trial type, trial group, and date within trial period), **When** commission association runs, **Then** the region row carries the trial commission rate.
2. **Given** a region with a matching hidden logistics fee record where the county fee mapping is empty, **When** logistics fee association runs, **Then** the city-level fee rate is used.
3. **Given** a region with a matching hidden logistics fee record where the county fee mapping JSON contains the region's county ID as a key, **When** logistics fee association runs, **Then** the county-specific rate from the JSON is used.
4. **Given** a region with a matching hidden logistics fee record where the county fee mapping JSON does NOT contain the region's county ID, **When** logistics fee association runs, **Then** the city-level fee rate is used as fallback.
5. **Given** a region with no matching hidden logistics fee record for the target date, **When** logistics fee association runs, **Then** the hidden logistics fee rate defaults to 0.

---

### User Story 5 - Pricing Calculation and Excel Export (Priority: P1)

The system computes pricing adjustments for every product-region combination (Cartesian product) and exports the result as an Excel file. Trial regions use commission rate differential to determine the adjustment; non-trial regions use hidden logistics cost applied to product weight. The output uses business-friendly column names.

**Why this priority**: This is the final output — equal priority to Story 1 since it IS the deliverable.

**Independent Test**: Given known product and region DataFrames with known values, verify the Cartesian product produces correct calculations for both trial and non-trial regions, and the Excel output matches expected content.

**Acceptance Scenarios**:

1. **Given** a product-region pair where the region is a trial region, **When** calculation runs, **Then** the fixed commission ratio equals the absolute difference between the non-trial commission rate and the trial commission rate, and pricing direction is determined by whether the trial rate is lower ("降价"), equal ("不变"), or higher ("涨价") than the non-trial rate.
2. **Given** a product-region pair where the region is NOT a trial region, **When** calculation runs, **Then** the fixed commission value equals the hidden logistics fee rate multiplied by the product's gross weight, and pricing direction is always "涨价".
3. **Given** the completed calculation result, **When** Excel export runs, **Then** column names are mapped to business terms (e.g., trial region ID becomes "区域id", product code becomes "商品SKU"), the adjustment field is left empty, the status field is set to "启用", and only the defined output columns are included.
4. **Given** each pipeline stage, **When** it runs, **Then** it produces an inspectable intermediate result (DataFrame) that can be independently validated before the next stage executes.

---

### Edge Cases

- What happens when a Lark table is empty or returns no records for the target date? The affected stage produces an empty DataFrame; downstream stages handle empty inputs gracefully and the final output is an Excel with headers only.
- What happens when the same product appears multiple times in conf_试验商品信息 for the same date? The join may produce duplicates — the system preserves all matching rows (no implicit deduplication).
- What happens when the county fee mapping JSON is malformed? The system treats it as empty and falls back to the city-level fee rate.
- What happens when a trial group configuration has multiple trial types for the same city? Each combination produces a separate row in the region data, which then generates separate product-region pairs in the Cartesian product.
- What happens when a Lark API call fails (network error, timeout, permission denied) during data extraction? The system fails immediately with a clear error message identifying which table could not be fetched. No retries are attempted; the operator or scheduler retries the entire run externally.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST extract data from 6 Lark multi-dimensional tables within a single Lark application (app_token: GmWEbfgCmatLAnsSyaocBisJnoe) as independent datasets.
- **FR-002**: System MUST filter products to only those that are (a) present in conf_试验商品信息 (日期=target_date) AND (b) present in conf_商品信息 (日期=target_date-1), joined on product ID.
- **FR-003**: System MUST preserve these product attributes through the pipeline: product ID (商品id), product code (商品编码), product name (商品名称), backend category name (后台类目名称), non-trial commission rate (非试验区域抽佣率, sourced from conf_试验商品信息), gross weight (毛重, sourced from conf_商品信息), and a trial region flag (是否试验区域, always set to 1).
- **FR-004**: System MUST identify trial regions by matching county-level geographic data against city-level trial group configurations, flagging each region as trial or non-trial.
- **FR-005**: System MUST associate trial commission rates to trial regions by matching trial group (试验分组), operation type (运营类型), and date range.
- **FR-006**: System MUST associate hidden logistics fee rates to all regions, with a county-level JSON override mechanism that falls back to city-level rates when no county-specific rate exists or when the mapping is absent/empty.
- **FR-007**: System MUST generate the Cartesian product of filtered products and marked regions, computing pricing parameters for each combination.
- **FR-008**: For trial regions, the fixed commission ratio MUST equal the absolute difference between the non-trial commission rate and the trial commission rate.
- **FR-009**: For trial regions, pricing direction MUST be determined by the signed difference (before absolute value): negative = "降价", zero = "不变", positive = "涨价".
- **FR-010**: For non-trial regions, the fixed commission value MUST equal the hidden logistics fee rate multiplied by the product gross weight, and pricing direction MUST always be "涨价".
- **FR-011**: The adjustment amount field MUST be left empty; the status field MUST be set to "启用" for all rows.
- **FR-012**: System MUST export the final result as an Excel file with business-friendly column names and a defined column order.
- **FR-013**: Each of the 7 pipeline stages MUST be independently executable and produce an inspectable intermediate result.
- **FR-014**: The target date MUST be configurable via a command-line parameter, defaulting to today's date.
- **FR-015**: The output file path MUST be configurable via a command-line parameter.
- **FR-016**: All business parameters (table IDs, field mappings, column names) MUST be centralized in the module's configuration file; no hard-coded values in processing logic.
- **FR-017**: System MUST use existing framework components (LarkExtractor, LarkTargetConfig) for data extraction — no direct API calls.
- **FR-018**: When any Lark API call fails during extraction, the system MUST terminate immediately with an error message identifying the failed table; no retry logic is implemented.
- **FR-019**: The system MUST log each pipeline stage's completion to stdout, including stage name, output row count, and elapsed time.

### Key Entities

- **Product**: An item available for sale, identified by product ID and product code. Carries attributes: name, backend category name (后台类目名称), non-trial commission rate, gross weight. Sourced from conf_商品信息 (filtered to target_date-1).
- **Trial Product**: A daily snapshot record from conf_试验商品信息 indicating which products participate in the trial on a given date. Contains product ID, date, and non-trial commission rate (the authoritative value used in pricing calculations).
- **County Region**: A geographic area at the county level, carrying city ID, province ID, and county ID attributes. Used as the regional dimension in pricing.
- **Trial Group Configuration**: Defines which cities participate in which trial groups and trial types, with associated trial date ranges. Filtered to CITY-level area type.
- **Trial Commission Rate**: The commission rate applicable to a specific trial group (试验分组) and operation type (运营类型, e.g., 自营区域/代理人区域) combination within a trial period.
- **Hidden Logistics Fee**: A city-level fee rate that affects pricing in non-trial regions. May carry a county-level override mapping (JSON structure keyed by county ID).
- **Pricing Plan Row**: A single product-region combination with computed pricing direction, fixed commission ratio (trial) or fixed commission value (non-trial), empty adjustment amount, and "启用" status.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The system generates a complete pricing plan Excel file (up to ~50,000 rows from ~10 products x ~5,000 regions) within 2 minutes of execution start.
- **SC-002**: The output Excel contains exactly the Cartesian product of filtered trial products and all marked regions — no missing or extra rows.
- **SC-003**: 100% of trial region rows carry a correctly calculated fixed commission ratio (verified by spot-checking against source data).
- **SC-004**: 100% of non-trial region rows carry a correctly calculated fixed commission value based on hidden logistics fee and product weight.
- **SC-005**: Pricing direction labels ("涨价", "降价", "不变") are correctly assigned for 100% of rows based on the defined business rules.
- **SC-006**: Each pipeline stage can be executed independently and its intermediate result inspected for correctness without running other stages.
- **SC-007**: The system produces identical output for the same input data regardless of execution environment (deterministic processing).
- **SC-008**: Running with no `--date` parameter produces a plan for the current date; running with `--output` writes to the specified path.
- **SC-009**: Console output during a successful run includes one log line per stage showing stage name, row count, and elapsed time — verified by inspecting stdout.

## Assumptions

- All 6 Lark configuration tables are maintained by business stakeholders and contain valid, up-to-date data.
- Product commission rates are expressed as decimal ratios (e.g., 0.05 for 5%) and are directly comparable between the non-trial rate field and the trial commission rate field.
- The hidden logistics fee rate and product gross weight use compatible units such that their product yields a meaningful monetary value for pricing.
- The county fee mapping JSON field, when non-empty, is a valid JSON object with county IDs as string keys and fee rates as numeric values.
- The trial group configuration uses CITY as the area type value for city-level trial assignments; other area types (e.g., district) exist but are not used for region identification.
- Trial type and trial group field names are consistent between the trial group configuration table and the trial commission rate table, enabling direct field matching.
- The existing LarkExtractor framework supports reading all 6 tables without modification.
- Multiple trial group configurations may exist for the same city (different trial types/groups), and each produces separate region rows that participate independently in the Cartesian product.
- The typical data volume per run is fewer than 10 trial products and approximately 5,000 county-level regions, yielding up to ~50,000 rows in the final output.

## Scope Boundaries

### In Scope

- Reading 6 configuration tables from a single Lark application
- 7-stage data processing pipeline (extract, filter products, mark regions, associate commissions, associate logistics fees, Cartesian product calculation, Excel export)
- Command-line interface for date and output path parameters
- Intermediate result inspection at each pipeline stage
- County-level logistics fee override via JSON mapping

### Out of Scope

- Writing results back to Lark or any other system (output is Excel file only)
- Scheduling or automated triggering of the generator (handled externally by cron/scheduler)
- Modification of source configuration tables in Lark
- Historical pricing comparison or trend analysis
- Multi-app_token support (single Lark application only)
- Real-time or streaming data processing
