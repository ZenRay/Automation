# Research: Trial Region Commission Pricing Generator

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)
**Created**: 2026-06-18

## R-001: Lark Source Date Filtering Strategy

**Decision**: Use API-level date filtering (`date_filter_field`) for sources where the filter is "exact date = target date" or "exact date = target_date - 1". For sources requiring "today within a date range" filtering, fetch all records and filter in pandas.

**Rationale**: The `LarkSourceConfig.date_filter_field` mechanism supports exact date matching via Lark's ExactDate filter. It does not natively support "target date falls within [start, end] range" matching. Updated filtering strategy:
- `conf_区县信息`: API-level exact date filter (`日期 = target_date`) — previously full lookup, changed for performance (3021 rows/day vs full history)
- `conf_商品信息`: API-level exact date filter (`日期 = target_date - 1`) — product data has 1-day lag
- `conf_试验商品信息`: API-level exact date filter (`日期 = target_date`) — previously planned as pandas range filter, but actual table only has `日期` field (no start/end dates)
- `conf_试验分组配置`, `conf_试验周期抽佣率`: pandas range filter (need "today within [start, end]" semantics)
- `conf_线上隐形物流费`: API-level exact date filter

**Alternatives considered**:
- Custom date filter logic in LarkExtractor: would require modifying `workers/lib/` (violates Principle I — Layer Isolation)
- Two-pass extraction (broad range then narrow): unnecessary complexity for small tables

## R-002: Excel Output vs Standard Lark Write-Back

**Decision**: Use `pd.DataFrame.to_excel()` directly in Stage 7. Do not use `LarkTargetConfig`, `DataRoute`, or `DataRouter`.

**Rationale**: The spec explicitly requires Excel output (FR-012). The framework's write-back infrastructure (`DataRouter`, `LarkTargetConfig`, `CleanupCondition`) is designed for Lark table writes. Adapting it for Excel would add complexity without benefit.

**Alternatives considered**:
- Adding an `ExcelTargetConfig` abstraction to `workers/lib/`: premature abstraction for a single use case; violates "don't add abstractions beyond what the task requires"
- Writing to Lark AND Excel: spec says Excel-only output (out of scope: "Writing results back to Lark")

## R-003: Pipeline Architecture — Stage Functions vs DataTransformer Steps

**Decision**: Implement 7 independent stage functions in `transformer.py` rather than registering steps with `DataTransformer`.

**Rationale**: The `DataTransformer.register_step()` pattern is designed for sequential transforms on a single DataFrame (e.g., clean → aggregate → format). This pipeline has multiple intermediate DataFrames flowing between stages (products, regions, cross product). Each stage takes different inputs and produces different outputs — it doesn't fit the single-DataFrame step pattern.

**Alternatives considered**:
- Using DataTransformer with 7 steps: each step would need access to different DataFrames, requiring global state or complex closures. Forces the pipeline into a pattern it doesn't match.
- Using DataRouter for internal data flow: overkill for in-memory DataFrame passing between functions

## R-004: Table IDs for Undiscovered Tables

**Decision**: Define table_ids as named constants in `config.py` using a `TABLE_IDS` dictionary. For the 4 tables whose table_ids were not provided in the spec (conf_试验分组配置, conf_试验周期抽佣率, conf_试验商品信息, conf_线上隐形物流费), use placeholder values that must be replaced before first run.

**Rationale**: The spec provides table_ids for only 2 of 6 tables. The remaining 4 are in the same Lark application (`GmWEbfgCmatLAnsSyaocBisJnoe`) and their table_ids can be discovered from the Lark API or the document URL. Centralizing them as named constants (Principle II) ensures they're easy to find and update.

**Alternatives considered**:
- Hardcoding in URL strings: violates Principle II (Configuration-Driven)
- Runtime table_id discovery via API: unnecessary complexity; table_ids are stable

## R-005: JSON County Fee Mapping Parsing

**Decision**: Parse the `区县费率映射` field as JSON once per row during Stage 5, with graceful fallback on malformed JSON (treat as empty mapping).

**Rationale**: The spec defines this field as a JSON object with county IDs as keys and fee rates as values. Malformed JSON should not crash the pipeline — it should fall back to the city-level rate (edge case documented in spec).

**Alternatives considered**:
- Pre-validating all JSON fields before processing: unnecessary overhead; try/except per row is simpler and handles the same cases
- Using `ast.literal_eval` instead of `json.loads`: less standard; JSON is the declared format

## R-007: Commission Rate Operation Type Join Strategy

**Decision**: Include `运营类型` (operation type) in the Stage 4 JOIN condition alongside `试验分组` (trial group).

**Rationale**: The `conf_试验周期抽佣率` table contains 2 records per trial group (one for "自营区域" and one for "代理人区域"). Joining on `试验分组` alone would produce a cross-product, doubling the row count for each region. Since `conf_区县信息` carries an `运营类型` field that matches the commission rate table's operation type, including it in the join ensures a 1:1 match per region.

**Alternatives considered**:
- Filtering commission rates to a single operation type before joining: loses data for regions with different operation types
- Post-join deduplication: wasteful and fragile; better to join correctly upfront

## R-006: Date Handling for Lark ExactDate Construction

**Decision**: Use `calendar.timegm(target_date.timetuple()) * 1000` for constructing Lark ExactDate timestamps, consistent with Constitution Principle III.

**Rationale**: Principle III explicitly requires UTC midnight construction to prevent timezone offset bugs. The target date is a `datetime.date` object (no time component), so `timetuple()` produces midnight local time which `timegm()` correctly converts to UTC milliseconds.

**Alternatives considered**: None — this is a NON-NEGOTIABLE constitutional requirement.
