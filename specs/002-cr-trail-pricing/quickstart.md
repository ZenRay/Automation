# Quickstart: Trial Region Commission Pricing Generator

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)
**Created**: 2026-06-18

## Prerequisites

```bash
pyenv local 3.12.13
uv venv .venv --python 3.12 && source .venv/bin/activate
uv pip install -e .
```

Ensure Lark API credentials are configured in `automation/conf/lark.ini` (prod section).

## Running the Pipeline

```bash
# Default: target date = today, output = cr_trail_pricing_{today}.xlsx
python -m workers.cr_trail_pricing.main

# Specific date
python -m workers.cr_trail_pricing.main --date 2026-06-18

# Custom output path
python -m workers.cr_trail_pricing.main --date 2026-06-18 --output /tmp/pricing_plan.xlsx
```

### Expected Console Output

```
[Stage 1/7] extract_sources: 6 tables extracted, {elapsed}s
  conf_county: 5000 rows
  conf_goods: 200 rows
  conf_trial_group: 15 rows
  conf_trial_commission: 10 rows
  conf_trial_goods: 8 rows
  conf_hidden_logistics: 350 rows
[Stage 2/7] filter_trial_products: 5 rows, {elapsed}s
[Stage 3/7] mark_trial_regions: 5000 rows, {elapsed}s
[Stage 4/7] associate_commission: 5000 rows, {elapsed}s
[Stage 5/7] associate_logistics_fee: 5000 rows, {elapsed}s
[Stage 6/7] compute_pricing: 25000 rows, {elapsed}s
[Stage 7/7] export_excel: /path/to/output.xlsx, {elapsed}s
```

## Running Tests

```bash
# Run all tests for this module
pytest workers/cr_trail_pricing/tests/ -v

# Run specific test
pytest workers/cr_trail_pricing/tests/test_pipeline.py::test_full_pipeline -v
```

## Validation Scenarios

### V1: Full Pipeline with Live Data

1. Run the pipeline with today's date
2. Open the output Excel file
3. Verify:
   - Column headers match: 区域id, 区域名称, 商品SKU, 商品名称, 调价方向, 调价幅度, 设置状态, 固定抽佣比例, 固定抽佣货值, 商品id, 四级类目名称
   - Row count ≈ (trial products) × (all regions)
   - Trial region rows have `固定抽佣比例` populated, `固定抽佣货值` empty
   - Non-trial region rows have `固定抽佣货值` populated, `固定抽佣比例` empty
   - All rows have `设置状态` = "启用"
   - `调价方向` is one of: "涨价", "降价", "不变"

### V2: Stage-by-Stage Inspection

Each stage function can be called independently for debugging:

```python
from workers.cr_trail_pricing.transformer import (
    filter_trial_products,
    mark_trial_regions,
    associate_commission,
    associate_logistics_fee,
    compute_pricing,
)
from datetime import date

# After extracting data via Stage 1...
products_df = filter_trial_products(lark_data["conf_goods"], lark_data["conf_trial_goods"], date.today())
regions_df = mark_trial_regions(lark_data["conf_county"], lark_data["conf_trial_group"], date.today())
# ... inspect intermediate DataFrames at each stage
```

### V3: Empty Result Handling

Run with a date that has no active trial:

```bash
python -m workers.cr_trail_pricing.main --date 2020-01-01
```

Expected: Excel file with column headers only (no data rows), warning logged.
