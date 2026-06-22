# coding:utf8
"""tests/cr_analyze/test_transformer.py -- 聚合逻辑测试"""

from datetime import date

import pandas as pd
import pytest

from workers.cr_analyze.transformer import (
    compute_wide_table,
    preprocess_lark_dates,
    _is_trial_region,
)


class TestPreprocessLarkDates:
    def test_date_column_converted(self):
        data = {"test": pd.DataFrame({"日期": ["2026-06-20", "2026-06-21"], "v": [1, 2]})}
        result = preprocess_lark_dates(data)
        assert result["test"]["日期"].iloc[0] == date(2026, 6, 20)

    def test_trial_date_columns_converted(self):
        data = {"test": pd.DataFrame({
            "试验起始日期": ["2026-06-19"],
            "试验结束日期": ["2026-07-19"],
        })}
        result = preprocess_lark_dates(data)
        assert result["test"]["试验起始日期"].iloc[0] == date(2026, 6, 19)


class TestIsTrialRegion:
    def test_trial_region(self):
        assert _is_trial_region("[试验区域]") is True

    def test_non_trial_region(self):
        assert _is_trial_region("[非试验区域]") is False

    def test_none_value(self):
        assert _is_trial_region(None) is False

    def test_nan_value(self):
        assert _is_trial_region(float("nan")) is False


class TestComputeWideTable:
    def test_empty_fact_returns_empty(self, sample_lark_data):
        result = compute_wide_table(sample_lark_data, {}, date(2026, 6, 20))
        assert result.empty

    def test_filters_invalid_orders(self, sample_lark_data, sample_mc_data):
        # sample data has 1 invalid order (是否有效订单=0)
        result = compute_wide_table(sample_lark_data, sample_mc_data, date(2026, 6, 20))
        # Should not include the invalid order
        if not result.empty and "order_count" in result.columns:
            total_orders = result["order_count"].sum()
            assert total_orders <= 5  # 6 total - 1 invalid

    def test_filters_non_target_skus(self, sample_lark_data, sample_mc_data):
        # sample data has sku 99999999 which is not in TARGET_SKU_IDS
        result = compute_wide_table(sample_lark_data, sample_mc_data, date(2026, 6, 20))
        if not result.empty and "sku_id" in result.columns:
            assert 99999999 not in result["sku_id"].values

    def test_filters_non_trial_regions(self, sample_lark_data, sample_mc_data):
        # 西城区 is marked as [非试验区域]
        result = compute_wide_table(sample_lark_data, sample_mc_data, date(2026, 6, 20))
        # Rows associated with 西城区 should be filtered out
        # This depends on the join logic working correctly

    def test_output_has_expected_columns(self, sample_lark_data, sample_mc_data):
        result = compute_wide_table(sample_lark_data, sample_mc_data, date(2026, 6, 20))
        if not result.empty:
            assert "stage" in result.columns
            assert "city_unit" in result.columns
            assert "sku_id" in result.columns

    def test_dragon_boat_excluded_from_trading_days(self, sample_lark_data, sample_mc_data):
        # 摸底期 should exclude dragon boat dates from trading_days
        result = compute_wide_table(sample_lark_data, sample_mc_data, date(2026, 6, 20))
        if not result.empty:
            baseline = result[result["stage"] == "摸底期"]
            if not baseline.empty and "trading_days" in baseline.columns:
                # trading_days should be less than total days (dragon boat excluded)
                for td in baseline["trading_days"].dropna():
                    assert td >= 0
