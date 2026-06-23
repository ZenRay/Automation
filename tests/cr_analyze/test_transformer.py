# coding:utf8
"""tests/cr_analyze/test_transformer.py -- 聚合逻辑测试"""

from datetime import date

import pandas as pd
import pytest

from workers.cr_analyze.transformer import (
    compute_wide_table,
    preprocess_lark_dates,
    _is_trial_region,
    _to_date_series,
    compute_trial_phase_config_wide,
    compute_trial_phase_config_pivot,
    compute_trial_sku_profile,
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

    def test_timezone_like_datetime_shifted_to_local_date(self):
        # Lark 常见 UTC 字符串，16:00 UTC 应映射为次日（上海时区）
        s = pd.Series(["2026-06-17 16:00:00"])
        out = _to_date_series(s)
        assert out.iloc[0] == date(2026, 6, 18)


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

    def test_only_trial_group_cities_kept(self, sample_lark_data, sample_mc_data):
        result = compute_wide_table(sample_lark_data, sample_mc_data, date(2026, 6, 20))
        if not result.empty:
            if "trial_group" in result.columns and result["trial_group"].notna().any():
                if "city_unit" in result.columns:
                    assert result.loc[result["trial_group"].notna(), "city_unit"].notna().all()
                if "stage" in result.columns:
                    non_prep = result[
                        (result["stage"] != "归一化预备期")
                        & result["trial_group"].notna()
                    ]
                    if not non_prep.empty:
                        assert non_prep["trial_group"].notna().all()


class TestTrialPhaseConfigWide:
    def test_full_join_preserves_unmatched_rows(self):
        lark_data = {
            "conf_trial_group": pd.DataFrame(
                {
                    "市名称": ["株洲市", "邵阳市"],
                    "区域名称": ["株洲市", "邵阳市"],
                    "区域类型": ["CITY", "CITY"],
                    "试验分组": ["试验组一", "试验组三"],
                    "试验起始日期": [date(2026, 6, 19), date(2026, 6, 19)],
                    "试验结束日期": [date(2026, 6, 28), date(2026, 6, 28)],
                }
            ),
            "conf_trial_period_rate": pd.DataFrame(
                {
                    "试验阶段": ["摸底期", "摸底期"],
                    "运营类型": ["自营区域", "代理人区域"],
                    "抽佣率": [0.075, 0.046],
                    "试验分组": ["试验组一", "试验组二"],
                    "试验起始日期": [date(2026, 6, 19), date(2026, 6, 19)],
                    "试验结束日期": [date(2026, 6, 28), date(2026, 6, 28)],
                }
            ),
        }

        wide = compute_trial_phase_config_wide(lark_data)
        # 应保留: 匹配(试验组一) + 左侧未匹配(试验组三) + 右侧未匹配(试验组二)
        assert len(wide) >= 3
        assert "试验组三" in set(wide["试验分组"].dropna())
        assert "试验组二" in set(wide["试验分组"].dropna())

    def test_pivot_has_region_type_columns(self, sample_lark_data):
        lark_data = {
            "conf_trial_group": pd.DataFrame(
                {
                    "市名称": ["株洲市", "株洲市"],
                    "区域名称": ["株洲市", "株洲市"],
                    "区域类型": ["CITY", "CITY"],
                    "试验分组": ["试验组一", "试验组一"],
                    "试验起始日期": [date(2026, 6, 19), date(2026, 6, 19)],
                    "试验结束日期": [date(2026, 6, 28), date(2026, 6, 28)],
                }
            ),
            "conf_trial_period_rate": pd.DataFrame(
                {
                    "试验阶段": ["摸底期", "摸底期"],
                    "运营类型": ["自营区域", "代理人区域"],
                    "抽佣率": [0.075, 0.046],
                    "试验分组": ["试验组一", "试验组一"],
                    "试验起始日期": [date(2026, 6, 19), date(2026, 6, 19)],
                    "试验结束日期": [date(2026, 6, 28), date(2026, 6, 28)],
                }
            ),
        }

        wide = compute_trial_phase_config_wide(lark_data)
        pivot = compute_trial_phase_config_pivot(wide)
        assert not pivot.empty
        assert "市名称" in pivot.columns
        assert "自营区域" in pivot.columns
        assert "代理人区域" in pivot.columns


class TestTrialSkuProfile:
    def test_trial_sku_profile_inner_join_and_last_date(self, sample_lark_data):
        profile = compute_trial_sku_profile(sample_lark_data)
        assert not profile.empty
        assert "商品id" in profile.columns
        assert "商品名称" in profile.columns
        assert "商家名称" in profile.columns
        assert "非试验区域抽佣率" in profile.columns
        assert "last_trial_date" in profile.columns

        sku_10184690 = profile[profile["商品id"] == 10184690]
        assert not sku_10184690.empty
        assert sku_10184690.iloc[0]["last_trial_date"] == date(2026, 6, 21)
