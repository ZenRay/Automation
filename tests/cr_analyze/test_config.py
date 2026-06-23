# coding:utf8
"""tests/cr_analyze/test_config.py -- 配置完整性测试"""

from pathlib import Path
from datetime import date

import pytest


class TestLarkSources:
    def test_seven_sources_defined(self):
        from workers.cr_analyze.config import LARK_SOURCES
        assert len(LARK_SOURCES) == 7

    def test_source_names(self):
        from workers.cr_analyze.config import LARK_SOURCES
        names = {s.name for s in LARK_SOURCES}
        expected = {
            "conf_trial_product_info",
            "conf_product_info", "conf_county_info",
            "conf_commission_adjustment", "conf_trial_region_price",
            "conf_trial_group", "conf_trial_period_rate",
        }
        assert names == expected

    def test_field_names_non_empty(self):
        from workers.cr_analyze.config import LARK_SOURCES
        for source in LARK_SOURCES:
            assert source.field_names is not None
            assert len(source.field_names) > 0, f"{source.name} has empty field_names"

    def test_date_fields_declared(self):
        from workers.cr_analyze.config import LARK_SOURCES
        for source in LARK_SOURCES:
            assert source.date_fields is not None
            assert len(source.date_fields) > 0, f"{source.name} missing date_fields"


class TestSQLQueries:
    def test_sql_queries_defined(self):
        from workers.cr_analyze.config import SQL_QUERIES
        assert len(SQL_QUERIES) >= 1

    def test_sql_file_exists(self):
        from workers.cr_analyze.config import SQL_QUERIES, SQL_BASE_DIR
        for query in SQL_QUERIES:
            path = SQL_BASE_DIR / query.sql_file
            assert path.exists(), f"SQL file not found: {path}"


class TestTrialPhaseConfig:
    def test_dragon_boat_dates(self):
        from workers.cr_analyze.config import TRIAL_PHASE_CONFIG
        dates = TRIAL_PHASE_CONFIG["dragon_boat_dates"]
        assert len(dates) == 3
        assert dates[0] == date(2026, 6, 19)
        assert dates[2] == date(2026, 6, 21)

    def test_historical_baseline_ranges(self):
        from workers.cr_analyze.config import TRIAL_PHASE_CONFIG
        ranges = TRIAL_PHASE_CONFIG["historical_baseline_ranges"]
        assert len(ranges) == 2
        assert ranges[0] == (date(2026, 4, 13), date(2026, 4, 26))

    def test_holiday_extension_config(self):
        from workers.cr_analyze.config import TRIAL_PHASE_CONFIG
        assert TRIAL_PHASE_CONFIG["holiday_extension_days"] == 3
        assert TRIAL_PHASE_CONFIG["baseline_min_effective_days"] >= 1


class TestTargetSkuIds:
    def test_three_skus(self):
        from workers.cr_analyze.config import TARGET_SKU_IDS
        assert len(TARGET_SKU_IDS) == 3
        assert 10184690 in TARGET_SKU_IDS
        assert 20519020 in TARGET_SKU_IDS
        assert 20588413 in TARGET_SKU_IDS


class TestAlertThresholds:
    def test_all_stages_present(self):
        from workers.cr_analyze.config import ALERT_THRESHOLDS
        assert "归一化预备期" in ALERT_THRESHOLDS
        assert "摸底期" in ALERT_THRESHOLDS
        assert "生效期" in ALERT_THRESHOLDS


class TestTargetR0Reference:
    def test_all_phases_present(self):
        from workers.cr_analyze.config import TARGET_R0_REFERENCE
        assert "归一化预备期" in TARGET_R0_REFERENCE
        assert "摸底期" in TARGET_R0_REFERENCE
        assert "生效期" in TARGET_R0_REFERENCE

    def test_control_group_values(self):
        from workers.cr_analyze.config import TARGET_R0_REFERENCE
        # 对照组 自营 7.5% across all phases
        for phase in TARGET_R0_REFERENCE:
            for region_type in TARGET_R0_REFERENCE[phase]:
                val = TARGET_R0_REFERENCE[phase][region_type]["对照组"]
                if "自营" in region_type:
                    assert abs(val - 0.075) < 0.001
                else:
                    assert abs(val - 0.046) < 0.001
