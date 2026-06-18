# coding:utf8
"""Unit tests for workers.cr_trail_pricing.config"""

import pytest

from workers.cr_trail_pricing.config import (
    APP_TOKEN,
    LARK_SOURCES,
    PRODUCT_KEEP_FIELDS,
    REGION_OUTPUT_FIELDS,
    COLUMN_RENAME_MAP,
    OUTPUT_COLUMNS,
)


class TestAppToken:
    def test_value(self):
        assert APP_TOKEN == "GmWEbfgCmatLAnsSyaocBisJnoe"


class TestLarkSources:
    EXPECTED_NAMES = [
        "conf_county",
        "conf_goods",
        "conf_trial_group",
        "conf_trial_commission",
        "conf_trial_goods",
        "conf_hidden_logistics",
    ]

    def test_count(self):
        assert len(LARK_SOURCES) == 6

    def test_names(self):
        names = [s.name for s in LARK_SOURCES]
        assert names == self.EXPECTED_NAMES

    def test_each_source_has_required_fields(self):
        for src in LARK_SOURCES:
            assert src.name, f"source missing name"
            assert src.url, f"{src.name}: missing url"
            assert src.table_name, f"{src.name}: missing table_name"
            assert len(src.field_names) > 0, f"{src.name}: empty field_names"

    def test_all_urls_have_table_param(self):
        for src in LARK_SOURCES:
            assert "table=" in src.url, (
                f"{src.name}: URL missing table= parameter"
            )

    def test_known_table_ids(self):
        table_ids = {s.name: s.url.split("table=")[1].split("&")[0] for s in LARK_SOURCES}
        assert table_ids["conf_county"] == "tblBgJYpBRT18Uvp"
        assert table_ids["conf_goods"] == "tblevDYqsTdwu8fo"

    def test_date_filtered_sources(self):
        """conf_county, conf_goods, conf_trial_goods, conf_hidden_logistics use API-level date filter"""
        date_filtered = {s.name: s for s in LARK_SOURCES if s.date_filter_field}
        assert set(date_filtered.keys()) == {
            "conf_county", "conf_goods", "conf_trial_goods", "conf_hidden_logistics",
        }
        for name, src in date_filtered.items():
            assert src.date_filter_field == "日期", f"{name}: wrong date_filter_field"
            assert src.date_fields == ["日期"], f"{name}: wrong date_fields"

    def test_unfiltered_sources(self):
        """conf_trial_group, conf_trial_commission have no API date filter (pandas range filter)"""
        unfiltered = {s.name: s for s in LARK_SOURCES if not s.date_filter_field}
        assert set(unfiltered.keys()) == {
            "conf_trial_group", "conf_trial_commission",
        }

    def test_date_range_sources_have_date_fields(self):
        """Sources with date range columns should declare them in date_fields"""
        range_sources = {"conf_trial_group", "conf_trial_commission"}
        for src in LARK_SOURCES:
            if src.name in range_sources:
                assert len(src.date_fields) >= 2, (
                    f"{src.name}: range-filtered source should have >= 2 date_fields"
                )

    def test_conf_county_fields_include_city_id(self):
        county = next(s for s in LARK_SOURCES if s.name == "conf_county")
        assert "市id" in county.field_names

    def test_conf_goods_fields(self):
        goods = next(s for s in LARK_SOURCES if s.name == "conf_goods")
        for field in ["日期", "商品id", "商品编码", "商品名称", "后台类目名称", "非试验区域抽佣率", "毛重"]:
            assert field in goods.field_names, f"conf_goods missing field: {field}"

    def test_conf_hidden_logistics_fields(self):
        logistics = next(s for s in LARK_SOURCES if s.name == "conf_hidden_logistics")
        for field in ["日期", "市id", "费率", "区县费率映射"]:
            assert field in logistics.field_names, f"conf_hidden_logistics missing field: {field}"


class TestProductKeepFields:
    EXPECTED = [
        "商品id", "商品编码", "商品名称", "后台类目名称",
        "非试验区域抽佣率", "毛重", "是否试验区域",
    ]

    def test_exact_fields(self):
        assert PRODUCT_KEEP_FIELDS == self.EXPECTED

    def test_count(self):
        assert len(PRODUCT_KEEP_FIELDS) == 7


class TestRegionOutputFields:
    EXPECTED = [
        "日期", "试验区域id", "试验区域名称", "市id", "省id", "区县id",
        "是否试验区域", "试验分组", "运营类型", "抽佣率",
    ]

    def test_exact_fields(self):
        assert REGION_OUTPUT_FIELDS == self.EXPECTED

    def test_count(self):
        assert len(REGION_OUTPUT_FIELDS) == 10


class TestColumnRenameMap:
    def test_trial_region_id(self):
        assert COLUMN_RENAME_MAP["试验区域id"] == "区域id"

    def test_trial_region_name(self):
        assert COLUMN_RENAME_MAP["试验区域名称"] == "区域名称"

    def test_product_code(self):
        assert COLUMN_RENAME_MAP["商品编码"] == "商品SKU"

    def test_count(self):
        assert len(COLUMN_RENAME_MAP) == 3


class TestOutputColumns:
    EXPECTED = [
        "区域id", "区域名称",
        "商品SKU", "商品名称",
        "调价方向", "调价幅度", "设置状态",
        "固定抽佣比例", "固定抽佣货值",
        "商品id", "后台类目名称",
    ]

    def test_exact_order(self):
        assert OUTPUT_COLUMNS == self.EXPECTED

    def test_count(self):
        assert len(OUTPUT_COLUMNS) == 11
