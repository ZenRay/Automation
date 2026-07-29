# coding:utf8
"""tests/test_store_cat1_stat_config.py -- store_cat1_stat 配置验证测试

验证 config.py 中 store_cat1_stat 相关配置的正确性。
"""

from __future__ import annotations

from workers.lib.models import LarkFieldType
from workers.upgrade_after_sale.config import (
    DATA_ROUTES,
    LARK_TARGETS,
    QUERY_WINDOWS,
    ROUTE_DATE_FIELDS,
    SQL_QUERIES,
    TARGET_STORE_CAT1_STAT,
)


def test_sql_queries_contains_store_cat1_stat():
    names = [q.name for q in SQL_QUERIES]
    assert "store_cat1_stat" in names

    cfg = next(q for q in SQL_QUERIES if q.name == "store_cat1_stat")
    assert cfg.sql_file == "store_cat1_stat_query.sql"
    assert cfg.use_temp_table is True
    assert cfg.temp_table_project == "datawarehouse_max_dev"


def test_query_windows_contains_store_cat1_stat():
    assert "store_cat1_stat" in QUERY_WINDOWS
    assert QUERY_WINDOWS["store_cat1_stat"] == {"start": -7, "end": 0}


def test_target_store_cat1_stat_field_count():
    assert len(TARGET_STORE_CAT1_STAT.field_mappings) == 13


def test_target_store_cat1_stat_table_name():
    assert TARGET_STORE_CAT1_STAT.table_name == "门店一级类目维度统计表"
    assert TARGET_STORE_CAT1_STAT.name == "store_cat1_stat"


def test_target_store_cat1_stat_field_types():
    mapping = {m.source_col: m for m in TARGET_STORE_CAT1_STAT.field_mappings}

    assert mapping["日期"].lark_type == LarkFieldType.DATE
    assert mapping["店铺id"].lark_type == LarkFieldType.NUMBER
    assert mapping["一级类目id"].lark_type == LarkFieldType.NUMBER
    assert mapping["一级类目名称"].lark_type == LarkFieldType.TEXT

    numeric_cols = [
        "下单数量",
        "下单金额",
        "送达数量",
        "送达金额",
        "实付金额",
        "售后赔付金额",
        "品质问题售后赔付金额",
        "平台抽佣金额",
        "自然日售后赔付金额",
    ]
    for col in numeric_cols:
        assert mapping[col].lark_type == LarkFieldType.NUMBER, f"{col} should be NUMBER"


def test_data_routes_contains_store_cat1_stat_detail():
    route_names = [r.name for r in DATA_ROUTES]
    assert "store_cat1_stat_detail" in route_names

    route = next(r for r in DATA_ROUTES if r.name == "store_cat1_stat_detail")
    assert route.source_ref == "mc:store_cat1_stat"
    assert route.target is TARGET_STORE_CAT1_STAT


def test_route_date_fields_contains_store_cat1_stat():
    assert ROUTE_DATE_FIELDS["store_cat1_stat_detail"] == "日期"


def test_lark_targets_contains_store_cat1_stat():
    target_names = [t.name for t in LARK_TARGETS]
    assert "store_cat1_stat" in target_names


def test_data_routes_order():
    """上传顺序：四级类目 -> 商家四级类目 -> 商品统计 -> 商家统计 -> 门店维度 -> 门店一级类目 -> 订单 -> 售后"""
    route_names = [r.name for r in DATA_ROUTES]
    assert route_names == [
        "cat4_stat_detail",
        "mct_cat4_stat_detail",
        "sku_stat_detail",
        "mct_stat_detail",
        "store_stat_detail",
        "store_cat1_stat_detail",
        "order_detail",
        "after_sale_detail",
        "dim_sku_detail",
    ]
