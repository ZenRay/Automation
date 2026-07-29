# coding:utf8
"""tests/test_cat4_stat_config.py -- cat4_stat 配置验证测试

验证 config.py 中 cat4_stat 相关配置的正确性。
"""

from __future__ import annotations

from workers.lib.models import LarkFieldType
from workers.upgrade_after_sale.config import (
    DATA_ROUTES,
    LARK_TARGETS,
    QUERY_WINDOWS,
    ROUTE_DATE_FIELDS,
    SQL_QUERIES,
    TARGET_CAT4_STAT,
)


def test_sql_queries_contains_cat4_stat():
    names = [q.name for q in SQL_QUERIES]
    assert "cat4_stat" in names

    cfg = next(q for q in SQL_QUERIES if q.name == "cat4_stat")
    assert cfg.sql_file == "cat4_stat_query.sql"
    assert cfg.use_temp_table is True
    assert cfg.temp_table_project == "datawarehouse_max_dev"


def test_query_windows_contains_cat4_stat():
    assert "cat4_stat" in QUERY_WINDOWS
    assert QUERY_WINDOWS["cat4_stat"] == {"start": -10, "end": 0}


def test_target_cat4_stat_field_count():
    assert len(TARGET_CAT4_STAT.field_mappings) == 37


def test_target_cat4_stat_table_name():
    assert TARGET_CAT4_STAT.table_name == "四级类目维度表"
    assert TARGET_CAT4_STAT.name == "cat4_stat"


def test_target_cat4_stat_field_types():
    mapping = {m.source_col: m for m in TARGET_CAT4_STAT.field_mappings}

    assert mapping["日期"].lark_type == LarkFieldType.DATE
    assert mapping["一级类目id"].lark_type == LarkFieldType.NUMBER
    assert mapping["一级类目名称"].lark_type == LarkFieldType.TEXT
    assert mapping["四级类目id"].lark_type == LarkFieldType.NUMBER
    assert mapping["四级类目名称"].lark_type == LarkFieldType.TEXT

    # 其余指标字段均为 NUMBER
    non_numeric = [
        col
        for col, m in mapping.items()
        if col not in ("日期", "一级类目名称", "四级类目名称")
        and m.lark_type != LarkFieldType.NUMBER
    ]
    assert non_numeric == [], f"Expected all NUMBER, got non-NUMERIC: {non_numeric}"


def test_data_routes_contains_cat4_stat_detail():
    route_names = [r.name for r in DATA_ROUTES]
    assert "cat4_stat_detail" in route_names

    route = next(r for r in DATA_ROUTES if r.name == "cat4_stat_detail")
    assert route.source_ref == "mc:cat4_stat"
    assert route.target is TARGET_CAT4_STAT


def test_route_date_fields_contains_cat4_stat():
    assert ROUTE_DATE_FIELDS["cat4_stat_detail"] == "日期"


def test_lark_targets_contains_cat4_stat():
    target_names = [t.name for t in LARK_TARGETS]
    assert "cat4_stat" in target_names


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


def test_cat4_stat_is_first_route():
    """cat4_stat_detail 是第一条路由（无附件，优先执行）"""
    assert DATA_ROUTES[0].name == "cat4_stat_detail"
