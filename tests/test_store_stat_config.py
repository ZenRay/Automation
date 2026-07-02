# coding:utf8
"""tests/test_store_stat_config.py -- store_stat 配置验证测试

验证 config.py 中 store_stat 相关配置的正确性。
"""

from __future__ import annotations

from workers.lib.models import LarkFieldType
from workers.upgrade_after_sale.config import (
    DATA_ROUTES,
    LARK_TARGETS,
    QUERY_WINDOWS,
    ROUTE_DATE_FIELDS,
    SQL_QUERIES,
    TARGET_STORE_STAT,
)


def test_sql_queries_contains_store_stat():
    names = [q.name for q in SQL_QUERIES]
    assert "store_stat" in names

    store_stat_cfg = next(q for q in SQL_QUERIES if q.name == "store_stat")
    assert store_stat_cfg.sql_file == "store_stat_query.sql"
    assert store_stat_cfg.use_temp_table is True
    assert store_stat_cfg.temp_table_project == "datawarehouse_max_dev"


def test_query_windows_contains_store_stat():
    assert "store_stat" in QUERY_WINDOWS
    assert QUERY_WINDOWS["store_stat"] == {"start": -7, "end": -1}


def test_target_store_stat_field_count():
    assert len(TARGET_STORE_STAT.field_mappings) == 48


def test_target_store_stat_table_name():
    assert TARGET_STORE_STAT.table_name == "门店维度统计表"
    assert TARGET_STORE_STAT.name == "store_stat"


def test_target_store_stat_field_types():
    mapping = {m.source_col: m for m in TARGET_STORE_STAT.field_mappings}

    # 日期字段为 DATE 类型
    assert mapping["日期"].lark_type == LarkFieldType.DATE

    # 店铺id 为 NUMBER 类型
    assert mapping["店铺id"].lark_type == LarkFieldType.NUMBER

    # 其余 48 个指标字段均为 NUMBER 类型
    non_numeric = [
        col
        for col, m in mapping.items()
        if col not in ("日期", "店铺id") and m.lark_type != LarkFieldType.NUMBER
    ]
    assert non_numeric == [], f"Expected all NUMBER, got non-NUMERIC: {non_numeric}"


def test_data_routes_contains_store_stat_detail():
    route_names = [r.name for r in DATA_ROUTES]
    assert "store_stat_detail" in route_names

    ss_route = next(r for r in DATA_ROUTES if r.name == "store_stat_detail")
    assert ss_route.source_ref == "mc:store_stat"
    assert ss_route.target is TARGET_STORE_STAT


def test_route_date_fields_contains_store_stat():
    assert ROUTE_DATE_FIELDS["store_stat_detail"] == "日期"


def test_lark_targets_contains_store_stat():
    target_names = [t.name for t in LARK_TARGETS]
    assert "store_stat" in target_names
