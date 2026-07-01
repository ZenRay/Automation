# coding:utf8
"""tests/test_store_stat_date_window.py -- store_stat 日期窗口测试

验证 store_stat 独立日期窗口的正确性。
"""

from __future__ import annotations

from datetime import date

from workers.lib.models import CleanupCondition
from workers.upgrade_after_sale.config import DATA_ROUTES, QUERY_WINDOWS
from workers.upgrade_after_sale.main import (
    _build_date_params,
    _compute_window,
    _replace_cleanup_windows,
)


def test_store_stat_default_window_sql_params():
    """store_stat 默认窗口 (-7, 0) 生成正确的 SQL 参数"""
    window = QUERY_WINDOWS["store_stat"]
    params = _build_date_params("2026-06-26", window["start"], window["end"])
    sql = params.sql_params()

    assert sql["date_param"] == "DATE '2026-06-26'"
    assert sql["start_offset"] == "-7"
    assert sql["end_offset"] == "0"


def test_store_stat_compute_window():
    """_compute_window 返回正确的日期范围"""
    params = _build_date_params("2026-06-26", -7, 0)
    start, end = _compute_window(params)
    assert str(start) == "2026-06-19"
    assert str(end) == "2026-06-26"


def test_store_stat_custom_window():
    """store_stat 支持自定义窗口偏移"""
    params = _build_date_params("2026-06-26", -14, -1)
    sql = params.sql_params()
    assert sql["start_offset"] == "-14"
    assert sql["end_offset"] == "-1"

    start, end = _compute_window(params)
    assert str(start) == "2026-06-12"
    assert str(end) == "2026-06-25"


def test_replace_cleanup_windows_store_stat():
    """_replace_cleanup_windows 正确替换 store_stat_detail 的清理窗口"""
    routes = _replace_cleanup_windows(
        DATA_ROUTES,
        {
            "after_sale_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "order_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "store_stat_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "store_cat1_stat_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "cat4_stat_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "mct_cat4_stat_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "sku_stat_detail": (date(2026, 6, 19), date(2026, 6, 26)),
        },
    )

    by_name = {r.name: r for r in routes}
    ss_cleanup = by_name["store_stat_detail"].target.cleanup_conditions

    assert isinstance(ss_cleanup, CleanupCondition)
    assert not ss_cleanup.is_runtime

    ss_filter = ss_cleanup.to_lark_filter()
    assert ss_filter["conditions"][0]["field_name"] == "日期"


def test_store_stat_window_independent_from_other_windows():
    """store_stat 窗口与 after_sale/order_item 窗口相互独立"""
    as_params = _build_date_params("2026-06-26", -7, 0)
    od_params = _build_date_params("2026-06-26", -3, -1)
    ss_params = _build_date_params("2026-06-26", -14, 0)

    as_sql = as_params.sql_params()
    od_sql = od_params.sql_params()
    ss_sql = ss_params.sql_params()

    # 各自的 start_offset 互不影响
    assert as_sql["start_offset"] == "-7"
    assert od_sql["start_offset"] == "-3"
    assert ss_sql["start_offset"] == "-14"
