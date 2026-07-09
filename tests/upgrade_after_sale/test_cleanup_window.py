# coding:utf8
from __future__ import annotations

from datetime import date

from workers.lib.models import CleanupCondition
from workers.upgrade_after_sale.config import DATA_ROUTES
from workers.upgrade_after_sale.main import _replace_cleanup_windows


def test_replace_cleanup_windows_per_route_date_field():
    routes = _replace_cleanup_windows(
        DATA_ROUTES,
        {
            "after_sale_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "order_detail": (date(2026, 6, 23), date(2026, 6, 26)),
            "store_stat_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "store_cat1_stat_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "cat4_stat_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "mct_cat4_stat_detail": (date(2026, 6, 19), date(2026, 6, 26)),
            "sku_stat_detail": (date(2026, 6, 19), date(2026, 6, 26)),
        },
    )

    by_name = {r.name: r for r in routes}
    as_cleanup = by_name["after_sale_detail"].target.cleanup_conditions
    od_cleanup = by_name["order_detail"].target.cleanup_conditions

    assert isinstance(as_cleanup, CleanupCondition)
    assert isinstance(od_cleanup, CleanupCondition)
    assert not as_cleanup.is_runtime
    assert not od_cleanup.is_runtime

    as_filter = as_cleanup.to_lark_filter()
    od_filter = od_cleanup.to_lark_filter()

    assert as_filter["conditions"][0]["field_name"] == "申请日期"
    assert od_filter["conditions"][0]["field_name"] == "日期"
