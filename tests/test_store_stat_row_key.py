# coding:utf8
"""tests/test_store_stat_row_key.py -- store_stat 复合 row_key 测试

验证 store_stat 路由的 row_key 构建逻辑。
"""

from __future__ import annotations

import pytest
import pandas as pd

from workers.upgrade_after_sale.main import _build_row_key, _inject_row_key


def test_build_row_key_store_stat_composite():
    """store_stat_detail 使用 店铺id + 日期 复合键"""
    df = pd.DataFrame(
        {
            "店铺id": [1001, 1002, 1003],
            "日期": pd.to_datetime(["2026-06-20", "2026-06-21", "2026-06-22"]),
            "曝光次数": [10, 20, 30],
        }
    )
    row_key = _build_row_key(df, "store_stat_detail")

    assert row_key.iloc[0] == "1001_2026-06-20"
    assert row_key.iloc[1] == "1002_2026-06-21"
    assert row_key.iloc[2] == "1003_2026-06-22"


def test_build_row_key_store_stat_no_duplicates():
    """生成的 row_key 无重复"""
    df = pd.DataFrame(
        {
            "店铺id": [1001, 1001, 1002],
            "日期": pd.to_datetime(["2026-06-20", "2026-06-21", "2026-06-20"]),
        }
    )
    row_key = _build_row_key(df, "store_stat_detail")
    assert row_key.is_unique


def test_build_row_key_store_stat_missing_store_id():
    """缺少 店铺id 列时抛出 ValueError"""
    df = pd.DataFrame({"日期": pd.to_datetime(["2026-06-20"]), "曝光次数": [10]})
    with pytest.raises(ValueError, match="店铺id"):
        _build_row_key(df, "store_stat_detail")


def test_build_row_key_store_stat_missing_date():
    """缺少 日期 列时抛出 ValueError"""
    df = pd.DataFrame({"店铺id": [1001], "曝光次数": [10]})
    with pytest.raises(ValueError, match="日期"):
        _build_row_key(df, "store_stat_detail")


def test_inject_row_key_handles_store_stat():
    """_inject_row_key 正确处理 mc_data['store_stat']"""
    mc_data = {
        "store_stat": pd.DataFrame(
            {
                "店铺id": [1001, 1002],
                "日期": pd.to_datetime(["2026-06-20", "2026-06-21"]),
                "曝光次数": [10, 20],
            }
        ),
    }
    _inject_row_key(mc_data)

    assert "row_key" in mc_data["store_stat"].columns
    assert mc_data["store_stat"]["row_key"].iloc[0] == "1001_2026-06-20"
