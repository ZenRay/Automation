# coding:utf8
"""tests/test_sku_stat_row_key.py -- sku_stat 复合 row_key 测试

验证 sku_stat 路由的四级复合 row_key 构建逻辑。
"""

from __future__ import annotations

import pytest
import pandas as pd

from workers.upgrade_after_sale.main import _build_row_key, _inject_row_key


def test_build_row_key_sku_stat_composite():
    """sku_stat_detail 使用 商家id + 四级类目id + 商品id + 日期 四级复合键"""
    df = pd.DataFrame(
        {
            "商家id": [1, 1, 2],
            "四级类目id": [1001, 1002, 1001],
            "商品id": [5001, 5002, 5001],
            "日期": pd.to_datetime(["2026-06-20", "2026-06-20", "2026-06-21"]),
            "下单店铺数": [5, 10, 15],
        }
    )
    row_key = _build_row_key(df, "sku_stat_detail")

    assert row_key.iloc[0] == "1_1001_5001_2026-06-20"
    assert row_key.iloc[1] == "1_1002_5002_2026-06-20"
    assert row_key.iloc[2] == "2_1001_5001_2026-06-21"


def test_build_row_key_sku_stat_no_duplicates():
    """相同输入不会生成重复的 row_key"""
    df = pd.DataFrame(
        {
            "商家id": [1, 1, 1],
            "四级类目id": [1001, 1001, 1001],
            "商品id": [5001, 5002, 5001],
            "日期": pd.to_datetime(["2026-06-20", "2026-06-20", "2026-06-21"]),
            "下单店铺数": [5, 10, 15],
        }
    )
    row_key = _build_row_key(df, "sku_stat_detail")
    assert row_key.nunique() == 3


def test_build_row_key_sku_stat_missing_merchant_id():
    """缺少商家id列时抛出 ValueError"""
    df = pd.DataFrame(
        {
            "四级类目id": [1001],
            "商品id": [5001],
            "日期": pd.to_datetime(["2026-06-20"]),
            "下单店铺数": [5],
        }
    )
    with pytest.raises(ValueError, match="Missing row key column '商家id'"):
        _build_row_key(df, "sku_stat_detail")


def test_build_row_key_sku_stat_missing_sku_id():
    """缺少商品id列时抛出 ValueError"""
    df = pd.DataFrame(
        {
            "商家id": [1],
            "四级类目id": [1001],
            "日期": pd.to_datetime(["2026-06-20"]),
            "下单店铺数": [5],
        }
    )
    with pytest.raises(ValueError, match="Missing row key column '商品id'"):
        _build_row_key(df, "sku_stat_detail")


def test_build_row_key_sku_stat_missing_date():
    """缺少日期列时抛出 ValueError"""
    df = pd.DataFrame(
        {
            "商家id": [1],
            "四级类目id": [1001],
            "商品id": [5001],
            "下单店铺数": [5],
        }
    )
    with pytest.raises(ValueError, match="Missing row key column '日期'"):
        _build_row_key(df, "sku_stat_detail")


def test_inject_row_key_sku_stat():
    """_inject_row_key 为 sku_stat 数据注入正确的 row_key"""
    mc_data = {
        "sku_stat": pd.DataFrame(
            {
                "商家id": [1],
                "四级类目id": [1001],
                "商品id": [5001],
                "日期": pd.to_datetime(["2026-06-20"]),
                "下单店铺数": [5],
            }
        )
    }
    _inject_row_key(mc_data)
    assert "row_key" in mc_data["sku_stat"].columns
    assert mc_data["sku_stat"]["row_key"].iloc[0] == "1_1001_5001_2026-06-20"
