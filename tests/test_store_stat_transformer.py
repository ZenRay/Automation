# coding:utf8
"""tests/test_store_stat_transformer.py -- store_stat normalize 测试

验证 normalize_store_stat_df 的正确性。
"""

from __future__ import annotations

import pandas as pd

from workers.upgrade_after_sale.transformer import normalize_store_stat_df


def test_normalize_store_stat_empty_df():
    """空 DataFrame 输入返回空 DataFrame"""
    df = pd.DataFrame()
    out = normalize_store_stat_df(df)
    assert out.empty


def test_normalize_store_stat_date_conversion():
    """日期列被正确转换为 datetime64"""
    df = pd.DataFrame(
        {
            "日期": ["2026-06-20", "2026-06-21", "2026-06-22"],
            "店铺id": [1001, 1002, 1003],
            "曝光次数": [10, 20, 30],
        }
    )
    out = normalize_store_stat_df(df)

    assert pd.api.types.is_datetime64_any_dtype(out["日期"])
    assert str(out["日期"].iloc[0].date()) == "2026-06-20"


def test_normalize_store_stat_numeric_unchanged():
    """数值列保持不变"""
    df = pd.DataFrame(
        {
            "日期": ["2026-06-20"],
            "店铺id": [1001],
            "曝光次数": [10],
            "下单金额": [99.5],
        }
    )
    out = normalize_store_stat_df(df)

    assert out["店铺id"].iloc[0] == 1001
    assert out["曝光次数"].iloc[0] == 10
    assert out["下单金额"].iloc[0] == 99.5


def test_normalize_store_stat_no_mutation():
    """原始 DataFrame 不被修改（copy 语义）"""
    df = pd.DataFrame(
        {
            "日期": ["2026-06-20"],
            "店铺id": [1001],
        }
    )
    original_date_val = df["日期"].iloc[0]
    normalize_store_stat_df(df)

    # 原始 df 的日期列仍然是字符串
    assert df["日期"].iloc[0] == original_date_val
    assert isinstance(df["日期"].iloc[0], str)
