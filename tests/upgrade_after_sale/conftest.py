# coding:utf8
"""Shared fixtures for upgrade_after_sale tests."""

import pytest


@pytest.fixture
def after_sale_sample_df():
    """售后明细表测试 DataFrame 样本。"""
    import pandas as pd

    return pd.DataFrame(
        {
            "售后单id": ["AS001", "AS002"],
            "日期": pd.to_datetime(["2026-07-08", "2026-07-08"]).date,
            "金额": [100.0, 200.0],
        }
    )
