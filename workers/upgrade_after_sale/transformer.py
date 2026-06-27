# coding:utf8
"""workers.upgrade_after_sale.transformer -- 轻量转换

当前 SQL 已产出目标字段，本模块仅做轻量标准化预留。
"""

from __future__ import annotations

import pandas as pd


def normalize_after_sale_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    # 统一日期列类型（保留 datetime64，避免校验层误判 python date）
    if "申请日期" in out.columns:
        out["申请日期"] = pd.to_datetime(out["申请日期"], errors="coerce")
    return out


def normalize_order_item_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "日期" in out.columns:
        out["日期"] = pd.to_datetime(out["日期"], errors="coerce")
    return out
