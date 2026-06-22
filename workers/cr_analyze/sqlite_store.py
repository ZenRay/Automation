# coding:utf8
"""workers.cr_analyze.sqlite_store -- SQLite 读写封装

提供 write_tables / read_table / list_tables / table_exists 四个操作，
使用 pandas 的 to_sql / read_sql 实现，全表覆盖写入。
"""

import json
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd

_SQLITE_SAFE_TYPES = (str, int, float, bool, type(None), date, datetime)


def _sanitize_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """统一将 DataFrame 转为 SQLite 兼容类型（向量化，适合大表）。

    策略:
    - extension dtype 列 → astype(object) → replace NA with None
    - object 列中可能含 list/dict → 检测并批量转 JSON str
    - numpy 标量通过 .astype(object) 自动转为 Python 原生类型
    """
    df = df.copy()

    # 0. 将 decimal.Decimal 转为 float（MaxCompute 返回 Decimal，SQLite 不支持）
    for col in df.select_dtypes(include=["object"]).columns:
        if df[col].apply(lambda x: isinstance(x, Decimal)).any():
            df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)

    for col in df.columns:
        dtype = df[col].dtype

        # 1. pandas extension types (StringDtype, Int64Dtype 等)
        if hasattr(dtype, "na_value"):
            df[col] = df[col].astype(object).where(df[col].notna(), None)
            continue

        # 2. object 列: 可能含 list/dict/嵌套对象
        if dtype == object:
            sample = df[col].dropna().head(50)
            has_complex = any(isinstance(v, (list, dict)) for v in sample)
            if has_complex:
                df[col] = df[col].apply(
                    lambda x: (
                        json.dumps(x, ensure_ascii=False, default=str)
                        if isinstance(x, (list, dict))
                        else x
                    )
                )

    return df


def write_tables(db_path: str, data: dict[str, pd.DataFrame]) -> int:
    """将多个 DataFrame 写入 SQLite，每张表使用 replace 模式覆盖。

    Args:
        db_path: SQLite 文件路径
        data:    {table_name: DataFrame} 字典

    Returns:
        写入的表数量
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(path)) as conn:
        for table_name, df in data.items():
            if df.empty and len(df.columns) == 0:
                # 跳过无列的空 DataFrame（无法创建有效 SQLite 表）
                continue
            df = _sanitize_for_sqlite(df)
            df.to_sql(table_name, conn, if_exists="replace", index=False)

    return len(data)


def read_table(db_path: str, table_name: str) -> pd.DataFrame:
    """读取单张 SQLite 表。

    Raises:
        ValueError: 当表不存在时
    """
    if not table_exists(db_path, table_name):
        raise ValueError(f"Table '{table_name}' not found in {db_path}")

    with sqlite3.connect(db_path) as conn:
        return pd.read_sql(f"SELECT * FROM [{table_name}]", conn)


def list_tables(db_path: str) -> list[str]:
    """列出 SQLite 中所有表名。"""
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    return [r[0] for r in rows]


def table_exists(db_path: str, table_name: str) -> bool:
    """检查表是否存在。"""
    if not Path(db_path).exists():
        return False

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
    return row[0] > 0
