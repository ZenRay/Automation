#coding:utf-8
"""
ETL SQL语句模块
导出Maxcompute相关的SQL语句
"""

# 从maxcompute_sql.dimention模块导入SQL语句
from .maxcompute_sql.dimention import (
    dim_changsha_goods_property_sentence,
    dim_store_history_trade_temp,
    dim_changsha_store_daily_full
)

# 导出所有SQL语句
__all__ = [
    'dim_changsha_goods_property_sentence',
    'dim_store_history_trade_temp', 
    'dim_changsha_store_daily_full'
]
