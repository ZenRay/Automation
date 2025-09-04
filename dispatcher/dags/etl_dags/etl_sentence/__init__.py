#coding:utf-8
"""
ETL SQL语句模块
导出Maxcompute相关的SQL语句
"""

# 从maxcompute_sql.dimention模块导入SQL语句
from .maxcompute_sql.dimension import (
    dim_sku_store_tags_sentence,
    dim_store_sentence,
    dim_store_history_trade_temp_sentence,
    dim_changsha_goods_property_sentence,
    dim_goods_sentence 
)

from .maxcompute_sql.fact import (
    fact_flow_sentence,
    fact_trade_sentence
)
# 导出所有SQL语句
__all__ = [
    "dim_sku_store_tags_sentence",
    "dim_store_sentence",
    "dim_store_history_trade_temp_sentence",
    "dim_changsha_goods_property_sentence",
    "dim_goods_sentence",
    "fact_flow_sentence",
    "fact_trade_sentence",
]
