#coding:utf-8
"""
Maxcompute SQL包
包含各种ETL SQL语句
"""
# 从dimention子模块导入
from .dimension.goods import dim_goods_sentence, dim_changsha_goods_property_sentence
from .dimension.stores import dim_store_sentence, dim_store_history_trade_temp_sentence
from .dimension.stores_goods import dim_sku_store_tags_sentence  # 确保这个变量在stores_goods.py中存在

# 从fact子模块导入
from .fact.flow import fact_flow_sentence
from .fact.trade import fact_trade_sentence

# 导出所有变量
__all__ = [
    'dim_sku_store_tags_sentence',
    'dim_store_sentence',
    'dim_store_history_trade_temp_sentence',
    'dim_goods_sentence',
    'dim_changsha_goods_property_sentence',
    'fact_flow_sentence',
    'fact_trade_sentence'
]