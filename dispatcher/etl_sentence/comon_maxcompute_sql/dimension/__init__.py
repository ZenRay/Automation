#coding:utf-8

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))

# dimention_path = os.path.join(current_dir, 'dimention')
# fact_path = os.path.join(current_dir, 'fact')


if current_dir not in sys.path:
    # sys.path.insert(0, dimention_path)
    sys.path.insert(0, current_dir)
    # sys.path.insert(0, fact_path)
    

# 从各模块导入
from goods import dim_goods_sentence, dim_changsha_goods_property_sentence
from stores import dim_store_sentence, dim_store_history_trade_temp_sentence
from stores_goods import dim_sku_store_tags_sentence

# 导出所有变量
__all__ = [
    'dim_sku_store_tags_sentence',
    'dim_store_sentence',
    'dim_store_history_trade_temp_sentence',
    'dim_goods_sentence',
    'dim_changsha_goods_property_sentence'
]