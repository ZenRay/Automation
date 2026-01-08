#coding:utf-8
"""
Maxcompute SQL包
包含各种ETL SQL语句
"""

from .merchant import (
    mct_roi_report_sentence,
    mct_mall_stat_sentence,
    mct_mct_cat4_stat_sentence,
    mct_sku_stat_sentence,
    mct_mct_province_stat_sentence,
    mct_cat4_stat_sentence,
    temp_dws_mct_stat_sentence
)

from .mall import (
    daily_mall_report_sentence
    ,daily_sku_report_sentence
    ,daily_page_index_report_sentence
)


# 导出所有变量
__all__ = [
    'mct_roi_report_sentence',
    'daily_mall_report_sentence',
    'daily_sku_report_sentence',
    'daily_page_index_report_sentence',
    'mct_mall_stat_sentence',
    'mct_mct_cat4_stat_sentence',
    'mct_sku_stat_sentence',
    'mct_mct_province_stat_sentence',
    'mct_cat4_stat_sentence',
    'temp_dws_mct_stat_sentence'
]