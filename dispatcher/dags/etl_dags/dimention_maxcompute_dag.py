#coding:utf-8
"""
MaxCompute维度表ETL DAG
用于执行维度表的ETL任务，包括商品属性、店铺日度、历史交易等维度表
"""

import sys
import os
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook


from airflow.models import Connection



# 添加etl_sentence模块路径到Python搜索路径
current_dir = os.path.dirname(os.path.abspath(__file__))
etl_sentence_path = os.path.join(current_dir, 'etl_sentence')
utils_path = os.path.join(current_dir, '..', 'utils')
if etl_sentence_path not in sys.path:
    sys.path.insert(0, etl_sentence_path)
    sys.path.insert(0, current_dir)
    sys.path.insert(0, utils_path)

logger = logging.getLogger("dispatcher.dags.etl_dags")

# 导入etl_sentence模块中的SQL语句
from etl_sentence.maxcompute_sql.dimention import (
    dim_changsha_goods_property_sentence, 
    dim_changsha_store_daily_full,
    dim_store_history_trade_temp
)


# 导入MaxcomputeHook
from utils.operator import MaxcomputeOperator


# Maxcompute Hints
hints = {
    "odps.sql.allow.fullscan": True,
    "odps.sql.type.system.odps2": True,
    "odps.sql.decimal.odps2": True,
    "odps.sql.hive.compatible": True,
    "odps.odtimizer.dynamic.filter.dpp.enable": True,
    "odps.odtimizer.enable.dynamic.filter": True,
    "odps.sql.python.version": "cp37",
}


# DAG配置
DAG_CONFIG = {
    'dag_id': 'dimention_maxcompute_dag',
    'description': 'MaxCompute维度表ETL任务 - 包含商品属性、店铺日度、历史交易等维度表',
    'schedule_interval': '0 2 * * *',  # 每天凌晨2点执行
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
    'tags': ['maxcompute', 'dimension', 'etl', 'changsha'],
    'default_args': {
        'retries': 3,
        'owner': 'ChangShaTeam',
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
        'email_on_retry': False,
        'email': ['renruia07348@biaoguoworks.com'],
        'email_on_success': False,
        'execution_timeout': timedelta(hours=2),  # 执行超时时间
    }
}

# 创建DAG
with DAG(**DAG_CONFIG) as dag:
    
    # 任务1: 开始任务
    start_task = BashOperator(
        task_id='start_dimension_etl',
        bash_command='echo "🚀 开始执行MaxCompute维度表ETL任务 $(date)"',
        doc="标记ETL任务开始"
    )
    
    # 任务2: 执行维度表ETL
    dim_changsha_goods_property_task = MaxcomputeOperator(
        task_id='dim_changsha_goods_property',
        doc="商品属性维度表ETL",
        sql=dim_changsha_goods_property_sentence,
        hints=hints,
        conn_id='maxcompute_prod'
    )
    
    dim_store_history_trade_temp_task = MaxcomputeOperator(
        task_id='dim_store_history_trade_temp',
        doc="店铺历史交易维度表ETL",
        sql=dim_store_history_trade_temp,
        hints=hints,
        conn_id='maxcompute_prod'
    )
    
    dim_changsha_store_daily_full_task = MaxcomputeOperator(
        task_id='dim_changsha_store_daily_full',
        doc="店铺日度维度表ETL",
        sql=dim_changsha_store_daily_full,
        hints=hints,
        conn_id='maxcompute_prod'
    )

    # 任务3: 完成标记
    complete_task = BashOperator(
        task_id='complete_dimension_etl',
        bash_command='echo "✅ MaxCompute维度表ETL任务完成 $(date)"',
        doc="标记ETL任务完成"
    )
    
    # 设置任务依赖关系
    start_task >> dim_changsha_goods_property_task >> dim_store_history_trade_temp_task >> dim_changsha_store_daily_full_task >> complete_task