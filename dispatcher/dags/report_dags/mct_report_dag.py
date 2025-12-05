#coding:utf-8
"""Merchant Center Report DAG
* 用于生成商家中心报表，包括商品、店铺、交易等数据的汇总

"""


import sys
import os
from os import path
import logging
from datetime import datetime, timedelta



from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

from airflow.models import Connection


# Maxcompute SQL Statements
from dispatcher.etl_sentence.report_sql.merchant import (
    mct_roi_report_sentence
)

# from utils.operator import MaxcomputeOperator, LarkOperator
from dispatcher.operators import MaxcomputeOperator, LarkOperator

# Maxcompute Hints
hints = {
    "odps.sql.allow.fullscan": True,
    "odps.sql.type.system.odps2": True,
    "odps.sql.decimal.odps2": True,
    "odps.sql.hive.compatible": True,
    "odps.odtimizer.dynamic.filter.dpp.enable": True,
    "odps.odtimizer.enable.dynamic.filter": True,
    "odps.sql.python.version": "cp311",
}


logger = logging.getLogger("dispatcher.dags.report_dags")
data_path = path.abspath("/opt/airflow/data")

# Default arguments for the DAG
DAG_CONFIG = {
    'dag_id': 'report_mct_dag',
    'description': 'MaxCompute商家中心报表任务 - 包含商品、店铺、交易等数据的汇总',
    'schedule_interval': '00 11 * * *',  # 每天1100执行
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
    'tags': ['maxcompute', 'report', 'changsha'],
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

# mct_roi_target_url = "https://bggc.feishu.cn/wiki/Jm36w0fKqiuxZikTDcxcaVu6ndd"

mct_roi_target_url = "https://bggc.feishu.cn/wiki/Jm36w0fKqiuxZikTDcxcaVu6ndd?sheet=a05b50"
save_file = path.join(data_path, "./mct_roi_report_data.csv")

with DAG(**DAG_CONFIG) as dag:
    # start_task
    start_task = BashOperator(
        task_id='start',
        bash_command='echo "Starting Merchant Report DAG at $(date)"',
    )
    
    
    # extract data
    extract_mct_roi_task = MaxcomputeOperator(
        task_id='extract_mct_roi_report_data',
        doc="商家ROI报表数据抽取",
        sql=mct_roi_report_sentence,
        hints=hints,
        conn_id='maxcompute_dev',
        params={
            "hints": hints,
            "file": path.join(data_path, "./mct_roi_report_data.csv")
        }
    )

    # update lark sheet
    update_mct_roi_raw_data_task = LarkOperator(
        task_id="pdate_mct_roi_raw_data",
        doc="更新商家ROI报表数据到飞书表格，原始数据",
        conn_id="lark_app_prod",
        params={
            "client_type": "sheet",
            "task_type": "single2single",
            "kwargs": {
                "target_url": mct_roi_target_url,
                "sheet_title": "原始数据",
                # "range_str": "A:AO",
                "start_cell": "A1",
                "batch_size": 20, 
                "file": save_file,
                # "drop_duplicates": False,
                # "loop": True
                #  "columns": [...], # 可选参数，如不指定则使用数据文件的列名
            }
        }
    )
    

    # update
    update_mct_roi_config_sku_task = LarkOperator(
        task_id="update_mct_roi_config_sku_task",
        doc="更新商家ROI报表数据到飞书表格，更新商品配置数据",
        conn_id="lark_app_prod",
        params={
            "client_type": "sheet",
            "task_type": "single2single",
            "kwargs": {
                "target_url": mct_roi_target_url,
                "sheet_title": "Config",
                # "range_str": "A:F",
                "start_cell": "A1",
                "file": save_file,
                "columns":["商品名称", "商品id",  "商家id", "四级类目名称",  "四级类目id", "最近下单间隔天数"],
                "filter_query": "商品名称 != '总计'"
                # "loop": False,
                # "drop_duplicates": True
            }
        }
    )
    
    # update
    update_mct_roi_config_cat4_mct_task = LarkOperator(
        task_id="update_mct_roi_config_cat4_mct_task",
        doc="更新商家ROI报表数据到飞书表格，更新商家类目配置数据",
        conn_id="lark_app_prod",
        params={
            "client_type": "sheet",
            "task_type": "single2single",
            "kwargs": {
                "target_url": mct_roi_target_url,
                "sheet_title": "Config",
                # "range_str": "H:K",
                "start_cell": "H1",
                "file": save_file,
                "columns":["四级类目id", "四级类目名称", "商家id", "商家名称"],
                "filter_query": "四级类目名称 != '总计' and 商家名称 != '总计'"
                # "loop": False,
                # "drop_duplicates": True
            }
        }
    )

    # update
    update_mct_roi_config_cat4_task = LarkOperator(
        task_id="update_mct_roi_config_cat4_task",
        doc="更新商家ROI报表数据到飞书表格，更新类目配置数据",
        conn_id="lark_app_prod",
        params={
            "client_type": "sheet",
            "task_type": "single2single",
            "kwargs": {
                "target_url": mct_roi_target_url,
                "sheet_title": "Config",
                # "range_str": "Q:R",
                "start_cell": "Q1",
                "file": save_file,
                "columns":['四级类目名称', "四级类目id"],
                "filter_query": "四级类目名称 != '总计'"
            }
        }
    )
    

    # update
    update_mct_roi_config_mct_task = LarkOperator(
        task_id="update_mct_roi_config_mct_task",
        doc="更新商家ROI报表数据到飞书表格，更新商家配置数据",
        conn_id="lark_app_prod",
        params={
            "client_type": "sheet",
            "task_type": "single2single",
            "kwargs": {
                "target_url": mct_roi_target_url,
                "sheet_title": "Config",
                "range_str": "T:U",
                "file": save_file,
                 "columns":["商家名称", "商家id"]
            }
        }
    )
    
     
    # send msg
    send_message_task = LarkOperator(
        task_id="notify_mct_roi_report",
        doc="发送商家ROI更新提醒信息到飞书",
        conn_id="lark_app_prod",
        params={
            "client_type": "im",
            "task_type": "send_message",
            "kwargs": {
                "receive_id_type": "open_id",
                "receive_id": "ou_f986b2f0354ea6f61492ba24aa3a8a22",
                "content": "商家中心ROI报表数据已生成，请查收[链接](https://bggc.feishu.cn/wiki/Jm36w0fKqiuxZikTDcxcaVu6ndd?sheet=a05b50)！",
                "message_type": "text"
            }
        }
    )

# 杨银涛 ou_f986b2f0354ea6f61492ba24aa3a8a22
# me ou_1a69b48a5944c26cc19b0272be54eabe

    start_task >> extract_mct_roi_task >> update_mct_roi_raw_data_task >> \
    update_mct_roi_config_sku_task >> update_mct_roi_config_cat4_mct_task >> \
    update_mct_roi_config_cat4_task >>  update_mct_roi_config_mct_task >> send_message_task