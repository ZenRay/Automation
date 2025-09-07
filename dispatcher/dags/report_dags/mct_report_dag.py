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
current_dir = path.dirname(path.abspath(__file__))

# Default arguments for the DAG
DAG_CONFIG = {
    'dag_id': 'report_mct_dag',
    'description': 'MaxCompute商家中心报表任务 - 包含商品、店铺、交易等数据的汇总',
    'schedule_interval': '20 8 * * *',  # 每天0820执行
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
            "file": path.join(current_dir, "./mct_roi_report_data.csv")
        }
    )

    # update lark sheet
    update_mct_roi_lark_sheet_task = LarkOperator(
        task_id="update_mct_roi_lark_sheet",
        doc="更新商家ROI报表数据到飞书表格",
        params={
            "client_type": "sheet",
            "task_type": "single2single",
            "kwargs": {
                "target_url": "https://test-datkt5aa0s25.feishu.cn/wiki/I4cRwuyAMiXFhRkFkLVcM0yonEb?sheet=XhYmqm",
                "sheet_title": "原始数据",
                "range_str": "A:AO",
                "file": path.join(current_dir, "./mct_roi_report_data.csv"),
                #  "columns": [...], # 可选参数，如不指定则使用数据文件的列名
            }
        }
    )

    # send msg
    send_message_task = LarkOperator(
        task_id="notify_mct_roi_report",
        doc="发送商家ROI更新提醒信息到飞书",
        conn_id="lark_app",
        params={
            "client_type": "im",
            "task_type": "send_message",
            "kwargs": {
                "receive_id_type": "open_id",
                "receive_id": "ou_f22c565bf4f3ca8b3fa5bd2f20039949",
                "content": "商家中心ROI报表数据已生成，请查收！",
                "message_type": "text"
            }
        }
    )


    start_task >> extract_mct_roi_task >> update_mct_roi_lark_sheet_task >> send_message_task