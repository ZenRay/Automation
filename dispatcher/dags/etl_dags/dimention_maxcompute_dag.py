#coding:utf-8
import sys
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# 添加etl_sentence模块路径到Python搜索路径
current_dir = os.path.dirname(os.path.abspath(__file__))
etl_sentence_path = os.path.join(current_dir, 'etl_sentence')
if etl_sentence_path not in sys.path:
    sys.path.insert(0, etl_sentence_path)
    sys.path.insert(0, current_dir)

logger = logging.getLogger("dispatcher.dags")

# 导入etl_sentence模块中的SQL语句
from etl_sentence.maxcompute_sql.dimention import (
    dim_changsha_goods_property_sentence, dim_changsha_store_daily_full, t_changsha_store_trade_t0d
)

# 导入automation模块
from automation.client import MaxComputerClient
from automation import hints

from airflow.providers.alibaba.hooks.maxcompute import MaxComputeHook

def extract():
    """执行Maxcompute SQL任务"""
    try:
        # 获取配置
        conf = {
            "access_id": Variable.get("MAXCOMPUTE_ACCESS_ID"),
            "secret_access_key": Variable.get("MAXCOMPUTE_SECRET_ACCESS_KEY"),
            "project": Variable.get("MAXCOMPUTE_PROJECT_DEV"),
            "endpoint": Variable.get("MAXCOMPUTE_ENDPOINT")
        }
        
        logger.info(f"配置信息: {conf}")
        
        # 执行SQL
        mc_client = MaxComputerClient(**conf)
        mc_client.execute_sql(
            dim_changsha_goods_property_sentence,
            hints=hints
        )
        logger.info(f"Execute SQL Success: {dim_changsha_goods_property_sentence}")
        
    except Exception as e:
        logger.error(f"执行SQL失败: {str(e)}")
        raise

# 创建DAG
with DAG(
    dag_id='dimention_maxcompute_dag',
    description='maxcompute dimention table ETL',
    schedule_interval='0 2 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['maxcompute', 'dimention'],
    default_args={
        'retries': 3,
        'owner': 'ChangShaTeam',
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
        'email_on_retry': False,
        'email': ['renruia07348@biaoguoworks.com'],
        'email_on_success': False,
    }
) as dag:
    
    # 任务1: 开始任务
    t1 = BashOperator(
        task_id='start_job',
        bash_command='echo "Start Job!"'
    )
    
    # 任务2: 执行Maxcompute ETL
    t2 = PythonOperator(
        task_id='execute_maxcompute_etl',
        python_callable=extract,
    )
    
    # 设置任务依赖关系
    t1 >> t2