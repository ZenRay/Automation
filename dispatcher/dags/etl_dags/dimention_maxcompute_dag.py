#coding:utf-8
"""
MaxCompute维度表ETL DAG
用于执行维度表的ETL任务，包括商品属性、店铺日度、历史交易等维度表
"""

import sys
import os
import logging
import json
from typing import Dict, Any
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
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
    dim_changsha_goods_property_sentence, 
    dim_changsha_store_daily_full,
    dim_store_history_trade_temp
)

# 导入automation模块
from automation.client import MaxComputerClient
from automation import hints


def build_maxcompute_config(conn_id: str = 'maxcompute_dev') -> Dict[str, Any]:
    """
    构建MaxCompute连接配置
    
    Args:
        conn_id: 连接ID
        
    Returns:
        包含连接配置的字典
        
    Raises:
        ValueError: 配置不完整时抛出
        Exception: 其他错误时抛出
    """
    try:
        # 获取连接配置
        conn = BaseHook.get_connection(conn_id)
        
        # 记录连接基本信息（不包含敏感信息）
        logger.info(f"连接类型: {conn.conn_type}")
        logger.info(f"连接ID: {conn.conn_id}")
        
        # 根据连接类型构建配置
        if conn.conn_type == 'maxcompute':
            # MaxCompute连接类型：从Extra字段解析配置
            extra = json.loads(conn._extra) if conn._extra else {}
            conf = {
                "access_id": extra.get('access_key_id'),
                "secret_access_key": extra.get('access_key_secret'),
                "project": extra.get('project'),
                "endpoint": extra.get('endpoint')
            }
        elif conn.conn_type == 'Generic':
            # Generic连接类型：从Extra字段解析配置
            extra = json.loads(conn.extra) if conn.extra else {}
            conf = {
                "access_id": conn.login,
                "secret_access_key": conn.password,
                "project": extra.get('project'),
                "endpoint": extra.get('endpoint', conn.host)
            }
        else:
            # 其他连接类型：使用标准字段
            conf = {
                "access_id": conn.login,
                "secret_access_key": conn.password,
                "project": conn.schema,
                "endpoint": conn.host
            }
        
        # 验证配置完整性
        required_fields = ['access_id', 'secret_access_key', 'project', 'endpoint']
        missing_fields = [field for field in required_fields if not conf.get(field)]
        
        if missing_fields:
            raise ValueError(f"缺少必要的配置字段: {', '.join(missing_fields)}")
        
        # 确保endpoint包含协议
        if conf['endpoint'] and not conf['endpoint'].startswith(('http://', 'https://')):
            conf['endpoint'] = f"https://{conf['endpoint']}"
        
        logger.info(f"MaxCompute配置构建成功: project={conf['project']}, endpoint={conf['endpoint']}")
        return conf
        
    except Exception as e:
        logger.error(f"构建MaxCompute配置失败: {str(e)}")
        raise


# 全局MaxCompute客户端
_mc_client = None


def get_maxcompute_client() -> MaxComputerClient:
    """
    获取MaxCompute客户端实例（单例模式）
    
    Returns:
        MaxCompute客户端实例
    """
    global _mc_client
    if _mc_client is None:
        conf = build_maxcompute_config()
        _mc_client = MaxComputerClient(**conf)
        logger.info("MaxCompute客户端创建成功")
    return _mc_client


def execute_sql_task(sql_sentence: str, task_name: str, hints: Dict[str, Any] = None) -> bool:
    """
    执行单个SQL任务
    
    Args:
        sql_sentence: SQL语句
        task_name: 任务名称（用于日志）
        hints: 执行提示参数
        
    Returns:
        执行成功返回True
        
    Raises:
        Exception: 执行失败时抛出
    """
    try:
        logger.info(f"开始执行 {task_name}...")
        
        # 获取客户端并执行SQL
        client = get_maxcompute_client()
        client.execute_sql(sql_sentence, hints=hints or {})
        
        logger.info(f"✅ {task_name} 执行完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ {task_name} 执行失败: {str(e)}")
        raise


def execute_dimension_etl():
    """执行维度表ETL任务（按依赖顺序）"""
    try:
        logger.info("🚀 开始执行MaxCompute维度表ETL任务...")
        
        # 按依赖顺序执行ETL任务
        etl_tasks = [
            {
                'sql': dim_changsha_goods_property_sentence,
                'name': '商品属性维度表ETL',
                'description': '构建商品属性维度表，为后续分析提供基础数据'
            },
            {
                'sql': dim_changsha_store_daily_full,
                'name': '店铺日度维度表ETL',
                'description': '构建店铺日度维度表，依赖商品属性表完成'
            },
            {
                'sql': dim_store_history_trade_temp,
                'name': '历史交易维度表ETL',
                'description': '构建历史交易维度表，依赖店铺日度表完成'
            }
        ]
        
        # 顺序执行，确保依赖关系
        for i, task in enumerate(etl_tasks, 1):
            logger.info(f"📋 任务 {i}/{len(etl_tasks)}: {task['description']}")
            execute_sql_task(task['sql'], task['name'], hints)
            
            # 任务间短暂等待，确保数据一致性
            if i < len(etl_tasks):
                logger.info("⏳ 等待数据同步...")
        
        logger.info("🎉 所有维度表ETL任务执行完成!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 维度表ETL任务执行失败: {str(e)}")
        raise

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
    dimension_etl_task = PythonOperator(
        task_id='execute_dimension_etl',
        python_callable=execute_dimension_etl,
        doc="执行维度表ETL任务，包括商品属性、店铺日度、历史交易等"
    )
    
    # 任务3: 完成标记
    complete_task = BashOperator(
        task_id='complete_dimension_etl',
        bash_command='echo "✅ MaxCompute维度表ETL任务完成 $(date)"',
        doc="标记ETL任务完成"
    )
    
    # 设置任务依赖关系
    start_task >> dimension_etl_task >> complete_task