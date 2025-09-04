#coding:utf-8
"""Airflow Operator
* MaxcomputeOperator, Maxcompute Operator
"""
import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from utils.hooks import MaxcomputeHook

logger = logging.getLogger("dags.utils.operator")

class MaxcomputeOperator(BaseOperator):
    """
    MaxCompute Operator
    
    Executes SQL statements on MaxCompute using MaxcomputeHook.
    """
    
    @apply_defaults
    def __init__(self, 
                 sql: str,
                 hints=None,
                 conn_id: str = 'maxcompute_dev',
                 *args, **kwargs):
        """
        Initialize MaxCompute Operator
        
        Args:
            sql: SQL statement to execute
            conn_id: Airflow connection ID for MaxCompute
        """
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.hints = hints
        self.hook = None

    def execute(self, context):
        """
        Execute the SQL statement on MaxCompute
        
        Args:
            context: Airflow execution context
        """
        logger.info(f"Executing SQL [Maxcompute] Start")
        
        if self.hook is None:
            self.hook = MaxcomputeHook(conn_id=self.conn_id)
        
        self.hook.execute_sql(self.sql, hints=self.hints)
        
        logger.info(f"Executing SQL [Maxcompute] Success: \n{self.sql}")