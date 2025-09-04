#coding:utf-8
"""Airflow Hook
* MaxComputeHook, Maxcompute hook
"""
import logging
from airflow.models import Connection
from airflow.hooks.base import BaseHook

from  automation.client import MaxComputerClient



logger = logging.getLogger("dags.utils.hooks")


class MaxcomputeHook(BaseHook):
    """
    MaxCompute Hook
    
    Provides a Hook to MaxCompute and methods to execute SQL statements.
    """
    _client = None
    
    def __init__(self, conn_id: str = 'maxcompute_dev'):
        """Init MaxCompute Hook
        
        Args:
            conn_id: Airflow connection ID for MaxCompute
        """
        super().__init__()
        self.conn_id = conn_id
        self.connection = self._get_connection()


    def _get_connection(self) -> Connection:
        """Get Airflow Connection
        
        Returns:
            Connection object
        """
        return Connection.get_connection_from_secrets(self.conn_id)
    

    @property
    def client(self) -> MaxComputerClient:
        """Get MaxComputerClient instance
        
        Returns:
            MaxComputerClient instance
        """
        if self._client is None:
            self._client = MaxComputerClient(
                endpoint=self.connection.extra_dejson.get('endpoint'),
                access_id=self.connection.extra_dejson.get('access_key_id', self.connection.login),
                secret_access_key=self.connection.extra_dejson.get('access_key_secret', self.connection.password),
                project=self.connection.extra_dejson.get('project', self.connection.schema)
            )
        return self._client
    

    def execute_sql(self, sql: str, *, hints: dict) -> None:
        """Execute SQL statement on MaxCompute
        
        Args:
            sql: SQL statement to execute
            hints: Optional execution hints
            
        """
        
        self.client.execute_sql(sql, hints=hints)
        logger.info("SQL execution completed.")