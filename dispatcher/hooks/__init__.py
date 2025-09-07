#coding:utf-8
"""Airflow Hook
* MaxComputeHook, Maxcompute hook
"""

import logging
import time
import pandas as pd
from decimal import Decimal
from os import path

from airflow.models import Connection
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException

from  automation.client import (
    MaxComputerClient, LarkIM, LarkSheets
)


from automation.client.lark.utils import (
    parse_column2index, parse_index2column, parse_sheet_cell
)



logger = logging.getLogger("dispatcher.hooks")


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
        try:
            # Prefer secrets backend, but fall back to metadata DB if not found there
            return Connection.get_connection_from_secrets(self.conn_id)
        except AirflowNotFoundException:
            return self.get_connection(self.conn_id)
    

    @property
    def client(self) -> MaxComputerClient:
        """Get MaxComputerClient instance
        
        Returns:
            MaxComputerClient instance
        """
        if self._client is None:
            # Support both possible key names stored in Connection.extra
            secret = (
                self.connection.extra_dejson.get('access_key_secret')
                or self.connection.extra_dejson.get('secret_access_key')
                or self.connection.password
            )


            self._client = MaxComputerClient(
                endpoint=self.connection.extra_dejson.get('endpoint'),
                access_id=self.connection.extra_dejson.get('access_key_id', self.connection.login),
                secret_access_key=secret,
                project=self.connection.extra_dejson.get('project', self.connection.schema)
            )
        return self._client
    

    def execute_sql(self, sql: str, *, hints: dict, file: str=None) -> None:
        """Execute SQL statement on MaxCompute
        
        If file is provided, save the results to the file.
        
        Args:
            sql: SQL statement to execute
            hints: Optional execution hints
            file: Optional file path to save results
        """
        
        self.client.execute_sql(sql, hints=hints)
        logger.info("SQL execution completed.")
        
        # TODO: Implement file handling if needed
        # if file is not None:
        #     self.client
        
    def execute(self, context):
        """Execute method for Airflow Operator
        
        Args:
            context: Airflow context
        """
        
        sql = context['params'].get('sql')
        hints = context['params'].get('hints', {})
        file = context['params'].get('file', None)
        
        if not sql:
            raise ValueError("SQL statement is required.")
        
        if file:
            self.client.execute_to_save(sql, file, hints=hints)
        else: 
            self.client.execute_sql(sql, hints=hints)
        logger.info("MaxCompute SQL execution task completed.")

    
class LarkHook(BaseHook):
    """
    Lark Hook

    Provides a Hook to Lark and methods to interact with the API.
    """
    _clients = {
        "im": None,
        "sheet": None
    }
    
    def __init__(self, conn_id: str = 'lark_app'):
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
    def sheet_client(self) -> LarkSheets:
        """Get LarkSheets instance

        Returns:
            LarkSheets instance
        """

        if self._clients["sheet"] is None:
            self._clients["sheet"] = LarkSheets(
                app_id=self.connection.json_dejson.get('app_id', self.connection.login),
                app_secret=self.connection.json_dejson.get('app_secret', self.connection.password),
                lark_host=self.connection.json_dejson.get('lark_host', 'https://open.feishu.cn'),
                url=None
            )
        return self._clients["sheet"]


    @property
    def im_client(self) -> LarkIM:
        """Get LarkIM instance

        Returns:
            LarkIM instance
        """
        if self._clients["im"] is None:
            self._clients["im"] = LarkIM(
                app_id=self.connection.json_dejson.get('app_id', self.connection.login),
                app_secret=self.connection.json_dejson.get('app_secret', self.connection.password),
                lark_host=self.connection.json_dejson.get('lark_host', 'https://open.feishu.cn'),
            )
        return self._clients["im"]

