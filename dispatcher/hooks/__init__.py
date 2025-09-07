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


class LarkSheetsHook(BaseHook):
    """
    Lark Sheets Hook

    Provides a Hook to Lark Sheets and methods to interact with the API.
    """
    _client = None
    
    def __init__(self, conn_id: str = 'lark_app', target_url=None):
        """Init MaxCompute Hook
        
        Args:
            conn_id: Airflow connection ID for MaxCompute
        """
        super().__init__()
        self.conn_id = conn_id
        self.connection = self._get_connection()
        self.target_url = target_url


    def _get_connection(self) -> Connection:
        """Get Airflow Connection
        
        Returns:
            Connection object
        """
        return Connection.get_connection_from_secrets(self.conn_id)
    

    @property
    def client(self) -> LarkSheets:
        """Get LarkSheets instance

        Returns:
            LarkSheets instance
        """
        if self._client is None:
            self._client = LarkSheets(
                app_id=self.connection.json_dejson.get('app_id', self.connection.login),
                app_secret=self.connection.json_dejson.get('app_secret', self.connection.password),
                lark_host=self.connection.json_dejson.get('lark_host', 'https://open.feishu.cn'),
                url=self.target_url
            )
        return self._client
    

    def load_data(self, file_path:str) -> pd.DataFrame:
        """Load Data
        Load data from file into DataFrame

        Args:
            file_path: Path to the file to upload

        Returns:
            DataFrame containing the file data
        """
        if not path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file format. Only .csv and .xlsx are supported.")

        return df
        
    

    def extract_data2sheet_values(
            self, df: pd.DataFrame, columns: list, range_str: str, 
            sheet_title: str, target_url:str = None
        ) -> None:
        """Extract data from DataFrame and update Lark Sheet

        Args:
            df: DataFrame containing the data
            columns: List of columns to extract
            range_str: Range in A1 notation (e.g., "A1:C10")
            sheet_title: Title of the sheet

        Returns:
            None
        """
        # Update target URL if provided
        if target_url is not None:
            self._client.extract_spreadsheet_info(target_url)
            
        sheet_id = self._client.get_sheet_id(sheet_title)
        raw_data = df.loc[:, columns].drop_duplicates().copy()
        
        
        end_col, end_row = parse_sheet_cell(range_str, parse_type="end")
        end_col_num = parse_column2index(end_col)
        
        if end_col_num > self._client._UPDATE_COL_LIMITATION:
            logger.warning("Data column count exceeds limit, splitting required.")
            range_str = []
            range_index = []
            col_range = list(
                range(0, end_col_num, self._client._UPDATE_COL_LIMITATION)
            ) + [end_col_num]
            
            for start, end in zip(col_range[:-1], col_range[1:]):
                range_str.append(f"{parse_index2column(start+1)}:{parse_index2column(end)}")
                range_index.append((start, end))
        else:
            range_index = [(0, end_col_num)]
            
        time.sleep(2)
        # update data
        for range_idx, range_col_idx in zip(range_index, range_str):
            data = []
            split_data = raw_data.iloc[:, range_idx[0]:range_idx[1]].copy()
            data.append(split_data.columns.to_list())
            
            
            for _, item in split_data.iterrows():
                record = []
                for col in split_data.columns:
                    if pd.notna(item.get(col)):
                        record.append(item.get(col) if not isinstance(item.get(col), (Decimal)) else float(item.get(col)))
                    else:
                        record.append(None)
                data.append(record)
            
            
            self._client.batchupdate_values_single_sheet(data, data_range=range_col_idx, sheet_id=sheet_id)
            logger.info(f"Data updated to sheet {sheet_title} in range {range_col_idx}")
            time.sleep(2)