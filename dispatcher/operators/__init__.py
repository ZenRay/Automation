#coding:utf-8
"""Airflow Operator
* MaxcomputeOperator, Maxcompute Operator
"""
import logging
import time
import sys
import pandas as pd

from decimal import Decimal
from os import path

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults




from dispatcher.hooks import MaxcomputeHook, LarkHook


from automation.client.lark.utils import (
    parse_column2index, parse_index2column, parse_sheet_cell
)

from automation.client.lark import (
    LarkIM, LarkSheets
)

from automation.client.lark.base.im import (
    TextMessage, ImageMessage, FileMessage, 
    StaticInteractiveMessage
)


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
        hints = context.get("params", {}).get('hints')
        file = context.get("params", {}).get('file')
          
        if hints is None:
            hints = self.hints
        
        # Get MaxCompute client
        client = self.hook.client

        if file is not None:
            # Prefer client-provided execute_to_save if available (implemented in automation client)
            client = getattr(self.hook, 'client', None)
            client.execute_to_save(self.sql, file, hints=hints)
            file = path.abspath(file)
        else:
            client.execute_sql(self.sql, hints=hints)
            file = None
        
        logger.info(f"Executing SQL [Maxcompute] Success: \n{self.sql}")
        return file
   
class LarkOperator(BaseOperator):
    """
    Lark Operator
    
    Sends messages using LarkIMHook.
    """
    
    @apply_defaults
    def __init__(self, 
                 conn_id: str = 'lark_app',
                 *args, **kwargs):
        """
        Initialize Lark Operator
        
        Args:
            conn_id: Airflow connection ID for Lark
        """
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.hook = None

    def execute(self, context):
        """Lark Operator Execute
        
        Args:
            context: Airflow execution context
        """
        logger.info(f"Sending message via Lark Start")
        
        if self.hook is None:
            self.hook = LarkHook(conn_id=self.conn_id)
        logger.info(f"Context Params: {context.get('params')}")
        # Get client according to context params
        if context.get("params").get("client_type") is None:
            raise ValueError("Argument 'client_type' is not supported in LarkOperator.Need provide 'im' or 'sheet' instead.")
        client_type = context['params'].get('client_type')
        
        if client_type == "im":
            client = self.hook.im_client
        elif client_type == "sheet":
            client = self.hook.sheet_client
        else:
            raise ValueError("Argument 'client_type' must be either 'im' or 'sheet'.")
        
        if context.get("params").get("kwargs") is None:
            raise ValueError("Need execute kwargs in context params. Argument 'kwargs' is required in LarkOperator.")

        if context.get("params").get("task_type") is None:
            raise ValueError("Need execute task_type in context params. Argument 'task_type' is required in LarkOperator.")

        kwargs = context['params'].get('kwargs')
        task_type = context['params'].get('task_type')

        if client_type == "sheet" and task_type == "single2single":
            self.single2single_update_sheet(client, kwargs)
        if client_type == "im" and task_type == "send_message":
            self.im_send_message(client, kwargs)

    def single2single_update_sheet(self, client, kwargs):
        """Single File to Single Sheet Update
        
        Args:
            client: LarkSheets client instance
            kwargs: Execution parameters from context

        Returns:
            None
        """
        
        target_url = kwargs.get("target_url")
        sheet_title = kwargs.get("sheet_title")
        range_str = kwargs.get("range_str")
        columns = kwargs.get("columns")
        file = kwargs.get("file")
        
        # refresh client information
        client.extract_spreadsheet_info(target_url)
        client.extract_sheets(client.spread_sheet)
        
        if target_url is None:
            raise ValueError("Argument 'target_url' is required for Lark Sheets.")

        if sheet_title is None:
            raise ValueError("Argument 'sheet_title' is required for Lark Sheets.")

        if range_str is None:
            raise ValueError("Argument 'range_str' is required for Lark Sheets.")

        if file is None:
            raise ValueError("Argument 'file' is required for Lark Sheets.")


        if file.endswith(".csv"):
            try:
                sep = kwargs.get("sep")
                df = pd.read_csv(file, sep="," if sep is None else sep)
            except Exception as e:
                raise ValueError(
                    f"Error reading CSV file: {e}"
                        f"1. check if the file ({file}) exists;"
                        f"2. check if the separator ({'default sep' if sep is None else sep}) is correct."
                )
        elif file.endswith(".xlsx"):
            try:
                sheet_name = kwargs.get("sheet_name")
                df = pd.read_excel(file, sheet_name=sheet_name if sheet_name is not None else 0)
            except Exception as e:
                raise ValueError(
                    f"Error reading Excel file: {e}"
                        f"1. check if the file ({file}) exists;"
                        f"2. check if the sheet name is correct."
                )
        else:
            raise ValueError("Unsupported file format. Only .csv and .xlsx are supported.")

        # adjust columns
        if columns is None:
            columns = df.columns.to_list()
        
        self._extract_data2sheet_values(
            df=df,
            columns=columns,
            target_url=target_url,
            range_str=range_str,
            sheet_title=sheet_title,
            lark_sheets=client
        )
    
        logger.info(
            f"Single file({file}) Send to Lark Sheet Success:\n"
            f"\tTarget URL: {target_url}\n"
            f"\tSheet Title: {sheet_title}\n"
            f"\tRange: {range_str}\n"
            f"\tColumns: {columns}\n"
        )
        
    def _extract_data2sheet_values(self, df, columns, target_url, range_str, sheet_title, lark_sheets):
        """Extract data from DataFrame and send to Lark Sheets.
        Args:
            df (pd.DataFrame): The DataFrame containing the data to send.
            columns (list): The list of columns to extract from the DataFrame.
            target_url (str): The URL of the target Lark Sheet.
            range_str (str): The range string for the target sheet.
            sheet_title (str): The title of the target sheet.
            lark_sheets (LarkSheets): The LarkSheets client instance.
        """
        # Update target URL
        lark_sheets.extract_spreadsheet_info(target_url)
        
        sheet_id = lark_sheets.get_sheet_id(sheet_title)
        raw_data = df.loc[:, columns].drop_duplicates().copy()
        
        end_col, end_row = parse_sheet_cell(range_str, parse_type="end")
        end_col_num = parse_column2index(end_col)
        
        if end_col_num > lark_sheets._UPDATE_COL_LIMITATION:
            logger.warning("Data column count exceeds limit, splitting required.")
            range_str = []
            range_index = []
            col_range = list(
                range(0, end_col_num, lark_sheets._UPDATE_COL_LIMITATION)
            ) + [end_col_num]
            
            for start, end in zip(col_range[:-1], col_range[1:]):
                range_str.append(f"{parse_index2column(start+1)}:{parse_index2column(end)}")
                range_index.append((start, end))
        else:
            range_index = [(0, end_col_num)]
            
        time.sleep(2)
        
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
            
            
            lark_sheets.batchupdate_values_single_sheet(data, data_range=range_col_idx, sheet_id=sheet_id)
            time.sleep(2)


    def im_send_message(self, client, kwargs):
        """Send message using LarkIM client
        
        Args:
            client: LarkIM client instance
            kwargs: Execution parameters from context

        Returns:
            None
        """
        receive_id_type = kwargs.get("receive_id_type", None)
        receive_id = kwargs.get("receive_id", None)
        content = kwargs.get("content", None)
        message_type = kwargs.get("message_type", None)

        if receive_id_type is None:
            raise ValueError("Argument 'receive_id_type' is required for Lark IM.")

        if receive_id is None:
            raise ValueError("Argument 'receive_id' is required for Lark IM.")

        if content is None:
            raise ValueError("Argument 'content' is required for Lark IM.")
        
        if message_type is None:
            raise ValueError("Argument 'message_type' is required for Lark IM.")
        elif message_type.lower() == "text":
            message = TextMessage(text=content)
            
            
        message.send_message(client.send_message, receive_id_type=receive_id_type, receive_id=receive_id) 
        
        
        logger.info(
            f"Message sent via Lark IM Success to receiver({receive_id}):\n"
            f"\tContent: {kwargs.get('content')}\n"
        )

