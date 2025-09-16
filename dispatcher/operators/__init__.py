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
    parse_column2index, parse_index2column, parse_sheet_cell, offset_sheet_cell
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
        
        # Always create a new hook instance for each execution
        self.hook = MaxcomputeHook(conn_id=self.conn_id)
        
        hints = context.get("params", {}).get('hints')
        file = context.get("params", {}).get('file')
          
        if hints is None:
            hints = self.hints
        
        # Get a fresh MaxCompute client for this execution
        client = self.hook.get_client()

        if file is not None:
            # Execute SQL and save to file
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
        columns = kwargs.get("columns")
        file = kwargs.get("file")
        start_cell = kwargs.get("start_cell", "A1")
        batch_size = kwargs.get("batch_size", 0)
        
        # refresh client information
        client.extract_spreadsheet_info(target_url)
        client.extract_sheets(client.spread_sheet)
        
        if target_url is None:
            raise ValueError("Argument 'target_url' is required for Lark Sheets.")

        if sheet_title is None:
            raise ValueError("Argument 'sheet_title' is required for Lark Sheets.")

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
        
        # Filter Query
        filter_query = kwargs.get("filter_query")
        if filter_query is not None:
            try:
                df = df.query(filter_query).copy()
            except Exception as e:
                raise ValueError(f"Error applying filter query '{filter_query}': {e}")
            
        # Fix Date Value to Int
        if '日期' in df.columns:
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce').apply(
                lambda x: x - client._START_DATE if pd.notna(x) else x
            ).dt.days

        logger.info(f"Data file ({file}) read success")
        # adjust columns
        if columns is None:
            columns = df.columns.to_list()
             
        self._extract_data2sheet_values(
            df=df,
            columns=columns,
            start_cell=start_cell,
            sheet_title=sheet_title,
            lark_sheets=client,
            batch_size=batch_size
        )
        
        end_cell = offset_sheet_cell(start_cell, offset_col=len(columns)-1, offset_row=len(df)+1)
        logger.info(
            f"Single file({file}) Send to Lark Sheet Success:\n"
            f"\tTarget URL: {target_url}\n"
            f"\tSheet Title: {sheet_title}\n"
            f"\tRange: {start_cell}:{end_cell}\n"
            f"\tColumns: {columns}\n"
        )

    def _extract_data2sheet_values(self, df, columns, start_cell, sheet_title, lark_sheets, batch_size=0):
        """Extract DataFrame to Lark Sheet Values
        
        Args:
            df: DataFrame to send
            columns: Columns to extract
            start_cell: Starting cell in the sheet
            sheet_title: Title of the sheet
            lark_sheets: LarkSheets client instance
            batch_size: Number of columns to send in each batch
        """
        if len(columns) > lark_sheets._UPDATE_COL_LIMITATION or batch_size > 0:
            logger.warning("Data column count exceeds limit or specified batch size, splitting required.")
            if batch_size == 0:
                batch_size = 20
            batch_indexes = list(range(0, len(columns) + 1, batch_size)) + [len(columns) + 1]
        else:
            batch_indexes = [0, len(columns) + 1]
            
        sheet_start_col, sheet_start_row = parse_sheet_cell(start_cell, parse_type="start")
        start_column_index = batch_indexes[0]
        for batch in batch_indexes[1:]:
            # parse data records and columns
            batch_columns = columns[start_column_index:batch]
            data = [df.loc[:, batch_columns].columns.to_list()]
            data += self._df2record(df.loc[:, batch_columns])

            start_cell = f"{parse_index2column(parse_column2index(sheet_start_col) + start_column_index)}{sheet_start_row}"
            end_cell = f"{parse_index2column(parse_column2index(sheet_start_col) + start_column_index + len(batch_columns) - 1)}{sheet_start_row + len(data) - 1}"
            data_range = f"{start_cell}:{end_cell}"

            lark_sheets.batchupdate_values_single_sheet(
                data, data_range=data_range, sheet_id=lark_sheets.get_sheet_id(sheet_title)
            )

            logger.debug(f"Batch columns {batch_columns} sent to range {data_range}")
            # next batch
            start_column_index = batch
            time.sleep(2)
            
    def _df2record(self, df):
        """Convert DataFrame to list of records

        Args:
            df: DataFrame to convert

        Returns:
            List of records
        """
        records = []
        for _, item in df.iterrows():
            record = []
            for col in df.columns:
                if pd.notna(item.get(col)):
                    record.append(item.get(col) if not isinstance(item.get(col), (Decimal)) else float(item.get(col)))
                else:
                    record.append(None)
            records.append(record)
        return records

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

