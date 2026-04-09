#coding:utf-8
"""
Maxcompute to Lark Docs Operator
1. Maxcompute ETL
2. Update Data to Lark Docs: Sheet or Multi Dimensional Table
"""


import logging
import time
import pandas as pd
import numpy as np
from decimal import Decimal


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


from dispatcher.hooks import MaxcomputeHook, LarkHook
from automation.utils.common.dataframe import dataframe2record
from automation.client.lark.utils import (
    offset_sheet_cell,
    parse_column2index,
    parse_index2column,
    parse_sheet_cell,
)

from ._lark_sheet_dataframe import (
    apply_lark_sheet_date_column,
    extract_dataframe_to_sheet_values,
)


logger = logging.getLogger(__name__)


class Maxcompute2LarkOperator(BaseOperator):
    """
    Maxcompute to Lark Docs Operator
    1. Maxcompute ETL
    2. Update Data to Lark Application: Sheet, Multi Dimensional Table, or aPaaS Service Workspaces
    3. Send Message to Lark IM, argument `receive_type` is 'user' or 'group', if 'user' the send private message, 
        if 'group' send group message.
    """

    @apply_defaults
    def __init__(self,
        maxcompute_conn_id,
        lark_conn_id,
        *args,
        **kwargs
    ):
        """
        Initialize the Maxcompute to Lark Docs Operator
        """
        # hints 不是 BaseOperator 的标准参数，需要在 super 之前取出
        self.maxcompute_hints = kwargs.pop("hints", None)

        super().__init__(*args, **kwargs)
        self.maxcompute_conn_id = maxcompute_conn_id
        self.lark_conn_id = lark_conn_id

        self.maxcompute_hook = None
        self.lark_hook = None

        
    def execute(self, context):
        """Operator Execute Entry
        
        Args:
        ------------
            context: Airflow context
        """
        sql = self._check_context_params("sql", context)
        
        client_type = self._check_context_params("client_type", context)

        message = context.get('params', {}).get("message", None)

        # Maxcompute ETL
        instance = self._maxcompute_etl(
            sql=sql
            ,context=context
        )

        # Update Lark Multi Dimension Table
        if client_type == "multi":
            url = self._check_context_params("url", context)
            self._update_lark_multi_dimension_table(
                context=context
                ,url=url
                ,instance=instance
            )
        # Update Lark Sheet (row/column chunking in Lark layer, not tied to MC batch size)
        elif client_type == "sheet":
            url = self._check_context_params("url", context)
            self._update_lark_sheet(
                context=context
                ,url=url
                ,instance=instance
            )
        # Update Lark aPaaS Service Workspaces
        elif client_type == "apaas":
            self._update_lark_apaas_service_workspaces(
                context=context
                ,instance=instance
            )
        else:
            msg = f"Lark Client Type '{client_type}' is not supported."
            logger.error(msg)
            raise Exception(msg)

        # Send Lark Message
        if message is not None:
            self._send_lark_message(
                context=context
                ,message=message
            )


    def _check_context_params(self, name, context):
        """Check Context Parameters
        
        Args:
        ------------
            name: Parameter name to check
            context: Airflow context
        
        Results:
        ------------
            Raise Exception if parameter is missing
        """
        params = context.get('params', {})
        if name not in params:
            msg = f"Parameter '{name}' is missing in context params."
            logger.error(msg)
            raise Exception(msg)
        return params[name]

    
    def _maxcompute_etl(self, sql, context, **kwargs):
        """
        Maxcompute ETL
        """
        self.log.info("Start Maxcompute ETL")

        if not self.maxcompute_hook:
            self.maxcompute_hook = MaxcomputeHook(
                conn_id=self.maxcompute_conn_id
            )
        client = self.maxcompute_hook.get_client()
        
        # extract parameters from context
        params = context.get('params', {})
        hints  = params.get("hints")

        if all([i is None for i in [hints, self.maxcompute_hints]]):
            msg = "Maxcompute hints is missing. Please provide hints in either operator args or context params."
            logger.error(msg)
            raise Exception(msg)
        
        hints = hints if hints else self.maxcompute_hints

        instance = client.execute_sql(
            sql,
            hints=hints,
            is_async=False,
            interactive=False
        )
        instance.wait_for_success()
        logger.info("Maxcompute ETL Completed:\n %s", sql)

        return instance
    

    def _update_lark_multi_dimension_table(self, context, url: str, instance=None, **kwargs):
        """Update Lark Multi Dimension Table
        
        Args:
        ------------
            context: Airflow context
            url: Lark Multi Dimension Table URL
            instance: Maxcompute Execution SQL Instance
        """
        logger.info("Start Update Lark Multi Dimension Table")

        # extract parameters from context
        table_name = self._check_context_params("table_name", context)
        params = context.get('params', {})
        
        table_id = params.get("table_id")
        is_clear = params.get("is_clear", False)
        filter = params.get("filter", None)
        
        # refresh client information
        if not self.lark_hook:
            self.lark_hook = LarkHook(
                conn_id=self.lark_conn_id
            )
        client = self.lark_hook.multi_client

        
        # refresh client information
        client.extract_app_information(url=url)
        client.extract_table_information(url=url)

        # get table id by table name
        if table_id is None and table_name is not None:
            table_id = client.tables_map.get(table_name, None)
        
        if table_id is None:
            msg = f"Table ID for Table Name '{table_name}' is missing."
            logger.error(msg)
            raise Exception(msg)

        # clear existing records
        if is_clear:
            records_id_list = []
            if filter is None:
                request_records = client.request_records_generator(url=url, table_id=table_id)
            elif isinstance(filter, dict):
                request_records = client.request_records_generator(url=url, filter=filter, table_id=table_id)
            else:
                logger.error("Filter parameter must be a dictionary.")
                raise ValueError("Filter parameter must be a dictionary.")
            
            for records in request_records: 
                records = records.get("data", {}).get("items", [])
                if records is not None and len(records) > 0:
                    records_id = [
                        record.get("record_id") for record in records if "record_id" in record
                    ]
                    records_id_list.extend(records_id)

            index = list(range(0, len(records_id_list), client.DELETE_RECORD_LIMITATION))
            for start, end in zip(index, index[1:] + [len(records_id_list)]):
                client.delete_batch_records(
                    url=url
                    ,table_id=table_id
                    ,records_id=records_id_list[start:end]
                )
                time.sleep(2)

        # Maxcompute Execution Instance
        if instance is None:
            msg = "Maxcompute execution instance is missing."
            logger.error(msg)
            raise Exception(msg)

        # Update records
        for batch in instance.iter_pandas(batch_size=client.ADD_RECORD_LIMITATION):
            index = list(range(0, batch.shape[0], client.ADD_RECORD_LIMITATION))
            data = dataframe2record(batch, type="dict")
            for start, end in zip(index, index[1:] + [batch.shape[0]]):
                records = []
                for record in data[start:end]:
                    records.append({"fields": record})
                client.add_batch_records(
                    records=records
                    ,url=url
                    ,table_id=table_id
                    ,table_name=table_name
                )
                time.sleep(2)
                
        logger.info(
            f"Maxcompute ETL Records Send to Multi Dimension Table Success:\n"
            f"\tTarget URL: {url}\n"
            f"\tTable Title: {table_name}\n"
        )

    def _update_lark_sheet(self, context, url: str, instance=None, **kwargs):
        """Write MaxCompute SQL result rows to a Lark spreadsheet via sheet_client.

        Result volume is determined by SQL only. Rows are streamed with
        ``instance.iter_pandas()`` using PyODPS tunnel defaults (``batch_size`` defaults to
        ``options.tunnel.read_row_batch_size`` when omitted), not an application cap.

        Feishu per-request limits (e.g. 5000×100) are handled in
        ``extract_dataframe_to_sheet_values`` / ``LarkSheets.batchupdate_values_single_sheet``.

        When ``is_clear`` is True, existing values in ``clear_range`` are cleared before writing
        (via ``LarkSheets.clear_sheet_values``), analogous to multi-table ``is_clear``.
        If ``clear_range`` is not provided, the operator infers clear ranges from
        target sheet grid metadata (column_count / row_count) and clears in
        row/column chunks to satisfy Feishu clear limits.
        """
        logger.info("Start Update Lark Sheet")

        sheet_title = self._check_context_params("sheet_title", context)
        params = context.get("params", {})
        start_cell = params.get("start_cell", "A1")
        columns = params.get("columns")
        batch_size = params.get("batch_size", 0)
        filter_query = params.get("filter_query")
        is_clear = params.get("is_clear", False)
        clear_range = params.get("clear_range")

        if not self.lark_hook:
            self.lark_hook = LarkHook(conn_id=self.lark_conn_id)
        client = self.lark_hook.sheet_client

        client.extract_spreadsheet_info(url)
        
        # 重试逻辑：处理飞书服务端临时错误
        max_retries = 3
        retry_delay = 5  # 秒
        sheets_data = None
        for attempt in range(max_retries):
            try:
                sheets_data = client.extract_sheets(client.spread_sheet)
                break
            except Exception as e:
                error_msg = str(e)
                # 如果是服务端错误（1315201 或 5xx），重试；否则直接抛出
                if ('1315201' in error_msg or 'Server Error' in error_msg) and attempt < max_retries - 1:
                    logger.warning(f"飞书服务端错误，{retry_delay}秒后重试（第{attempt + 1}次失败）: {error_msg}")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"飞书 API 调用失败: {error_msg}")
                    raise

        if instance is None:
            msg = "Maxcompute execution instance is missing."
            logger.error(msg)
            raise Exception(msg)

        sheet_id = client.get_sheet_id(sheet_title)
        if sheet_id is None:
            raise ValueError(f"Sheet '{sheet_title}' not found in spreadsheet: {url}")
        sheet_meta = next(
            (
                item
                for item in (sheets_data or [])
                if item.get("sheet_id") == sheet_id or item.get("title") == sheet_title
            ),
            None,
        )

        if is_clear:
            if clear_range is not None:
                logger.info("Clearing sheet range before write: %s", clear_range)
                client.clear_sheet_values(clear_range, sheet_id=sheet_id)
                time.sleep(2)
            else:
                start_col, _ = parse_sheet_cell(start_cell, parse_type="single")
                start_col_idx = parse_column2index(start_col)
                col_limit = getattr(client, "_CLEAR_COL_LIMITATION", client._UPDATE_COL_LIMITATION)
                row_limit = getattr(client, "_CLEAR_ROW_LIMITATION", client._UPDATE_ROW_LIMITATION)
                total_clear_cols = 0

                # Infer clear width from target sheet metadata first.
                if sheet_meta is not None:
                    total_clear_cols = (
                        sheet_meta.get("grid_properties", {}).get("column_count", 0) or 0
                    )

                # Fallback: when metadata is unavailable, use write columns if provided.
                if total_clear_cols == 0 and columns is not None and len(columns) > 0:
                    total_clear_cols = len(columns)
                clear_end_row = 50000
                if sheet_meta is not None:
                    clear_end_row = (
                        sheet_meta.get("grid_properties", {}).get("row_count", 0) or clear_end_row
                    )

                if total_clear_cols <= 0:
                    raise ValueError(
                        "Unable to infer clear columns when `is_clear=True` and `clear_range` is not provided. "
                        "Please pass `clear_range` explicitly or provide `columns`."
                    )

                clear_ranges = []
                for col_start in range(0, total_clear_cols, col_limit):
                    col_end = min(col_start + col_limit, total_clear_cols) - 1
                    clear_start_col = parse_index2column(start_col_idx + col_start)
                    clear_end_col = parse_index2column(start_col_idx + col_end)
                    # Include row bounds and split by row limit to satisfy clear API constraints.
                    for row_start in range(1, clear_end_row + 1, row_limit):
                        row_end = min(row_start + row_limit - 1, clear_end_row)
                        clear_ranges.append(
                            f"{clear_start_col}{row_start}:{clear_end_col}{row_end}"
                        )

                for infer_range in clear_ranges:
                    logger.info("Clearing inferred sheet range before write: %s", infer_range)
                    client.clear_sheet_values(infer_range, sheet_id=sheet_id)
                    time.sleep(2)

        columns_fixed = list(columns) if columns is not None else None
        current_cell = start_cell
        include_header = True
        total_rows_written = 0
        batches = 0

        for batch_df in instance.iter_pandas():
            df = batch_df.copy()
            if filter_query is not None:
                try:
                    df = df.query(filter_query).copy()
                except Exception as e:
                    raise ValueError(f"Error applying filter query '{filter_query}': {e}") from e

            if df.shape[0] == 0:
                continue

            apply_lark_sheet_date_column(df, client)

            if columns_fixed is None:
                columns_fixed = df.columns.to_list()
            else:
                df = df.reindex(columns=columns_fixed)

            extract_dataframe_to_sheet_values(
                df=df,
                columns=columns_fixed,
                start_cell=current_cell,
                sheet_title=sheet_title,
                lark_sheets=client,
                batch_size=batch_size,
                include_header=include_header,
            )

            n = df.shape[0]
            rows_this = (1 + n) if include_header else n
            total_rows_written += rows_this
            current_cell = offset_sheet_cell(current_cell, offset_row=rows_this)
            include_header = False
            batches += 1

        if batches == 0:
            logger.warning("No rows written to Lark Sheet (all iterator batches empty after filter).")
            return

        sheet_start_col, sheet_start_row = parse_sheet_cell(start_cell, parse_type="start")
        end_row = sheet_start_row + total_rows_written - 1
        end_col = parse_index2column(
            parse_column2index(sheet_start_col) + len(columns_fixed) - 1
        )
        end_cell = f"{end_col}{end_row}"

        logger.info(
            "Maxcompute ETL rows sent to Lark Sheet success:\n"
            "\tTarget URL: %s\n"
            "\tSheet Title: %s\n"
            "\tRange: %s:%s\n"
            "\tColumns: %s\n",
            url,
            sheet_title,
            start_cell,
            end_cell,
            columns_fixed,
        )

    def _update_lark_apaas_service_workspaces(self, context, instance=None, **kwargs):
        """Update Lark aPaaS Service Workspaces
        
        Args:
        ------------
            context: Airflow context
            instance: Maxcompute Execution SQL Instance
        """
        logger.info("Start Update Lark aPaaS Service Workspaces")
        # extract parameters from context
        table_name = self._check_context_params("table_name", context)
        
        
        params = context.get('params', {})
        filter_conditions = params.get("filter_conditions", None)
        # specify the preference for conflict resolution
        prefer = params.get("prefer", "resolution=merge-duplicates, missing=default")
        workspace_id = params.get("workspace_id", None)
        workspace_url = params.get("workspace_url", None)

        # refresh client information
        if not self.lark_hook:
            self.lark_hook = LarkHook(
                conn_id=self.lark_conn_id
            )
        client = self.lark_hook.apaas_client

        if workspace_id is None:
            if workspace_url is None:
                msg = f"Missing Workspace URL and Workspace ID in context params. Those params must exist lest one"
                logger.error(msg)
                raise Exception(msg)
            else:
                workspace_id = client._extract_workspace_from_url(workspace_url)
        
        
        
        # if filter_conditions is not None then clear the existing records
        if filter_conditions is not None and len(filter_conditions) > 0:
            client.delete_table_records(
                table_name=table_name,
                filter_conditions=filter_conditions,
                workspace_id=workspace_id
            )
            logger.info(f"Existing Records Deleted from aPaaS Service Workspace Table{workspace_id}:{table_name} with Filter Conditions: {filter_conditions}".format(
                workspace_id=workspace_id,
                table_name=table_name,
                filter_conditions=filter_conditions
            ))
        
        # Update records
        if instance is not None:
            for batch in instance.iter_pandas(batch_size=client.ADD_RECORD_LIMITATION):
                # 1. convert Decimal type to float type
                for col in batch.columns:
                    if batch[col].dtype == 'object':
                        # Check if the column contains Decimal type
                        try:
                            if batch[col].apply(lambda x: isinstance(x, Decimal) if pd.notna(x) else False).any():
                                batch[col] = batch[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
                        except:
                            pass
                
                # 2. replace all NULL/NaN values with None
                batch = batch.replace({np.nan: None, pd.NaT: None})
                
                records = batch.to_dict(orient="records")
                
                client.add_table_records(
                    workspace_id=workspace_id
                    ,table_name=table_name
                    ,records=records
                    ,columns=batch.columns.tolist()
                    ,prefer=prefer
                )
            logger.info(f"Maxcompute ETL Records Send to aPaaS Service Workspace Table {workspace_id}:{table_name} Success".format(
                workspace_id=workspace_id,
                table_name=table_name
            ))
            time.sleep(2)
        logger.info(
            f"Lark aPaaS Service Workspace Table Records Delete And Update Success:\n"
            f"\tWorkspace ID: {workspace_id}\n"
            f"\tTable Name: {table_name}\n"
            f"\tFilter Conditions: {filter_conditions}\n"
        )
        
        
        
    
    def _send_lark_message(self, context, message: str, **kwargs):
        """Send Lark Message
        
        Args:
        ------------
            context: Airflow context
            message: Message content to send
        """
        logger.info("Start Send Lark Message")
        
        # extract parameters from context
        receive_type = self._check_context_params("receive_type", context)
        message_type = self._check_context_params("message_type", context)
        params = context.get('params', {})
        receive_id = params.get("receive_id", context)

        if receive_type == "group":
            receive_type_id = "chat_id"
        elif receive_type == "user":
            receive_type_id = "open_id"
        else:
            msg = f"Lark Receive Type '{receive_type}' is not supported."
            logger.error(msg)
            raise Exception(msg)
        
        # refresh client information
        if not self.lark_hook:
            self.lark_hook = LarkHook(
                conn_id=self.lark_conn_id
            )
        client = self.lark_hook.im_client

        client.send_message(
            receive_id=receive_id
            ,receive_type=receive_type
            ,receive_type_id=receive_type_id
            ,message=message
            ,message_type=message_type
        )

        logger.info(
            f"Lark Message Sent Success:\n"
            f"\tReceiver ID: {receive_id}\n"
            f"\tMessage: {message}\n"
        )