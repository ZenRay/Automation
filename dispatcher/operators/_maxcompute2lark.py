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


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


from dispatcher.hooks import MaxcomputeHook, LarkHook
from automation.utils.common.dataframe import dataframe2record


logger = logging.getLogger(__name__)


class Maxcompute2LarkOperator(BaseOperator):
    """
    Maxcompute to Lark Docs Operator
    1. Maxcompute ETL
    2. Update Data to Lark Docs: Sheet or Multi Dimensional Table
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
        super().__init__(*args, **kwargs)
        self.maxcompute_conn_id = maxcompute_conn_id
        self.lark_conn_id = lark_conn_id

        self.maxcompute_hook = None
        self.lark_hook = None

        # maxcompute hints
        self.maxcompute_hints = kwargs.get("hints", None)

        
    def execute(self, context):
        """Operator Execute Entry
        
        Args:
        ------------
            context: Airflow context
        """
        sql = self._check_context_params("sql", context)
        url = self._check_context_params("url", context)
        client_type = self._check_context_params("client_type", context)

        message = context.get('params', {}).get("message", None)

        # Maxcompute ETL
        instance = self._maxcompute_etl(
            sql=sql
            ,context=context
        )

        # Update Lark Multi Dimension Table
        if client_type == "multi":
            self._update_lark_multi_dimension_table(
                context=context
                ,url=url
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
        logger.info("Start Update Lark Sheet")
        
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
            table_id = client.tables_map.get(table_name, {})

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