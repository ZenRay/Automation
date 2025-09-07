#coding:utf8
"""Maxcomputer ODPS Client

Load data and transform data by maxcomputer
"""
import logging
from os import path
from odps import ODPS, options

# activate quota
options.sql.use_odps2_extension = True
options.sql.settings = {
    "odps.sql.execution.mode": "interactive",
    "odps.sql.submit.mode": "script"
}


logger = logging.getLogger("automation.maxcomputerClient")
class MaxComputerClient:
    def __init__(self, *args, **kwargs) -> None:
        self._client = ODPS(**kwargs)
        self._quota_name = kwargs.get("quota_name")
        logger.info("Load Macomputer Client Success")

    
    def execute_sql(
        self, sent, hints=None, is_async=False, interactive=False, **kwargs
    ):
        """Execute SQL Sentence"""

        if not is_async and (not interactive or not self._quota_name):
            instance = self._client.execute_sql(sent, hints=hints)
            logger.info("SQL Sentence Executed in blocking model")
            return instance

        elif interactive and self._quota_name:
            instance = self._client.execute_sql_interactive(
                sent
                ,hints=hints
                ,use_mcqa_v2=True
                ,quota_name=self._quota_name
            )
            logger.info("SQL Sentence Executed in interactive model")
            return instance
        else:
            self._client.run_sql(sent, hints=hints)
            logger.info("SQL Sentence Executed in ascync model")
            return self._client
    
    
    
    def execute_to_save(self, sql, file_path, hints=None) -> None:
        """Execute SQL and Save Result to Local File
        Args:
            sql: SQL statement
            file_path: Local file path to save the result
        """
        instance = self.execute_sql(sql, hints=hints)
        instance.wait_for_success()
        df = instance.to_pandas()
        if file_path.endswith('.csv'):
            df.to_csv(file_path, index=False)
        elif file_path.endswith('.xlsx'):
            df.to_excel(file_path, index=False)
        else:
            raise ValueError("Unsupported file format. Only .csv and .xlsx are supported.")

        logger.info(f"SQL Result saved to local file: {path.abspath(file_path)}")