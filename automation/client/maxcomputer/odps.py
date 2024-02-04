#coding:utf8
"""Maxcomputer ODPS Client

Load data and transform data by maxcomputer
"""
import logging
from odps import ODPS, options


logger = logging.getLogger("automation.maxcomputerClient")
class MaxComputerClient:
    def __init__(self, *args, **kwargs) -> None:
        self._client = ODPS(**kwargs)

        logger.info("Load Macomputer Client Success")

    
    def execute_sql(self, sent, hints=None, is_async=False, **kwargs):
        """Execute SQL Sentence"""

        if not is_async:
            instance = self._client.execute_sql(sent, hints=hints)
            logger.info("SQL Sentence Executed in blocking model")
            return instance
        else:
            self._client.run_sql(sent, hints=hints)
            logger.info("SQL Sentence Executed in ascync model")
            return self._client
    