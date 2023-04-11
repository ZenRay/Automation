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

    
    def execute_sql(self, sent, hints=None, **kwargs):
        """Execute SQL Sentence"""
        if hints is None:
            self._client.execute_sql(sent)
        else:
            self._clli.execute_sql(sent, hints=hints)

        logger.info("SQL Sentence Executed")
    