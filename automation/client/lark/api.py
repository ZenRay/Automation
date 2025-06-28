# -*- coding: UTF-8 -*-
import datetime
from typing import List

import logging
import sys


from .base import request
from ..exceptions import LarkException


logger = logging.getLogger("automation.lark.api")

class LarkClient(object):
    def __init__(self, lark_host="https://open.feishu.cn"):
        """Init Lark Object"""
        self._host = lark_host
        self._tenant_access_token = None


    @property
    def tenant_access_token(self, app_id, app_secret):
        """Get Tenant Access Token"""
        if self._tenant_access_token is None:
            url = self._host+"/open-apis/auth/v3/app_access_token/internal/"
            headers = {
                'Content-Type': 'application/json; charset=utf-8'
            }
            payload = {
                'app_id': app_id,
                'app_secret': app_secret
            }
            resp = request("POST", url, headers, payload)
            self._tenant_access_token = resp['tenant_access_token']
            logger.info("Initial Access Token Success.")

        return self._tenant_access_token


