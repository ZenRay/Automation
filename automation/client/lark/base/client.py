# -*- coding: UTF-8 -*-
import datetime
import time
from typing import List

import logging
import re
import threading
from datetime import datetime, timedelta



from .token import AccessToken
from ..utils import request
from ..exceptions import LarkException, RegexException


logger = logging.getLogger("automation.lark.base.client")



class LarkClient(object):
    _instances = {}
    _instances_lock = threading.Lock() 

    def __new__(cls, *args, **kwargs):
        app_id = kwargs.get("app_id")
        app_secret = kwargs.get("app_secret")
        lark_host = kwargs.get("lark_host", "https://open.feishu.cn")

        if not app_id or not app_secret:
            raise ValueError("app_id and app_secret are required parameters")
        
        key = (app_id, app_secret, lark_host)


        with cls._instances_lock:
            if key not in cls._instances:
                instance = super().__new__(cls)
                instance._initialized = False
                cls._instances[key] = instance
            return cls._instances[key]


    def __init__(self, *, app_id, app_secret, lark_host="https://open.feishu.cn"):
        """Init Lark Object"""
        if self._initialized:
            return

        self._host = lark_host
        self.__app_id = app_id
        self.__app_secret = app_secret
        self._access_token = AccessToken()
        # self._user_token = UserAccessToken()
        self._token_lock = threading.Lock()
        self._initialized = True
        logger.info(f"Lark Client Initialized for app_id: {app_id}")



    @property
    def tenant_access_token(self):
        """Get Tenant Access Token"""
        if not self._access_token.is_valid:
            self._refresh_access_token()
        return self._access_token.tenant_access_token



    @property
    def app_access_token(self):
        """Get Application Access Token"""
        if not self._access_token.is_valid:
            self._refresh_access_token()
        return self._access_token.app_access_token
    


    def _refresh_access_token(self):
        with self._token_lock:
            if self._access_token.is_valid:
                return
                
            url = f"{self._host}/open-apis/auth/v3/app_access_token/internal/"
            headers = {'Content-Type': 'application/json'}
            payload = {'app_id': self.__app_id, 'app_secret': self.__app_secret}
            try:
                for attempt in range(3):
                    try:
                        resp = request("POST", url, headers=headers, payload=payload)
                        if resp["code"] == 0:
                            break
                        else:
                            raise LarkException(msg=f"Error Response: {resp}")
                    except Exception as e:
                        if attempt == 2:
                            raise
                        logger.warning(f"Token refresh attempt {attempt+1} failed: {e}")
                        time.sleep(2 ** attempt)

                # adjust expire time before 120 seconds
                expire_time = datetime.now() + timedelta(seconds=resp["expire"] - 120)

                self._access_token = AccessToken(
                    app_access_token=resp.get('app_access_token'),
                    tenant_access_token=resp.get('tenant_access_token'),
                    expire_time=expire_time
                )
                logger.info("Token refreshed successfully")

            except Exception as e:
                logger.error(f"Token refresh failed after 3 attempts: {e}")
                raise LarkException(msg="Failed to refresh access token") from e
                
