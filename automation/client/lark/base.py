#coding:utf8
"""Lark Client Base Class"""
import logging

from datetime import datetime

from ..exceptions import LarkException

logger = logging.getLogger("automation.lark.base")


class AccessToken:
    """Lark Access Token"""
    def __init__(self, tenant_access_token=None, app_access_token=None, expire_time=None):
        """Access Token Information

        Args:
        tenant_access_token, str Lark tenant access token
        app_access_token, str Lark application access token
        expire_time, datetime token expire datetime
        """
        self.tenant_access_token = tenant_access_token
        self.app_access_token = app_access_token
        self.expire_time = expire_time or datetime.min


    @property
    def is_valid(self):
        """Check Token Validate"""
        has_tokens = (
            self.app_access_token is not None and 
            self.tenant_access_token is not None
        )

        is_not_expired = (
            self.expire_time is not None and 
            datetime.now() < self.expire_time
        )

        return has_tokens and is_not_expired


