#coding:utf8
"""Lark Client Base Class"""
import logging
import requests



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
        self.expire_time = expire_time





def request(method, url, headers, payload={}, params=None):
    response = requests.request(method, url, headers=headers, json=payload, params=params)

    resp = {}
    if response.text[0] == '{':
        resp = response.json()
    else:
        logger.info("response:\n"+response.text)
    code = resp.get("code", -1)
    if code == -1:
        code = resp.get("StatusCode", -1)
    if code == -1 and response.status_code != 200:
         response.raise_for_status()
    if code != 0:
        logger.error("Error Response: {0}".format(resp.text))
        raise LarkException(code=code, msg=resp.get("msg", ""))
    return resp