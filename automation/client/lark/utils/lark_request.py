#coding:utf8
import requests
import logging
import string


from ..exceptions import LarkException, LarkSheetException


logger = logging.getLogger("automation.lark.utils.lark_request")



def request(method, url, headers, payload={}, params=None):
    """Lark API Request"""
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
        logger.error("Error Response: {0}".format(resp))
        raise LarkException(code=code, msg=resp.get("msg", ""))
    return resp
