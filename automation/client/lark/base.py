#coding:utf8
"""Lark Client Base Class"""
import logging
import requests
import json


from ..exceptions import LarkException

logger = logging.getLogger("automation.lark.base")


def request(method, url, headers, payload={}, params=None):
    response = requests.request(method, url, headers=headers, json=payload, params=params)
    logger.info("URL: " + url)
    logger.info("X-Tt-Logid: " + response.headers['X-Tt-Logid'])
    logger.info("headers:\n"+json.dumps(headers,indent=2, ensure_ascii=False))
    logger.info("payload:\n"+json.dumps(payload,indent=2, ensure_ascii=False))
    resp = {}
    if response.text[0] == '{':
        resp = response.json()
        logger.info("response:\n"+json.dumps(resp,indent=2, ensure_ascii=False))
    else:
        logger.info("response:\n"+response.text)
    code = resp.get("code", -1)
    if code == -1:
        code = resp.get("StatusCode", -1)
    if code == -1 and response.status_code != 200:
         response.raise_for_status()
    if code != 0:
        raise LarkException(code=code, msg=resp.get("msg", ""))
    return resp