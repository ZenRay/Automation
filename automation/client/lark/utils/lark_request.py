#coding:utf8
import requests
import logging
import string


from ..exceptions import LarkException


logger = logging.getLogger("automation.lark.utils.lark_request")



def request(method, url, headers, payload={}, params=None, refresh_client=None, data=None):
    """Lark API Request"""
    # Refresh Access Token if needed
    if refresh_client and hasattr(refresh_client, "_refresh_access_token") and callable(getattr(refresh_client, "_refresh_access_token")):
        refresh_client._refresh_access_token()
    response = requests.request(
        method, url, headers=headers, json=payload, params=params, data=data
    )

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
    return resp
