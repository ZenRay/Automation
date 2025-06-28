# -*- coding: UTF-8 -*-
import datetime
from typing import List

import logging
from datetime import datetime, timedelta
import re


from .base import AccessToken, request
from ..exceptions import LarkException, RegexException


logger = logging.getLogger("automation.lark.api")

class LarkClient(object):
    def __init__(self, lark_host="https://open.feishu.cn"):
        """Init Lark Object"""
        self._host = lark_host
        self._token = AccessToken()


    # FIXME: 最后需要删除下面app_id 和 app_secret 信息
    @property
    def tenant_access_token(self, app_id="cli_a8d27f9bf635500e", app_secret="w7xAfJLTfEoUR2pJnbVz7c2luiXww7zn"):
        """Get Tenant Access Token"""
        if self._token.tenant_access_token is None or datetime.now() >= self._token.expire_time:
            url = self._host+"/open-apis/auth/v3/app_access_token/internal/"
            headers = {
                'Content-Type': 'application/json; charset=utf-8'
            }
            payload = {
                'app_id': app_id,
                'app_secret': app_secret
            }
            resp = request("POST", url, headers, payload)
            # self._token = resp['tenant_access_token']
            self._token = AccessToken(
                app_access_token=resp['app_access_token'],
                tenant_access_token=resp['tenant_access_token'],
                expire_time=datetime.now() + timedelta(seconds=resp["expire"])
            )
            logger.info("Load Access Token Success.")

        return self._token.tenant_access_token



    @property
    def app_access_token(self, app_id="cli_a8d27f9bf635500e", app_secret="w7xAfJLTfEoUR2pJnbVz7c2luiXww7zn"):
        """Get Application Access Token"""
        if self._token.app_access_token is None or datetime.now() >= self._token.expire_time:
            url = self._host+"/open-apis/auth/v3/app_access_token/internal/"
            headers = {
                'Content-Type': 'application/json; charset=utf-8'
            }
            payload = {
                'app_id': app_id,
                'app_secret': app_secret
            }
            resp = request("POST", url, headers, payload)
            # self._token = resp['tenant_access_token']
            self._token = AccessToken(
                app_access_token=resp['app_access_token'],
                tenant_access_token=resp['tenant_access_token'],
                expire_time=datetime.now() + timedelta(seconds=resp["expire"])
            )
            logger.info("Load Access Token Success.")

        return self._token.app_access_token
    



class LarkMultiDimTable(LarkClient):
    """Lark Multi Dimention Table Process"""

    def __init__(self, lark_host="https://open.feishu.cn"):
        super().__init__(lark_host)
        self._table_name = None
        self._app_token = None
        
        self._regex_pattern = re.compile("http.*/(?P<app_type>base|wiki|docx|sheets)/(?P<app_token>[a-z0-9]*)\?table=(?P<table_id>[a-z0-9]*)", re.I)


    @property
    def access_token(self):
        """Tenant Access Token"""
        return super().tenant_access_token
    


    def extract_app_information(self, url: str):
        """Extract Table App Token And Table Name From URL"""
        
        match = self._regex_pattern.match(url)
        raw_url = url

        if not url.startswith("http") or match is None:
            logger.error("URL Address Is invalid, get url: {url}".format(url=url))
            raise RegexException("URL Address Is invalid, get url: {url}".format(url=url))
        

        # Request to get table name
        app_token = match.group('app_token')
        app_type = match.group("app_type")
        
        # if the app type is diversity, the API address is different
        # FIXME: 待解决多类型文档的请求
        if app_type == "base":
            url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}"
            payload = {}
        elif app_type == "wiki":
            url = f"{self._host}/open-apis/wiki/v2/spaces/get_node"
            payload = {
                "obj_type": "bitable"
                ,"token": app_token
            }
        
            
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+ self.access_token,
        }
        resp =  request("GET", url, headers, payload=payload)
        
        if resp["msg"] == "success":
            logger.info(f"Extract Table Information Success, Origin URL: {raw_url}\nGet Table App Information: {resp}")
            # FIXME: 待解决多类型文档的请求
            if app_type == "base":
                self._table_name = resp["data"]["app"]["name"]
                self._app_token = resp["data"]["app"]["app_token"]

        else:
            logger.error(f"Extract Table Information Fail, Origin URL: {raw_url}")
            raise LarkException(code=resp["code"], msg=resp["msg"])


    def _check_app_token(self, url):
        """Check App Token Validate"""
        app_token = self._regex_pattern.match(url).group('app_token')
        if app_token != self._app_token:
            self.extract_app_information(url=url)
            logger.info("Use New URL, Update App Token Success.")
        

    def request_records_generator(self, *, table_id: str=None, url: str=None, view_id: str=None, page_size: int=None, automatic_fields: bool=False, **kwargs):
        """Request Records Generator
        It's a generator, each request can return a new records
        
        Args:
        -----------------
        table_id: str, table id is none, url address must exist.
        url: str, table url, if it's None, table id must exist.
        view_id: str, table view id, it can be None.
        page_size: str, request params, it can be None.
        automatic_fields: bool, get the record automatic information like create time, last modified time, create by, last modified by, if it's true.
        kwargs: another request parameter information, like: field_names specified fields, sort specified ascending method by fields, filter specified
            filter records condition.
        
        Result:
        -----------------
        Dict, return successfull request records.
        """
        
        _table_id = None
        if url is not None:
            self._check_app_token(url=url)
            _table_id = self._regex_pattern.match(url).group("table_id")
        
        if table_id is not None:
            logger.debug("Specify Table Id, Don't Use the URL address Table id")
        elif table_id is None and _table_id is not None:
            table_id = _table_id
        else:
            logger.error("There isn't specified Table. URL address: {url}".format(url=url))
            raise LarkException(msg="There isn't specified Table.")


        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+ self.access_token,
        }
        
        # Update Parameters
        params = {"automatic_fields": automatic_fields}
        if view_id: params['view_id'] = view_id
        if page_size: params["page_size"] = page_size

        if isinstance(kwargs.get("field_names"), (list, tuple)): params["field_names"] = kwargs.get("field_names")
        if isinstance(kwargs.get("sort"), (tuple, list)): params["sort"] = kwargs["sort"]

        # filter condition
        if isinstance(kwargs.get("filter"), dict): params["filter"] = kwargs.get("filter")
        
        has_more = True
        while has_more:
            resp = request("GET", url, headers, params=params)
            yield resp

            # update continue boolean
            has_more = resp.get("data").get("has_more")
            params["page_token"] = resp.get("data").get("page_token")

