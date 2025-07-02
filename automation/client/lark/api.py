# -*- coding: UTF-8 -*-
import datetime
import time
from typing import List

import logging
import re
import threading
from datetime import datetime, timedelta


from .base import AccessToken, UserAccessToken
from .utils import request
from ..exceptions import LarkException, RegexException


logger = logging.getLogger("automation.lark.api")



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
        self._user_token = UserAccessToken()
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
                



class LarkMultiDimTable(LarkClient):
    """Lark Multi Dimention Table Process"""

    def __init__(self, app_id, app_secret, lark_host="https://open.feishu.cn"):
        super().__init__(app_id=app_id, app_secret=app_secret, lark_host=lark_host)
        self._table_name = None
        self._app_token = None
        
        self._regex_pattern = re.compile("http.*/(?P<app_type>base|wiki|docx|sheets)/(?P<app_token>[a-z0-9]*)\?table=(?P<table_id>[a-z0-9]*)", re.I)


    @property
    def access_token(self):
        """Tenant Access Token"""
        return super().tenant_access_token
    
    @property
    def app_type(self):
        """App Type (base/wiki/docx/sheets)"""
        return self._app_type


    def extract_app_information(self, url: str):
        """Extract Table App Token And Table Name From URL"""
        
        match = self._regex_pattern.match(url)
        raw_url = url

        if not url.startswith("http") or match is None:
            logger.error("URL Address Is invalid, get url: {url}".format(url=url))
            raise RegexException("URL Address Is invalid, get url: {url}".format(url=url))
        
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+ self.access_token,
        }

        # Request to get table name
        self._app_type = match.group("app_type")
        
        # if the app type is diversity, the API address is different
        # FIXME: 待解决多类型文档的请求
        if self._app_type == "base":
            app_token = match.group('app_token')
            
        elif self._app_type == "wiki":
            # Wiki Multi Dimention Table Node Information
            wiki_app_token = match.group('app_token')
            url = f"{self._host}/open-apis/wiki/v2/spaces/get_node"
            params = {
                "obj_type": "wiki",
                "token": wiki_app_token
            }
            resp = request("GET", url, headers, params=params)
            # Extract Table Name And App Token, if request success
            if resp["code"] == 0:
                app_token = resp["data"]["node"]["obj_token"]
            else:
                logger.error(f"Extract Table Information Fail, Origin URL: {raw_url}")
                

        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}"
        resp = request("GET", url, headers)
        
        if resp["msg"] == "success":
            logger.info(f"Extract Table Information Success, Origin URL: {raw_url}\nGet Table App Information: {resp}")
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
        # if there is params, use POST method else use GET
        if "filter" in params:
            method = "POST"
            url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records/search"
        else:
            method="GET"
            url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records"

        while has_more:
            resp = request(method, url, headers, params=params)
            yield resp

            # update continue boolean
            has_more = resp.get("data").get("has_more")
            params["page_token"] = resp.get("data").get("page_token")
            time.sleep(3)



    def delete_record(self, record_id: str, *, table_id: str=None, url: str=None):
        """Delete Specified Single Record"""
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
        

        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records/{record_id}"
                	

        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+ self.access_token,
        }

        if record_id is  None:
            raise LarkException(msg="Multi Dimention Table Delete Records Failed, Because There isn't records")


        resp = request("DELETE", url, headers)
        
        if resp.get("code", -1) == 0:
            logger.info(f"Delete Record {record_id} From Table {table_id} Success.")
        
    



    def delete_batch_records(self, records_id: List[str], *, table_id: str=None, url: str=None):
        """Batch Delete Multiple Records
        
        Args:
        -----------------
        records_id: List[str], list of record ids to delete
        table_id: str, table id, if it's None, url address must exist.
        url: str, table url, if it's None, table id must exist.
        
        Result:
        -----------------
        Dict, return successful response with deleted record information.
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

        if not records_id:
            raise LarkException(msg="Record IDs list cannot be empty")

        # If get single record, use delete_record
        if isinstance(records_id, str):
            self.delete_record(record_id=records_id, url=url)
            return

        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records/batch_delete"
        
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+ self.access_token,
        }
        
        payload = {
            'records': records_id
        }

        resp = request("POST", url, headers, payload)
        
        if resp.get("code", -1) == 0:
            result = [item.get("record_id") for item in resp.get("data", {}).get("records", [])]
            logger.info(f"Batch Delete {len(result)}/{len(records_id)} Records From Table {table_id} Success.")
        
        

    def add_record(self, fields: dict, *, table_id: str=None, url: str=None):
        """Add Single Record
        
        Args:
        -----------------
        fields: dict, record fields data, format: {"字段名": "值"}
        table_id: str, table id, if it's None, url address must exist.
        url: str, table url, if it's None, table id must exist.
        
        Result:
        -----------------
        Dict, return created record information.
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

        if not fields:
            raise LarkException(msg="Fields data cannot be empty")

        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records"
        
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+ self.access_token,
        }
        
        payload = {
            'fields': fields
        }

        resp = request("POST", url, headers, payload)
        
        if resp.get("code", -1) == 0:
            record_id = resp.get("data", {}).get("record", {}).get("record_id")
            logger.info(f"Create Record {record_id} In Table {table_id} Success.")
        else:
            logger.error(f"Create Record In Table {table_id} Failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Create record failed"))
        
        return resp


    def add_batch_records(self, records: List[dict], *, table_id: str=None, url: str=None):
        """Batch Add Multiple Records
        
        Args:
        -----------------
        records: List[dict], list of record data, each item format: {"fields": {"字段名": "值"}}
        table_id: str, table id, if it's None, url address must exist.
        url: str, table url, if it's None, table id must exist.
        
        Result:
        -----------------
        Dict, return created records information.
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

        if not records:
            raise LarkException(msg="Records data cannot be empty")

        # If get single record, use create_record
        if isinstance(records, dict):
            return self.add_record(fields=records, table_id=table_id, url=url)

        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records/batch_create"
        
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+ self.access_token,
        }
        
        payload = {
            'records': records
        }

        resp = request("POST", url, headers, payload)
        
        if resp.get("code", -1) == 0:
            created_records = resp.get("data", {}).get("records", [])
            logger.info(f"Batch Create {len(created_records)}/{len(records)} Records In Table {table_id} Success.")
        else:
            logger.error(f"Batch Create Records In Table {table_id} Failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Batch create records failed"))
        
        return resp



 