# coding:utf8
import datetime
import json
import time
from typing import List

import logging
import re
import threading
from datetime import datetime, timedelta

import os
from pathlib import Path

from requests_toolbelt import MultipartEncoder

from ..utils.lark_request import request
from ..base import AccessToken, LarkClient
from ..common import LarkBitableURL, MIMEType
from ..exceptions import LarkException, RegexException

logger = logging.getLogger("automation.lark.api.multidimention_table")


class LarkMultiDimTable(LarkClient):
    """Lark Multi Dimention Table Process

    Property:
    -----------------
    ADD_RECORD_LIMITATION: int, the limitation of adding records each batch.
    DELETE_RECORD_LIMITATION: int, the limitation of deleting records each batch.
    """

    ADD_RECORD_LIMITATION = 1000
    DELETE_RECORD_LIMITATION = 450

    def __init__(self, app_id, app_secret, lark_host="https://open.feishu.cn"):
        """Initialize Lark Multi Dimention Table Client

        Property:
        -----------------
        access_token: str, Lark tenant access token.
        app_type: str, Multi Dimention Table App Type (base/wiki/docx/sheets)
        app_table_map: dict, The table and table_id mapping in Multi Dimention Table App
        """
        super().__init__(app_id=app_id, app_secret=app_secret, lark_host=lark_host)
        self._table_name = None
        self._app_token = None
        self._app_type = None
        self._app_table_map = {}

        self.__raw_url = None

        self._regex_pattern = re.compile(
            r"http.*/(?P<app_type>base|wiki|docx|sheets)/(?P<app_token>[a-z0-9]*)\?table=(?P<table_id>[a-z0-9]*)",
            re.I,
        )

    @property
    def access_token(self):
        """Tenant Access Token"""
        return super().tenant_access_token

    @property
    def app_type(self):
        """App Type (base/wiki/docx/sheets)"""
        return self._app_type

    @property
    def app_token(self):
        """Multi Dimention Table App Token"""
        return self._app_token

    @property
    def tables_map(self):
        """Multi Dimention Table App Tables Mapping"""
        return self._app_table_map

    def extract_app_information(self, url: str):
        """Extract Table App Token And Table Name From URL"""

        match = self._regex_pattern.match(url)
        raw_url = url

        if not url.startswith("http") or match is None:
            logger.error("URL Address Is invalid, get url: {url}".format(url=url))
            raise RegexException(
                "URL Address Is invalid, get url: {url}".format(url=url)
            )

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + self.access_token,
        }

        # Request to get table name
        self._app_type = match.group("app_type")

        # if the app type is diversity, the API address is different
        # FIXME: 待解决多类型文档的请求
        if self._app_type == "base":
            app_token = match.group("app_token")

        elif self._app_type == "wiki":
            # Wiki Multi Dimention Table Node Information
            wiki_app_token = match.group("app_token")
            url = f"{self._host}/open-apis/wiki/v2/spaces/get_node"
            params = {"obj_type": "wiki", "token": wiki_app_token}
            resp = request("GET", url, headers, params=params)
            # Extract Table Name And App Token, if request success
            if resp["code"] == 0:
                app_token = resp["data"]["node"]["obj_token"]
            else:
                logger.error(f"Extract Table Information Fail, Origin URL: {raw_url}")

        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}"
        resp = request("GET", url, headers)

        if resp["msg"] == "success":
            logger.info(
                f"Extract Table Information Success, Origin URL: {raw_url}\nGet Table App Information: {resp}"
            )
            self._table_name = resp["data"]["app"]["name"]
            self._app_token = resp["data"]["app"]["app_token"]
            self.__raw_url = raw_url

        else:
            logger.error(f"Extract Table Information Fail, Origin URL: {raw_url}")
            raise LarkException(code=resp["code"], msg=resp["msg"])

    def extract_table_information(self, *, url: str = None, app_token: str = None):
        """Extract Table Information From App

        Args:
        -----------------
        url: str, table url, if it's None, app_token must exist.
        app_token: str, Multi Dimention Table App Token, if it's None, url must exist.

        Result:
        -----------------
        Dict, return table information mapping.
        """

        if url is not None:
            self._check_app_token(url=url)
            app_token = self._app_token

        if app_token is None:
            logger.error(
                "There isn't specified App Token. URL address: {url}".format(url=url)
            )
            raise LarkException(msg="There isn't specified App Token.")

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + self.access_token,
        }

        url = f"{self._host}/open-apis/bitable/v1/apps/{app_token}/tables"
        resp = request("GET", url, headers)
        if resp.get("code", -1) == 0:
            tables = resp.get("data", {}).get("items", [])
            self._app_table_map = {
                table.get("name"): table.get("table_id") for table in tables
            }
            logger.info(
                f"Extract Table Information From App {app_token} Success: {self._app_table_map}"
            )
            return self._app_table_map
        else:
            logger.error(
                f"Extract Table Information From App {app_token} Failed: {resp}"
            )
            raise LarkException(
                code=resp.get("code"),
                msg=resp.get("msg", "Extract table information failed"),
            )

    def list_fields(self, *, table_id: str = None, url: str = None) -> list[dict]:
        """获取指定表格的字段列表

        Args:
            table_id: 表格 ID，与 url 二选一
            url:      表格 URL，与 table_id 二选一

        Returns:
            list[dict]：字段列表，每个 dict 包含 field_name、type 等信息
            参考: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/bitable-v1/app-table-field/list
        """
        _table_id = None
        if url is not None:
            self._check_app_token(url=url)
            _table_id = self._regex_pattern.match(url).group("table_id")

        if table_id is not None:
            _table_id = table_id
        elif _table_id is None:
            raise LarkException(msg="There isn't specified Table.")

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + self.access_token,
        }

        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{_table_id}/fields"
        resp = request("GET", url, headers)

        if resp.get("code", -1) == 0:
            items = resp.get("data", {}).get("items", [])
            logger.debug(f"Listed {len(items)} fields from table {_table_id}")
            return items
        else:
            logger.error(f"List fields from table {_table_id} failed: {resp}")
            raise LarkException(
                code=resp.get("code"), msg=resp.get("msg", "List fields failed")
            )

    def create_table(
        self,
        name: str,
        fields: list[dict],
        default_view_name: str = None,
        *,
        app_token: str = None,
    ) -> str:
        """在多维表格中创建新数据表

        Args:
            name:                数据表名称（不可包含 / \\ ? * : [ ] 等特殊字符）
            fields:              字段定义列表，每项格式 {"field_name": "xxx", "type": 1, "property": {...}}
                                 第一个字段为索引字段，仅支持 type 1/2/5/13/15/20/22
            default_view_name:   可选，默认表格视图名称
            app_token:           可选，若为 None 则使用当前已解析的 self._app_token

        Returns:
            str：新创建数据表的 table_id

        API 参考:
            https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table/create
        """
        _app_token = app_token or self._app_token
        if _app_token is None:
            raise LarkException(
                msg="app_token is required. Call extract_app_information() first or pass app_token."
            )

        if not name or not name.strip():
            raise LarkException(msg="Table name cannot be empty")

        if not fields:
            raise LarkException(
                msg="At least one field is required (first field becomes the index field)"
            )

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + self.access_token,
        }

        payload = {
            "table": {
                "name": name.strip(),
                "fields": fields,
            }
        }
        if default_view_name:
            payload["table"]["default_view_name"] = default_view_name.strip()

        url = f"{self._host}/open-apis/bitable/v1/apps/{_app_token}/tables"
        resp = request("POST", url, headers, payload)

        if resp.get("code", -1) == 0:
            table_id = resp.get("data", {}).get("table_id")
            logger.info(
                f"Created table '{name}' in app {_app_token}, table_id={table_id}"
            )

            # 刷新 tables_map
            try:
                self.extract_table_information(app_token=_app_token)
            except Exception as e:
                logger.warning(
                    f"Table created successfully but failed to refresh tables_map: {e}"
                )

            return table_id
        else:
            logger.error(f"Create table '{name}' failed: {resp}")
            raise LarkException(
                code=resp.get("code"),
                msg=resp.get("msg", f"Create table '{name}' failed"),
            )

    def create_field(
        self,
        field_def: dict,
        *,
        table_id: str = None,
        url: str = None,
    ) -> dict:
        """在指定数据表中新增一个字段

        Args:
            field_def:  字段定义，格式 {"field_name": "xxx", "type": 2, "property": {...}}
            table_id:   目标表 ID，与 url 二选一
            url:        目标表 URL，与 table_id 二选一

        Returns:
            dict：API 响应中的字段信息

        API 参考:
            https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/bitable-v1/app-table-field/create
        """
        _table_id = None
        if url is not None:
            self._check_app_token(url=url)
            _table_id = self._regex_pattern.match(url).group("table_id")

        if table_id is not None:
            _table_id = table_id
        elif _table_id is None:
            raise LarkException(msg="table_id or url is required")

        if self._app_token is None:
            raise LarkException(
                msg="app_token is required. Call extract_app_information() first."
            )

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + self.access_token,
        }

        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{_table_id}/fields"
        resp = request("POST", url, headers, field_def)

        if resp.get("code", -1) == 0:
            field_info = resp.get("data", {}).get("field", {})
            logger.info(
                f"Created field '{field_def.get('field_name')}' in table {_table_id}, "
                f"field_id={field_info.get('field_id')}"
            )
            return field_info
        else:
            logger.error(f"Create field '{field_def.get('field_name')}' failed: {resp}")
            raise LarkException(
                code=resp.get("code"), msg=resp.get("msg", "Create field failed")
            )

    def _check_app_token(self, url):
        """Check App Token Validate

        If the URL address app token is different from the current one, re-extract the app information
        """

        if url != self.__raw_url:
            self.extract_app_information(url=url)
            logger.info("Use New URL, Update App Token Success.")

    def request_records_generator(
        self,
        *,
        table_id: str = None,
        url: str = None,
        view_id: str = None,
        page_size: int = None,
        automatic_fields: bool = False,
        **kwargs,
    ):
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
            logger.error(
                "There isn't specified Table. URL address: {url}".format(url=url)
            )
            raise LarkException(msg="There isn't specified Table.")

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + self.access_token,
        }

        # Update Parameters
        params = {"automatic_fields": automatic_fields}
        if view_id:
            params["view_id"] = view_id
        if page_size:
            params["page_size"] = page_size

        if isinstance(kwargs.get("field_names"), (list, tuple)):
            params["field_names"] = json.dumps(kwargs.get("field_names"))
        if isinstance(kwargs.get("sort"), (tuple, list)):
            params["sort"] = json.dumps(kwargs["sort"])

        # filter condition
        filter_obj = (
            kwargs.get("filter") if isinstance(kwargs.get("filter"), dict) else None
        )

        has_more = True
        post_page_token = None
        # if there is filter, use POST search method else use GET
        if filter_obj is not None:
            method = "POST"
            url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records/search"
        else:
            method = "GET"
            url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records"

        while has_more:
            if method == "POST":
                # POST search: filter + pagination 必须在 JSON body 中
                body = {"filter": filter_obj}
                if page_size:
                    body["page_size"] = page_size
                if post_page_token:
                    body["page_token"] = post_page_token
                resp = request(method, url, headers, payload=body, params=params)
            else:
                resp = request(method, url, headers, params=params)
            yield resp

            # 空页早停：飞书 search API 在 filter 无匹配时可能仍返回 has_more=True，
            # 导致无效遍历整个表。当前页无记录时直接终止分页。
            items = resp.get("data", {}).get("items", [])
            if not items:
                logger.debug("Empty page with has_more=True, stopping pagination")
                break

            # update continue boolean
            has_more = resp.get("data").get("has_more")
            page_token = resp.get("data").get("page_token")
            if method == "POST":
                post_page_token = page_token
            else:
                params["page_token"] = page_token
            time.sleep(1)

    def delete_record(self, record_id: str, *, table_id: str = None, url: str = None):
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
            logger.error(
                "There isn't specified Table. URL address: {url}".format(url=url)
            )
            raise LarkException(msg="There isn't specified Table.")

        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records/{record_id}"

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + self.access_token,
        }

        if record_id is None:
            raise LarkException(
                msg="Multi Dimention Table Delete Records Failed, Because There isn't records"
            )

        resp = request("DELETE", url, headers)

        if resp.get("code", -1) == 0:
            logger.info(f"Delete Record {record_id} From Table {table_id} Success.")

    def delete_batch_records(
        self, records_id: List[str], *, table_id: str = None, url: str = None
    ):
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
            logger.error(
                "There isn't specified Table. URL address: {url}".format(url=url)
            )
            raise LarkException(msg="There isn't specified Table.")

        if not records_id:
            raise LarkException(msg="Record IDs list cannot be empty")

        # If get single record, use delete_record
        if isinstance(records_id, str):
            self.delete_record(record_id=records_id, url=url)
            return

        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records/batch_delete"

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + self.access_token,
        }

        payload = {"records": records_id}

        resp = request("POST", url, headers, payload)

        if resp.get("code", -1) == 0:
            result = [
                item.get("record_id")
                for item in resp.get("data", {}).get("records", [])
            ]
            logger.info(
                f"Batch Delete {len(result)}/{len(records_id)} Records From Table {table_id} Success."
            )
        else:
            logger.error(
                f"Batch Delete Records From Table {table_id} Failed: "
                f"code={resp.get('code')}, msg={resp.get('msg')}"
            )

    def add_record(self, fields: dict, *, table_id: str = None, url: str = None):
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
            logger.error(
                "There isn't specified Table. URL address: {url}".format(url=url)
            )
            raise LarkException(msg="There isn't specified Table.")

        if not fields:
            raise LarkException(msg="Fields data cannot be empty")

        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records"

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + self.access_token,
        }

        payload = {"fields": fields}

        resp = request("POST", url, headers, payload)

        if resp.get("code", -1) == 0:
            record_id = resp.get("data", {}).get("record", {}).get("record_id")
            logger.info(f"Create Record {record_id} In Table {table_id} Success.")
        else:
            logger.error(f"Create Record In Table {table_id} Failed: {resp}")
            raise LarkException(
                code=resp.get("code"), msg=resp.get("msg", "Create record failed")
            )

        return resp

    def add_batch_records(
        self,
        records: List[dict],
        *,
        table_id: str = None,
        url: str = None,
        table_name: str = None,
    ):
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
        if table_name is not None and self.tables_map:
            _table_id = self.tables_map.get(table_name)

        if url is not None:
            self._check_app_token(url=url)
            if _table_id is None:
                _table_id = self._regex_pattern.match(url).group("table_id")

        if table_id is not None:
            logger.debug("Specify Table Id, Don't Use the URL address Table id")
        elif table_id is None and _table_id is not None:
            table_id = _table_id
        else:
            logger.error(
                "There isn't specified Table. URL address: {url}".format(url=url)
            )
            raise LarkException(msg="There isn't specified Table.")

        if not records:
            raise LarkException(msg="Records data cannot be empty")

        # If get single record, use create_record
        if isinstance(records, dict):
            return self.add_record(fields=records, table_id=table_id, url=url)

        url = f"{self._host}/open-apis/bitable/v1/apps/{self._app_token}/tables/{table_id}/records/batch_create"

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": "Bearer " + self.access_token,
        }

        payload = {"records": records}

        resp = request("POST", url, headers, payload)

        if resp.get("code", -1) == 0:
            created_records = resp.get("data", {}).get("records", [])
            logger.info(
                f"Batch Create {len(created_records)}/{len(records)} Records In Table {table_id} Success."
            )
        else:
            logger.error(f"Batch Create Records In Table {table_id} Failed: {resp}")
            raise LarkException(
                code=resp.get("code"),
                msg=resp.get("msg", "Batch create records failed"),
            )

        return resp

    def upload_attachment(
        self,
        file,
        *,
        app_token: str = None,
        file_name: str = None,
        mime_type: str = None,
        need_binary: bool = True,
        parent_type: str = "bitable_file",
        parent_node: str = None,
    ):
        """Upload attachment material to Bitable and return API response.

        The response is expected to contain attachment metadata including file_token.
        """
        _app_token = app_token or self._app_token
        if _app_token is None:
            raise LarkException(msg="app_token is required for attachment upload")

        if file is None:
            raise ValueError(
                "File path or bytes must be provided for attachment upload"
            )

        local_name = file_name
        if isinstance(file, (str, os.PathLike)):
            local_path = Path(file)
            if local_name is None:
                local_name = local_path.name
            if need_binary:
                with open(local_path, "rb") as f:
                    file = f.read()

        if local_name is None:
            local_name = "attachment"

        if mime_type is None:
            suffix = Path(local_name).suffix.lower()
            suffix_map = {
                ".jpg": MIMEType.JPG.value,
                ".jpeg": MIMEType.JPEG.value,
                ".png": MIMEType.PNG.value,
                ".gif": MIMEType.GIF.value,
                ".webp": MIMEType.WEBP.value,
                ".bmp": MIMEType.BMP.value,
                ".mp4": MIMEType.MP4.value,
            }
            mime_type = suffix_map.get(suffix, MIMEType.STREAM.value)

        upload_parent_node = parent_node or _app_token
        url = LarkBitableURL.UPLOAD_ATTACHMENT.value
        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        file_size = len(file) if isinstance(file, (bytes, bytearray)) else 0
        form = MultipartEncoder(
            fields={
                "file_name": local_name,
                "parent_type": parent_type,
                "parent_node": upload_parent_node,
                "size": str(file_size),
                "file": (local_name, file, mime_type),
            }
        )
        headers["Content-Type"] = form.content_type
        resp = request("POST", url, headers=headers, data=form)
        if resp.get("code", -1) != 0:
            logger.error(f"Failed to upload attachment({local_name}): {resp}")
            raise LarkException(
                code=resp.get("code"), msg=resp.get("msg", "Upload attachment failed")
            )
        return resp
