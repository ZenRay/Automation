# coding:utf8
import datetime
import time
from typing import List

import logging
import re
import threading
from datetime import datetime, timedelta


from ..base import AccessToken, LarkClient
from ..utils import request, data_generator, parse_sheet_cell
from ..exceptions import LarkException, RegexException


logger = logging.getLogger("automation.lark.api.spread_sheet")




class LarkSheets(LarkClient):
    """Lark Sheets Process"""

    # Sheet Value Update Limition
    _UPDATE_ROW_LIMITION = 5000
    _UPDATE_COL_LIMITION = 100
    
    def __init__(self, app_id, app_secret, lark_host="https://open.feishu.cn"):
        super().__init__(app_id=app_id, app_secret=app_secret, lark_host=lark_host)
        self._spread_sheet_token = None
        self._sheet_id = None
        self._app_type = None
        self._regex_pattern = re.compile(r"http.*/(?P<app_type>sheets|wiki)/(?P<token>[a-zA-Z0-9_-]+)(?:\?sheet=(?P<sheet_id>[a-zA-Z0-9_-]+))?", re.I)
        
        self._sheets_mapping = {}
    
    @property
    def access_token(self):
        """Tenant Access Token"""
        return super().tenant_access_token
    
    
    @property
    def sheets_mapping(self):
        """Sheet Information
        
        Mapping about Sheet title and sheet id:
            * SpreadSheet: <spread_sheet_token>
            * <Sheet Name>: <Sheet Id>
        """
        if len(self._spread_sheet_token) == 0:
            raise LarkException(
                "Spreadsheet token is required to get sheets mapping"
                    "Use obj.extract_spreadsheet_info(url) to extract it from the URL"
            )
        # check whether SpreadSheet mapping exists, 
        if not self._sheets_mapping.get("SpreadSheet"):
            self._sheets_mapping["SpreadSheet"] = self._spread_sheet_token
            
            for sheet in self.get_sheets():
                self._sheets_mapping[sheet["title"]] = sheet["sheet_id"]
            
        return self._sheets_mapping
    
    
    @property
    def spread_sheet(self):
        """Spread Sheet Token"""
        if not self._spread_sheet_token:
            raise LarkException(
                "Must call extract_spreadsheet_info(url) first"
                    " or Set spread_sheet value"
            )

        return self._spread_sheet_token

    @spread_sheet.setter
    def spread_sheet(self, value):
        """Set Spread Sheet Token
        
        Pass URL or Spread Sheet Token, if get url then extract spread sheet
        token. Otherwise set the value
        """
        if value.startswith("http"):
            self.extract_spreadsheet_info(value)
        else:
            self._spread_sheet_token = value
            
        logger.info(f"Update Spread Sheet Token: {self._spread_sheet_token} Success.")
        
        

    def extract_spreadsheet_info(self, url: str):
        """Extract Sheet Meta Information from URL
        
        Args:
        -----------------
        url: str, sheet url
        
        Result:
        -----------------
        Tuple of spreadsheet token and sheet id
        """
        match = self._regex_pattern.match(url)
        raw_url = url

        if not url.startswith("http") or match is None:
            logger.error(f"URL Address Is invalid, get url: {url}")
            raise RegexException(f"URL Address Is invalid, get url: {url}")
        
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': 'Bearer '+ self.access_token,
        }

        # extract app type and token
        self._app_type = match.group("app_type")
        token = match.group("token")

        # extract sheet id
        if self._app_type == "sheets":
            # extract sheet token
            self._spread_sheet_token = token
            self._sheet_id = match.group("sheet_id")
            
        elif self._app_type == "wiki":
            # extract token if app_type is wiki
            wiki_token = token
            node_url = f"{self._host}/open-apis/wiki/v2/spaces/get_node"
            params = {
                "obj_type": "wiki",
                "token": wiki_token
            }
            resp = request("GET", node_url, headers, params=params)
            
            if resp.get("code", -1) == 0:
                obj_token = resp.get("data", {}).get("node", {}).get("obj_token")
                if obj_token:
                    self._spread_sheet_token = obj_token
                    logger.info(f"Retrieved spreadsheet token {obj_token} from wiki node")
                else:
                    logger.error(f"Failed to extract spreadsheet token from wiki node: {resp}")
                    raise LarkException(msg="Could not extract spreadsheet token from wiki node")
            else:
                logger.error(f"Failed to get wiki node information: {resp}")
                raise LarkException(code=resp.get("code", -1), msg=resp.get("msg", "Failed to get wiki node information"))
        
        if not self._spread_sheet_token:
            raise RegexException("Could not extract spreadsheet token from URL")
        
        logger.info(f"Extracted spreadsheet token: {self._spread_sheet_token}, sheet id: {self._sheet_id}, app type: {self._app_type}")
        return self._spread_sheet_token, self._sheet_id
    
    
    def get_spreadsheet_meta(self, spreadsheet_token: str = None):
        """Get Spreadsheet Metadata
        
        Args:
        -----------------
        spreadsheet_token: str, optional spreadsheet token
        
        Result:
        -----------------
        Dict with spreadsheet metadata
        """
        token = spreadsheet_token or self._spread_sheet_token
        if not token:
            raise LarkException(msg="Spreadsheet token is required")
        
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/metainfo"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        resp = request("GET", url, headers)
        
        if resp.get("code", -1) == 0:
            logger.info(f"Get spreadsheet metadata success: {token}")
            self._spread_sheet_token = token
            return resp.get("data")
        else:
            logger.error(f"Get spreadsheet metadata failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Get spreadsheet metadata failed"))
    
    def get_sheets(self, spreadsheet_token: str = None):
        """Get Sheets in a Spreadsheet
        
        Args:
        -----------------
        spreadsheet_token: str, optional spreadsheet token
        
        Result:
        -----------------
        List of sheets in the spreadsheet
        """
        token = spreadsheet_token or self._spread_sheet_token
        if not token:
            raise LarkException(msg="Spreadsheet token is required")
        
        url = f"{self._host}/open-apis/sheets/v3/spreadsheets/{token}/sheets/query"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        resp = request("GET", url, headers)
        
        if resp.get("code", -1) == 0:
            logger.info(f"Get sheets list success for spreadsheet: {token}")
            return resp.get("data", {}).get("sheets", [])
        else:
            logger.error(f"Get sheets list failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Get sheets list failed"))
    
    
    # TODO: READ SHEE VALUE
    def read_sheet_values(self, range_str: str, spreadsheet_token: str = None, sheet_id: str = None, 
                          value_render_option: str = "formatted", date_time_render_option: str = "formatted"):
        """Read Values from a Sheet
        
        Args:
        -----------------
        range_str: str, cell range in A1 notation (e.g. "<Sheet1>!A1:D5")
        spreadsheet_token: str, optional spreadsheet token
        sheet_id: str, optional sheet id (can be used instead of sheet name in range)
        value_render_option: str, how values should be rendered ("formatted" or "unformatted")
        date_time_render_option: str, how dates should be rendered ("formatted" or "formatted_string")
        
        Result:
        -----------------
        Dict with values in the specified range
        """
        token = spreadsheet_token or self._spread_sheet_token
        if not token:
            raise LarkException(msg="Spreadsheet token is required")
        
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/values"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        params = {
            'valueRenderOption': value_render_option,
            'dateTimeRenderOption': date_time_render_option,
            'range': range_str
        }
        
        if sheet_id:
            params['sheetId'] = sheet_id
        
        resp = request("GET", url, headers, params=params)
        
        if resp.get("code", -1) == 0:
            logger.info(f"Read sheet values success for range: {range_str}")
            return resp.get("data", {})
        else:
            logger.error(f"Read sheet values failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Read sheet values failed"))
    
    
    
    def update_sheet_values(self, range_str: str, values: List[List], spreadsheet_token: str = None, 
                        sheet_id: str = None, value_input_option: str = "raw"):
        """Update Values in a Sheet
        
        Args:
        -----------------
        range_str: str, cell range in A1 notation (e.g. "A1:D5")
        values: List[List], 2D array of values to update
        spreadsheet_token: str, optional spreadsheet token
        sheet_id: str, optional sheet id (can be used instead of sheet name in range)
        value_input_option: str, how input data should be interpreted ("raw" or "user_entered")
        
        Use case: obj.update_sheet_values("A1:B3", [[2,1]], sheet_id="37cd38")
        Result:
        -----------------
        Dict with update result
        """
        token = spreadsheet_token or self._spread_sheet_token
        sheet_id = sheet_id or self._sheet_id
        if not token:
            raise LarkException(msg="Spreadsheet token is required")
        
        if not sheet_id:
            raise LarkException(msg="Sheet ID is required for updating values")
        
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/values"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        payload = {
            'valueRange': {
                'range': f"{sheet_id}!{range_str}",
                'values': values
            },
            'valueInputOption': value_input_option
        }
        
        if sheet_id:
            payload['sheetId'] = sheet_id
        
        resp = request("PUT", url, headers, payload=payload)
        
        if resp.get("code", -1) == 0:
            logger.info(f"Update sheet values success for range: {range_str}")
            return resp.get("data", {})
        else:
            logger.error(f"Update sheet values failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Update sheet values failed"))
    
    
    def append_sheet_values(self, range_str: str, values: List[List], spreadsheet_token: str = None,
                           sheet_id: str = None, value_input_option: str = "raw"):
        """Append Values to a Sheet
        
        Args:
        -----------------
        range_str: str, cell range in A1 notation (e.g. "<Sheet1>!A1")
        values: List[List], 2D array of values to append
        spreadsheet_token: str, optional spreadsheet token
        sheet_id: str, optional sheet id (can be used instead of sheet name in range)
        value_input_option: str, how input data should be interpreted ("raw" or "user_entered")
        
        Use case: obj.append_sheet_values("37cd38!A1:B8", [[1, 1]])
        
        References: https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/append-data
        Result:
        -----------------
        Dict with append result
        """
        token = spreadsheet_token or self._spread_sheet_token
        if not token:
            raise LarkException(msg="Spreadsheet token is required")
        
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/values_append"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        payload = {
            'valueRange': {
                'range': range_str,
                'values': values
            },
            'valueInputOption': value_input_option,
            'insertDataOption': 'INSERT_ROWS'
        }
        
        if sheet_id:
            payload['sheetId'] = sheet_id
        
        resp = request("POST", url, headers, payload=payload)
        
        if resp.get("code", -1) == 0:
            logger.info(f"Append sheet values success for range: {range_str}")
            return resp.get("data", {})
        else:
            logger.error(f"Append sheet values failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Append sheet values failed"))


    def batchupdate_values_single_sheet(self, datas:List[List], sheet_range:str,
                spreadsheet_token: str=None, *, sheet_title: str = None, sheet_id: str = None, 
                value_input_option: str = "raw"):
        """Batch Update Values in a Sheet
        
        Args:
        ------------
        datas: List[List], 2D array of values to update
        date_range: str, date range to update (e.g. "A1:B2")
        """
        token = spreadsheet_token or self._spread_sheet_token
        if not token:
            raise LarkSheetException(msg="Spreadsheet token is required")

        # Parse Sheet ID
        if sheet_title is not None:
            sheet_id = self.sheets_mapping.get(sheet_title)
        
        sheet_id = sheet_id or self._sheet_id

        if sheet_id is None:
            raise LarkSheetException(
                "Sheet ID is required, but missing sheet title or sheet_id"
            )
        
        
        # Prepare Request
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/values_batch_update"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }

        # TODO: Right now just push under the number of column limition, there
        # should do the number of row limitation as well
        if len(datas[0]) > self._UPDATE_COL_LIMITION:
            raise LarkSheetException("Column limit exceeded")
        
        # Parse Column and Row
        if "!" in sheet_range:
            _, sheet_range = sheet_range.split("!")
            
        if ":" in sheet_range:
            start_cell, end_cell = sheet_range.split(":")
        else:
            start_cell = sheet_range
        
        start_col, start_row = parse_sheet_cell(start_cell)
        last_offset_row = 0
        for index, item in enumerate(data_generator(datas, self._UPDATE_ROW_LIMITION)):
            # adjust cell
            start_cell = offset_sheet_cell(
                start_cell, offset_row=last_offset_row, offset_col=0
            )
            end_cell = offset_sheet_cell(
                start_cell, offset_row=len(item) * index, offset_col=len(item[0])
            )
            sheet_range = f"{start_cell}:{end_cell}"
            
            # update last_offset_row
            last_offset_row += len(item)
            payload = {
                'valueRanges': [{
                        'range': f"{sheet_id}!{sheet_range}",
                        'values': item
                }],
                'valueInputOption': value_input_option
            }

            resp = request("POST", url, headers, payload=payload)

            if resp.get("code", -1) == 0:
                logger.info(f"Batch update sheet values success for range: {sheet_range}")
                return resp.get("data", {})
            else:
                logger.error(f"Batch update sheet values failed: {resp}")
                raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Batch update sheet values failed"))


    # TODO: ADD SHEET
    def add_sheet(self, properties: dict, spreadsheet_token: str = None):
        """Add a New Sheet to a Spreadsheet
        
        Args:
        -----------------
        properties: dict, sheet properties (must include at least 'title')
        spreadsheet_token: str, optional spreadsheet token
        
        Result:
        -----------------
        Dict with new sheet info
        """
        token = spreadsheet_token or self._spread_sheet_token
        if not token:
            raise LarkException(msg="Spreadsheet token is required")
        
        if not properties.get('title'):
            raise LarkException(msg="Sheet title is required")
        
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/sheets_batch_update"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        payload = {
            'requests': [{
                'addSheet': {
                    'properties': properties
                }
            }]
        }
        
        resp = request("POST", url, headers, payload=payload)
        
        if resp.get("code", -1) == 0:
            logger.info(f"Add sheet success with title: {properties.get('title')}")
            return resp.get("data", {}).get("replies", [{}])[0].get("addSheet", {})
        else:
            logger.error(f"Add sheet failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Add sheet failed"))


    # TODO: DELETE SHEET    
    def delete_sheet(self, sheet_id: str, spreadsheet_token: str = None):
        """Delete a Sheet from a Spreadsheet
        
        Args:
        -----------------
        sheet_id: str, ID of the sheet to delete
        spreadsheet_token: str, optional spreadsheet token
        
        Result:
        -----------------
        Dict with operation result
        """
        token = spreadsheet_token or self._spread_sheet_token
        if not token:
            raise LarkException(msg="Spreadsheet token is required")
        
        if not sheet_id:
            raise LarkException(msg="Sheet ID is required")
        
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/sheets_batch_update"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        payload = {
            'requests': [{
                'deleteSheet': {
                    'sheetId': sheet_id
                }
            }]
        }
        
        resp = request("POST", url, headers, payload=payload)
        
        if resp.get("code", -1) == 0:
            logger.info(f"Delete sheet success with ID: {sheet_id}")
            return resp.get("data", {})
        else:
            logger.error(f"Delete sheet failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Delete sheet failed"))
            

    def clear_sheet_values(self, range_str, spreadsheet_token: str = None, sheet_id: str = None):
        """Clear Values in a Sheet Range or multiple ranges
        
        Args:
        -----------------
        range_str: str or List[str], cell range(s) in A1 notation (e.g. "<Sheet1>!A1:D5" or ["<Sheet1>!A1:D5", "<Sheet1>!F1:H10"])
            Note: Each range MUST include a sheet name or sheet ID (e.g. "<Sheet1>!A1:D5")
        spreadsheet_token: str, optional spreadsheet token
        sheet_id: str, optional default sheet id (will NOT be used if ranges already contain sheet identifiers)
        
        Result:
        -----------------
        Dict with clear operation result
        
        Required Permissions:
        -----------------
        sheets:spreadsheet or sheets:spreadsheet:write
        """
        token = spreadsheet_token or self._spread_sheet_token
        if not token:
            raise LarkException(msg="Spreadsheet token is required")
        
        # 处理单个范围或多个范围的情况
        ranges = range_str if isinstance(range_str, list) else [range_str]
        
        # 检查所有范围是否包含 sheet 标识，如果不包含则抛出异常
        for r in ranges:
            if "!" not in r:
                raise LarkException(msg=f"Sheet identifier is required in each range string. Missing in: '{r}'")
        
        # 构建范围字符串列表 (所有范围都必须已经包含 sheet 标识)
        processed_ranges = ranges
        
        # 使用 values_batch_update 接口通过设置空值来清除数据
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/values_batch_update"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        # 为每个范围创建对应的空值数组
        value_ranges = []
        
        for range_str in processed_ranges:
            try:
                # 检查范围是否包含 :，这表示它是一个范围而不是单个单元格
                if ":" in range_str:
                    # 假设格式为 "<Sheet1>!A1:D5" 或者已经处理为这种格式
                    sheet_and_range = range_str.split("!")
                    if len(sheet_and_range) != 2:
                        raise ValueError(f"Invalid range format: {range_str}")
                    
                    cell_range = sheet_and_range[1]
                    start_cell, end_cell = cell_range.split(":")
                    
                    # 提取结束单元格的行号和列号
                    start_col_match, start_row_match = parse_sheet_cell(start_cell)
                    end_col_match, end_row_match = parse_sheet_cell(end_cell)
                    
                    # 计算需要清除的行数和列数
                    def col_to_num(col):
                        num = 0
                        for c in col:
                            num = num * 26 + (ord(c) - ord('A') + 1)
                        return num
                    
                    start_col = col_to_num(start_col_match.group())
                    end_col = col_to_num(end_col_match.group())
                    start_row = int(start_row_match.group())
                    end_row = int(end_row_match.group())
                    
                    rows = end_row - start_row + 1
                    cols = end_col - start_col + 1
                    
                    # 创建空值数组
                    empty_values = [['' for _ in range(cols)] for _ in range(rows)]
                else:
                    # 如果只是单个单元格，则创建一个1x1的空数组
                    empty_values = [['']]
                
                value_ranges.append({
                    'range': range_str,
                    'values': empty_values
                })
                
            except Exception as e:
                logger.error(f"Error processing range {range_str}: {str(e)}")
                raise LarkException(msg=f"Failed to process range {range_str}: {str(e)}")
        
        payload = {
            'valueRanges': value_ranges,
            'valueInputOption': 'RAW'
        }
        
        try:
            resp = request("POST", url, headers, payload=payload)
            
            if resp.get("code", -1) == 0:
                ranges_str = ", ".join(processed_ranges)
                logger.info(f"Clear sheet values success for ranges: {ranges_str}")
                return resp.get("data", {})
            else:
                error_code = resp.get("code", -1)
                error_msg = resp.get("msg", "Clear sheet values failed")
                
                # 权限相关错误码处理
                if error_code == 11403:
                    logger.error("Permission denied: The application lacks necessary permissions (sheets:spreadsheet:write)")
                    raise LarkException(code=error_code, msg="Application lacks required permissions: sheets:spreadsheet:write")
                elif error_code == 11412:
                    logger.error("Access denied: The application doesn't have permission to access this spreadsheet")
                    raise LarkException(code=error_code, msg="No permission to access or modify this spreadsheet")
                else:
                    logger.error(f"Clear sheet values failed: {resp}")
                    raise LarkException(code=error_code, msg=error_msg)
        except Exception as e:
            if isinstance(e, LarkException):
                raise
            logger.error(f"Error clearing sheet values: {str(e)}")
            raise LarkException(msg=f"Failed to clear sheet values: {str(e)}")
        
    
    