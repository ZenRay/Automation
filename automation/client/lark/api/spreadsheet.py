# coding:utf8
import datetime
import time
from typing import List

import logging
import re
import threading
from datetime import datetime, timedelta


from ..base import (
    AccessToken, LarkClient, SheetItem, SpreadSheetMeta,
    SheetValueRenderOption, SheetDateTimeRenderOption,
    SheetAppendValuesOption
)
from ..utils import (
    request, data_generator, parse_sheet_cell, offset_sheet_cell,
    parse_column2index, parse_index2column
)
from ..exceptions import LarkException, RegexException, LarkSheetException



logger = logging.getLogger("automation.lark.api.spread_sheet")




class LarkSheets(LarkClient):
    """Lark Sheets Process"""

    # Sheet Value Update Limition
    _UPDATE_ROW_LIMITATION = 5000
    _UPDATE_COL_LIMITATION = 100
    
    def __init__(self, app_id, app_secret, lark_host="https://open.feishu.cn", url=None):
        super().__init__(app_id=app_id, app_secret=app_secret, lark_host=lark_host)
        self._spread_sheet = SpreadSheetMeta()
        self._active_sheet_id = None
        self._app_type = None
        self._regex_pattern = re.compile(r"http.*/(?P<app_type>sheets|wiki)/(?P<token>[a-zA-Z0-9_-]+)(?:\?sheet=(?P<sheet_id>[a-zA-Z0-9_-]+))?", re.I)

        if url is not None:
            self.extract_spreadsheet_info(url)
            self.extract_sheets()
    
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
        
        Returns a dictionary with sheet titles as keys and sheet IDs as values.
        """
        if not self._spread_sheet.spreadsheet_token:
            raise LarkException(
                "Spreadsheet token is required to get sheets mapping. "
                "Use obj.extract_spreadsheet_info(url) to extract it from the URL"
            )
        
        # If we don't have sheet items yet, fetch them
        if not self._spread_sheet.sheet_items:
            self.extract_sheets()
            
        # Create a mapping dictionary from the sheet items
        mapping = {"SpreadSheet": self._spread_sheet.spreadsheet_token}
        
        for sheet_item in self._spread_sheet.sheet_items:
            mapping[sheet_item.sheet_title] = sheet_item.sheet_id
            
        return mapping
    
    
    @property
    def spread_sheet(self):
        """Spread Sheet Token"""
        if not self._spread_sheet.spreadsheet_token:
            raise LarkException(
                "Must call extract_spreadsheet_info(url) first"
                " or Set spread_sheet value"
            )

        return self._spread_sheet.spreadsheet_token

    @spread_sheet.setter
    def spread_sheet(self, value):
        """Set Spread Sheet Token
        
        Pass URL or Spread Sheet Token, if get url then extract spread sheet
        token. Otherwise set the value
        """
        if value.startswith("http"):
            self.extract_spreadsheet_info(value)
        else:
            self._spread_sheet.spreadsheet_token = value
            
        logger.info(f"Update Spread Sheet Token: {self._spread_sheet.spreadsheet_token} Success.")
        
        
    
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
            self._active_sheet_id = match.group("sheet_id")
            # Update the SpreadSheetMeta object
            self._spread_sheet.spreadsheet_token = token
            
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
                    # Update the SpreadSheetMeta object
                    self._spread_sheet.spreadsheet_token = obj_token
                    logger.info(f"Retrieved spreadsheet token {obj_token} from wiki node")
                else:
                    logger.error(f"Failed to extract spreadsheet token from wiki node: {resp}")
                    raise LarkException(msg="Could not extract spreadsheet token from wiki node")
            else:
                logger.error(f"Failed to get wiki node information: {resp}")
                raise LarkException(code=resp.get("code", -1), msg=resp.get("msg", "Failed to get wiki node information"))
        
        if not self._spread_sheet.spreadsheet_token:
            raise RegexException("Could not extract spreadsheet token from URL")
        
        logger.info(f"Extracted spreadsheet token: {self._spread_sheet.spreadsheet_token}, sheet id: {self._active_sheet_id}, app type: {self._app_type}")
        return self._spread_sheet.spreadsheet_token, self._active_sheet_id
    
    
    def extract_spreadsheet_meta(self, spreadsheet_token: str = None):
        """Get Spreadsheet Metadata
        
        Args:
        -----------------
        spreadsheet_token: str, optional spreadsheet token
        
        Result:
        -----------------
        Dict with spreadsheet metadata
        """
        token = self._check_spreadsheet_token(spreadsheet_token)
        
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/metainfo"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        resp = request("GET", url, headers)
        
        if resp.get("code", -1) == 0:
            logger.info(f"Get spreadsheet metadata success: {token}")
            # Update the token if it was provided as a parameter
            if spreadsheet_token:
                self._spread_sheet.spreadsheet_token = token
            
            # Update the SpreadSheetMeta with title from response
            meta_data = resp.get("data", {}).get("properties", {})
            sheets = meta_data = resp.get("data", {}).get("sheets", [])
            if "title" in meta_data:
                self._spread_sheet.spreadsheet_title = meta_data["title"]
                
            
            for sheet in sheets:
                self._spread_sheet.sheet_items = SheetItem(
                    sheet_id=sheet.get("sheetId")
                    ,sheet_title=sheet.get("title")
                )
            return meta_data
        else:
            logger.error(f"Get spreadsheet metadata failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Get spreadsheet metadata failed"))
    

    
    

    def read_sheet_values(self, range_str: str, spreadsheet_token: str = None, sheet_id: str = None, 
            value_render_option: SheetValueRenderOption = SheetValueRenderOption.FORMATTED_VALUE, 
            date_time_render_option: SheetDateTimeRenderOption = SheetDateTimeRenderOption.FORMATTED_STRING):
        """Read Values from a Sheet
        
        Args:
        -----------------
        range_str: str, cell range in A1 notation (e.g. "<Sheet1>!A1:D5")
        spreadsheet_token: str, optional spreadsheet token
        sheet_id: str, optional sheet id or title (can be used instead of sheet name in range)
        value_render_option: str, how values should be rendered:
            * ToString, Return Values as String
            * Formula, Return formula values that can't calulate
            * FormattedValue, Return values with formatting applied, and formula values that calculated
            * UnformattedValue, Return values without formatting applied, and formula values that calculated
        date_time_render_option: str, how dates should be rendered
            * FormattedString,  Return values with formatted date, and formula values that calculated
            * None, Return calculate days since difference since 1899-12-30, and float value is 
                ration in 24h
        Result:
        -----------------
        Dict with values in the specified range
        """
        token = self._check_spreadsheet_token(spreadsheet_token)
        
        # Extract real sheet_id and concate sheet id and range info
        # Update url
        range_str = self._update_data_range(range_str, sheet_id=sheet_id)
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/values/{range_str}"
        
        
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        params = {
            'valueRenderOption': value_render_option.value,
            'dateTimeRenderOption': date_time_render_option.value,
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
                        sheet_id: str = None):
        """Update Values in a Sheet
        
        Args:
        -----------------
        range_str: str, cell range in A1 notation (e.g. "A1:D5")
        values: List[List], 2D array of values to update
        spreadsheet_token: str, optional spreadsheet token
        sheet_id: str, optional sheet id (can be used instead of sheet name in range)
        
        Use case: obj.update_sheet_values("A1:B3", [[2,1]], sheet_id="37cd38")
        Result:
        -----------------
        Dict with update result
        """
        token = self._check_spreadsheet_token(spreadsheet_token)
        sheet_id = sheet_id or self._active_sheet_id
        
        if not sheet_id:
            raise LarkException(msg="Sheet ID is required for updating values")
        
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/values"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        payload = {
            'valueRange': {
                'range': self._update_data_range(range_str, sheet_id=sheet_id),
                'values': values
            },
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
                    sheet_id: str = None, value_input_option: str = "raw",
                    insert_data_option: SheetAppendValuesOption = SheetAppendValuesOption.INSERT_ROWS):
        """Append Values to a Sheet
        
        Args:
        -----------------
        range_str: str, cell range in A1 notation (e.g. "<Sheet1>!A1")
        values: List[List], 2D array of values to append
        spreadsheet_token: str, optional spreadsheet token
        sheet_id: str, optional sheet id (can be used instead of sheet name in range)
        value_input_option: str, how input data should be interpreted ("raw" or "user_entered")
        insert_data_option: SheetAppendValuesOption, how to insert data ("OVERWRITE" or "INSERT_ROWS")

        Use case: obj.append_sheet_values("37cd38!A1:B8", [[1, 1]])
        
        References: https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/append-data
        Result:
        -----------------
        Dict with append result
        """
        token = self._check_spreadsheet_token(spreadsheet_token)
        
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
            'insertDataOption': insert_data_option.value
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

    
    def batchupdate_values_single_sheet(self, datas:List[List], data_range:str,
                spreadsheet_token: str=None, *, sheet_id: str = None, 
                value_input_option: str = "raw"):
        """Batch Update Values in a Sheet
        
        Args:
        ------------
        datas: List[List], 2D array of values to update
        date_range: str, date range to update (e.g. "A1:B2")
        """
        token = self._check_spreadsheet_token(spreadsheet_token)

        # Parse Sheet ID
        if sheet_id is not None:
            sheet_id = self.get_sheet_id(sheet_id)
                
        
        # Prepare Request
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/values_batch_update"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        # Single range update
        if isinstance(data_range, str):
            data_range = self._update_data_range(
                data_range, sheet_id, update_sheet_method="new"
            )
            # TODO: Right now just push under the number of column limition, there
            # should do the number of row limitation as well
            if len(datas[0]) > self._UPDATE_COL_LIMITATION:
                raise LarkSheetException("Column limit exceeded")
            
            
            start_col, start_row = parse_sheet_cell(data_range, "start")

            # init last_offset_row
            last_offset_row = 0

            for index, item in enumerate(data_generator(datas, self._UPDATE_ROW_LIMITATION)):
                # adjust cell
                start_cell = offset_sheet_cell(
                    f"{start_col}{start_row}", offset_row=last_offset_row, offset_col=0
                )
                _, start_cell_row = parse_sheet_cell(start_cell, "single")
                end_cell = offset_sheet_cell(
                    start_cell, offset_row=len(item) - 1, offset_col=len(item[0])-1
                )
                data_range = f"{start_cell}:{end_cell}"
                
                # update last_offset_row
                last_offset_row += len(item)
                payload = {
                    'valueRanges': [{
                            'range': f"{sheet_id}!{data_range}",
                            'values': item
                    }],
                    'valueInputOption': value_input_option
                }

                resp = request("POST", url, headers, payload=payload, refresh_client=self)

                if resp.get("code", -1) == 0:
                    logger.debug(f"Batch update sheet values success for range: {data_range}")
                    time.sleep(2)
                else:
                    logger.error(f"Batch update sheet values failed: {resp}")
                    raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Batch update sheet values failed"))
            logger.info(f"Batch update values success!")

    
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
        token = self._check_spreadsheet_token(spreadsheet_token)
        
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
            # Refresh
            self.extract_sheets()
            logger.info(f"Add sheet success with title: {properties.get('title')}")
            return resp.get("data", {}).get("replies", [{}])[0].get("addSheet", {})
        else:
            logger.error(f"Add sheet failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Add sheet failed"))


    
    def delete_sheet(self, sheet_id: str, spreadsheet_token: str = None):
        """Delete a Sheet from a Spreadsheet
        
        Args:
        -----------------
        sheet_id: str, ID or Title of the sheet to delete
        spreadsheet_token: str, optional spreadsheet token
        
        Result:
        -----------------
        Dict with operation result
        """
        token = self._check_spreadsheet_token(spreadsheet_token)
        
        if not sheet_id:
            raise LarkException(msg="Sheet ID is required")
        
        raw_sheet_id = sheet_id
        if self._check_sheet_exits(sheet_id):
            sheet_id = self.get_sheet_id(sheet_id)
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
                logger.error(f"'{sheet_id}'('{raw_sheet_id}') exists, but delete sheet failed: {resp}")
                raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Delete sheet failed"))
        else:
            logger.error(f"Sheet with ID '{raw_sheet_id}' does not exist")
            raise LarkSheetException(msg=f"Sheet with ID '{raw_sheet_id}' does not exist")


    
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
        token = self._check_spreadsheet_token(spreadsheet_token)
        
        # Handle single range or multiple ranges
        ranges = range_str if isinstance(range_str, list) else [range_str]
        
        # Build range string list (all ranges must already contain sheet identifiers)
        processed_ranges = ranges

        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }

        # Create empty value arrays for each range
        less_row_limitation_ranges = []
        over_row_limitation_ranges = []
        total_row_records = 1
        for range_str in processed_ranges:
            range_str = self._update_data_range(range_str, sheet_id=sheet_id)
            try:
                # Check if range contains a colon, which indicates it's a range rather than a single cell
                if ":" in range_str:
                    # Assume format is "<Sheet1>!A1:D5" or already processed to this format
                    sheet_and_range = range_str.split("!")
                    
                    cell_range = sheet_and_range[1]
                    start_cell, end_cell = cell_range.split(":")
                    
                    # Extract column and row from cells
                    start_col, start_row = parse_sheet_cell(start_cell)
                    end_col, end_row = parse_sheet_cell(end_cell)
                    
                    start_col_num = parse_column2index(start_col)
                    end_col_num = parse_column2index(end_col)
                    
                     # Calculate number of columns
                    cols = end_col_num - start_col_num + 1
                    # if cell not contain number
                    
                    if (re.search("\d", start_cell) and re.search("\d", end_cell)):
                        # Calculate number of rows
                        rows = end_row - start_row + 1
                        
                    else:
                        # parse the data length
                        rows = 1
                        for index in range(start_col_num, end_col_num + 1):
                            col_name = parse_index2column(index)
                            current_col_data = self.read_sheet_values(
                                f"{sheet_id}!{col_name}:{col_name}"
                            ).get("valueRange", {}).get("values", [])
                            
                            time.sleep(2)
                            if len(current_col_data) > rows:
                                rows = len(current_col_data)
                    
                                total_row_records = rows
                    # # Create empty value array
                    empty_values = [[None for _ in range(cols)] for _ in range(rows)]
                
                else:
                    # check total_rows
                    sheet_and_range = range_str.split("!")
                    cell = sheet_and_range[-1]
                    _, total_row_records = parse_sheet_cell(cell)
                    empty_values = [[None]]
                    
                    start_cell, end_cell = cell_range.split(":")
                    start_col, start_row = parse_sheet_cell(start_cell)
                    end_col, end_row = parse_sheet_cell(end_cell)

                    cols = end_col - start_col + 1
                    rows = end_row - start_row + 1
                    if not re.search(r"\d", cell):
                        current_col_data = self.read_sheet_values(
                            f"{sheet_id}!{cell}"
                        ).get("valueRange", {}).get("values", [])
                        time.sleep(2)
                        if len(current_col_data) > rows:
                            rows = len(current_col_data)
                            total_row_records = rows
                            
                    empty_values = [[None for _ in range(cols)] for _ in range(rows)]

                # divide the datas by ROW LIMITATION
                if total_row_records < self._UPDATE_ROW_LIMITATION:
                    less_row_limitation_ranges.append({
                        'range': range_str,
                        'values': empty_values
                    })
                else:
                    over_row_limitation_ranges.append({
                        'range': range_str,
                        'values': empty_values
                    })
            except Exception as e:
                logger.error(f"Error processing range {range_str}: {str(e)}")
                raise LarkException(msg=f"Failed to process range {range_str}: {str(e)}")
        
        if len(less_row_limitation_ranges) > 0:
            # Use values_batch_update API to clear data by setting empty values
            url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/values_batch_update"
            
            payload = {
                'valueRanges': less_row_limitation_ranges,
                'valueInputOption': 'RAW'
            }
            
            try:
                resp = request("POST", url, headers, payload=payload, refresh_client=self)
                
                if resp.get("code", -1) == 0:
                    ranges_str = ", ".join(processed_ranges)
                    logger.info(f"Clear sheet values success for ranges: {ranges_str}")
                    
                    if len(over_row_limitation_ranges) == 0:
                        return None
                    #     return resp.get("data", {})
                else:
                    error_code = resp.get("code", -1)
                    error_msg = resp.get("msg", "Clear sheet values failed")
                    
                    logger.error(f"Clear sheet values failed: {resp}")
                    raise LarkException(code=error_code, msg=error_msg)

            except Exception as e:
                logger.error(f"Error clearing sheet values: {str(e)}")
                if isinstance(e, LarkException):
                    raise
                raise LarkException(msg=f"Failed to clear sheet values: {str(e)}")
        
        if len(over_row_limitation_ranges) > 0:
            
            for range_info in over_row_limitation_ranges:
                self.batchupdate_values_single_sheet(
                    range_info["values"], data_range=range_info["range"], sheet_id=sheet_id
                )
                
                time.sleep(2)

    def update_single_range_style(self,  range_str: str, format_style:dict, spreadsheet_token:str = None, sheet_id:str=None):
        """Update Single Range Style
        
        Update cell style in single range. Request format style like, 'style' is the argument `format_style`:
        {
        "appendStyle":{
            "range": "1QXD0s!A3:C4",
            "style":{
                "font":{
                    "bold":true,
                    "italic":true,
                    "fontSize":"10pt/1.5",
                    "clean":false  
                    },    
                "textDecoration":0,
                "formatter":"",
                "hAlign": 0 , 
                "vAlign":0,   
                "foreColor":"#000000",
                "backColor":"#21d11f",
                "borderType":"FULL_BORDER",
                "borderColor": "#ff0000",
                "clean": false 
                }
            }
        }
        
        Reference: https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/set-cell-style
        
        Args:
        --------------
        range_str, str: The A1 notation range to update.
        format_style, dict: The style to apply to the range.
        spreadsheet_token, str: The token of the spreadsheet.
        sheet_id, str: The ID of the sheet or the title of sheet
        """

        token = self._check_spreadsheet_token(spreadsheet_token)

        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }

        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{token}/style"
        range_str = self._update_data_range(range_str, sheet_id=sheet_id)
        payload = {
            "appendStyle":{
                "range": range_str,
                "style":format_style
                }
        }
        
        try:
            resp = request("PUT", url, headers, payload=payload, refresh_client=self)
            
            if resp.get("code", -1) == 0:
                logger.info(f"Update Style Success at: {range_str}")

            else:
                error_code = resp.get("code", -1)
                error_msg = resp.get("msg", f"Update Range('{range_str}') Style failed")

                logger.error(f"Update sheet style failed: {resp}")
                raise LarkException(code=error_code, msg=error_msg)

        except Exception as e:
            logger.error(f"Error updating sheet style: {str(e)}")
            if isinstance(e, LarkException):
                raise
            raise LarkException(msg=f"Failed to update sheet style: {str(e)}")


    def _update_data_range(self, range_str: str, sheet_id: str=None, update_sheet_method: str="old"):
        """Update Data Range
        
        Update data range, according by the sheet id
        1. Pass data range with sheet_id
            * if `update_sheet_method` is 'new', must pass `sheet_id` to update data_range
            * if `update_sheet_method` is 'old', use raw sheet_id in range_str. But
                if lack of sheet id, raise Exception
        2. Pass data range without sheet_id, there must be a `sheet_id`

        Args:
        --------------
        range_str: str data range that can or can't contain sheet id
        sheet_id: str sheet id
        update_sheet_method: str default "old" whether update sheet_id in range_str, "old" or "new"
            if "old" use raw sheet_id in range_str, else update sheet_id
        """
        # Extract real sheet_id and concate sheet id and range info
        if "!" in range_str:
            range_sheet_id, range_str = range_str.split("!")
            
            if update_sheet_method == "new":
                if sheet_id is None:
                    raise LarkSheetException(
                        f"Force update sheet_id must pass sheet_id, but get None"
                    )
                
                range_sheet_id = sheet_id
        elif sheet_id is not None:
            range_sheet_id = sheet_id
        else:
            raise LarkSheetException(
                f"Lack of sheet_id, you can pass sheet_id or range_str with sheet id"
                    ", but get {range_str}"
            )
        
        sheet_id = self.get_sheet_id(range_sheet_id)
        
        # Update range
        range_str = f"{sheet_id}!{range_str}"
        
        return range_str 
    
    def extract_sheets(self, spreadsheet_token: str = None):
        """Get Sheets in a Spreadsheet
        
        Args:
        -----------------
        spreadsheet_token: str, optional spreadsheet token
        
        Result:
        -----------------
        List of sheets in the spreadsheet
        """
        token = self._check_spreadsheet_token(spreadsheet_token)
        
        url = f"{self._host}/open-apis/sheets/v3/spreadsheets/{token}/sheets/query"
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.access_token}',
        }
        
        resp = request("GET", url, headers)
        
        if resp.get("code", -1) == 0:
            logger.info(f"Get sheets list success for spreadsheet: {token}")
            sheets_data = resp.get("data", {}).get("sheets", [])
            
            # Clear existing sheet items before adding new ones
            self._spread_sheet.clear_sheets_meta()
            
            # Convert to SheetItems and add to SpreadSheetMeta
            for sheet in sheets_data:
                sheet_item = {
                    "sheet_id": sheet.get("sheet_id"),
                    "sheet_title": sheet.get("title")
                }
                self._spread_sheet.append(sheet_item)
                
            return sheets_data
        else:
            logger.error(f"Get sheets list failed: {resp}")
            raise LarkException(code=resp.get("code"), msg=resp.get("msg", "Get sheets list failed"))
        
        
    
    def _check_sheet_exits(self, sheet_id: str):
        """Check if a Sheet Exists in the Current Sheet Mapping
        
        Args:
        -----------------
        sheet_id: str, ID or Title of the sheet to check
        
        Result:
        -----------------
        Bool indicating if the sheet exists
        """
        # Use SpreadSheetMeta to check if sheet exists
        self.extract_sheets(self._spread_sheet.spreadsheet_token)
        sheet = self._spread_sheet.get_sheet_by_id(sheet_id) or self._spread_sheet.get_sheet_by_title(sheet_id)
        
        return sheet is not None
    
    def _check_spreadsheet_token(self, spreadsheet_token):
        """Check SpreadSheet Token Exists"""
        token = spreadsheet_token or self._spread_sheet.spreadsheet_token
        
        if not token:
            raise LarkException(msg="Spreadsheet token is required")
        return token
    

    def get_sheet_id(self, sheet_id: str):
        """Get Actual Sheet ID
        
        Use sheet ID or sheet Title to extract actual sheet id
        
        Args:
        ----------------
        sheet_id: str, ID or Title of sheet
        
        Result:
        ----------------
        str: Actual sheet ID
        """
        sheet = self._spread_sheet.get_sheet_by_id(sheet_id) or self._spread_sheet.get_sheet_by_title(sheet_id)
        return sheet.sheet_id if sheet else None
    
    
