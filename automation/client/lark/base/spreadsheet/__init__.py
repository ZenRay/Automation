#coding:utf8
from enum import Enum
from ._sheet import SheetItem
from ._spreadsheet import SpreadSheetMeta



class SheetValueRenderOption(Enum):
    """Sheet Value Render Option"""
    TO_STRING = "ToString"
    FORMATTED_VALUE = "FormattedValue"
    UNFORMATTED_VALUE = "UnformattedValue"
    FORMULA = "Formula"
    

class SheetDateTimeRenderOption(Enum):
    """Sheet Datetime Render Option"""
    NONE = None
    FORMATTED_STRING = "FormattedString"

    
class SheetAppendValuesOption(Enum):
    """Sheet Append Option
    
    OVERWRITE: overwrite the current cell value, if lenth of value is less than
        data range
    INSERT_ROWS: insert new cell,if lenth of value is less than
        data range 
    """
    OVERWRITE = "OVERWRITE"
    INSERT_ROWS = "INSERT_ROWS"