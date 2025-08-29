#coding:utf8
import requests
import logging
import string


from ..exceptions import LarkException, LarkSheetException


logger = logging.getLogger("automation.lark.utils")

# Sheet Cell Regex Pattern
__CELL_PATTERN = re.compile(r"^(?P<col>[A-Z]{0,})(?P<row>\d{0,})$")

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

    

def data_generator(data, batch_size=100):
    """Generate batches of data.

    Generate batches of data, which can be processed in smaller chunks.
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]
        


def parse_sheet_cell(cell: str):
    """Parse a sheet cell reference.
    Parse Cell Address `col` and `row` from a cell reference string. If there
    is only a column or a row, set `row` is 1 and `col` is "A".

    Args:
        cell (str): The cell reference (e.g., "A1").

    Returns:
        tuple: A tuple containing the column (str) and row (int) of the cell.
    """
    match = __CELL_PATTERN.match(cell)
    col = match.group("col")
    row = int(match.group("row")) if match.group("row") else 1

    # Raise ValueError if cell reference is invalid
    # * Format like "9A"
    # * Parse Cell Fail
    if not match:
        raise LarkSheetException(f"Invalid cell reference: {cell}")
    
    
    # Set column with start column, if empty
    col =  col.upper()
    if len(col) == 0:
        col = "A"
    

    return col, row



def offset_sheet_cell(current_cell: str, *, offset_row: int = None, offset_col: int = None):
    """Offset a sheet cell reference by a certain number of rows and columns.

    Args:
        current_cell (str): The current cell reference (e.g., "A1").
        offset_row (int): The number of rows to offset (can be negative).
        offset_col (int): The number of columns to offset (can be negative).

    Returns:
        str: The new cell reference after applying the offset.
    """
    if offset_col is None:
        offset_col = 0
    
    if offset_row is None:
        offset_row = 0
    
    # parse col and row
    col, row = parse_sheet_cell(current_cell)
    
    new_row = row + offset_row
    if new_row < 1:
        raise LarkSheetException(f"Row index must be greater than 0 after offset, got {new_row}")

    # Use different column offset functions based on the sign of the offset
    if offset_col > 0:
        new_col = _column_positive_offset(col, offset_col)
    elif offset_col < 0:
        new_col = _column_negative_offset(col, abs(offset_col))
    else:
        new_col = col
        
    return f"{new_col}{new_row}"



def _column_positive_offset(col, offset):
    """Apply a positive offset to a column letter.

    Args:
        col (str): The current column letter (e.g., "A").
        offset (int): The number of positions to offset the column.

    Returns:
        str: The new column letter after applying the offset.
    """
    if not col or offset < 1:
        return col

    new_col = ""
    current_offset = 0
    offset_col = offset
    to_continue = True
    while to_continue:
        if offset_col > 26:
            current_offset = offset_col % 26
            offset_col = offset_col // 26
        else:
            current_offset = offset_col
            offset_col = 0
        
        if ord(col[-1])  + current_offset > ord("Z"):
            offset_col += 1
            new_col = chr(ord(col[-1]) + current_offset - 26) + new_col
        else:
            new_col = chr(ord(col[-1]) + current_offset) + new_col
        
        # update origin col
        col = col[:-1]
        
        if offset_col == 0:
            new_col = col + new_col
            col = ""
            to_continue = False
        elif len(col) > 0 and ord(col[-1]) + offset_col <= ord("Z"):
            new_col = chr(ord(col[-1]) + offset_col) + new_col
            new_col = col[:-1] + new_col
            col = ""
            to_continue = False
        elif len(col) == 0 and offset_col > 0 and offset_col < 26:
            new_col = string.ascii_uppercase[offset_col - 1] + new_col
            col = ""
            to_continue = False


    return new_col


def _column_negative_offset(col, offset):
    """Apply a negative offset to a column letter.

    Args:
        col (str): The current column letter (e.g., "C").
        offset (int): The absolute value of positions to move backward (e.g., 2 to go from "C" to "A").

    Returns:
        str: The new column letter after applying the negative offset.
        
    Raises:
        LarkSheetException: If the offset would result in a column before "A".
    """
    if not col or offset < 1:
        return col
    
    # parse column char to value
    col_value = 0
    for char in col:
        col_value = col_value * 26 + (ord(char) - ord('A') + 1)

    # apply negative offset
    new_col_value = col_value - offset

    # check column is before 'A' column
    if new_col_value < 1:
        raise LarkSheetException(f"Column offset would result in a column before 'A': {col} with offset -{offset}")

    # alternative column value to char
    result = ""
    while new_col_value > 0:
        remainder = (new_col_value - 1) % 26
        result = chr(ord('A') + remainder) + result
        new_col_value = (new_col_value - 1) // 26
    
    return result