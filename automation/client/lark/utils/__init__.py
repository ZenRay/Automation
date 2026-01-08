#coding:utf8

from .lark_request import request

from .spread_sheet import (
    parse_sheet_cell,
    offset_sheet_cell,
    parse_column2index,
    parse_index2column
)



def data_generator(data, batch_size=100):
    """Generate batches of data.

    Generate batches of data, which can be processed in smaller chunks.
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]