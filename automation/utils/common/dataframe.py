#coding:utf-8
"""Pandas DataFrame Utility Functions
* dataframe2record: Convert DataFrame to list of records

"""


import numpy as np
from decimal import Decimal


def dataframe2record(df, type: str = "raw"):
    """Convert DataFrame to list of records

    Args:
        df: DataFrame to convert
        type: Type of conversion
            'raw' - raw values, just extract values in each row
            'dict' - key value mapping, key the column name
    Returns:
        List of records
    """
    records = []
    if type == "raw":
        for _, item in df.iterrows():
            record = []
            for col in df.columns:
                if not np.isnan(item.get(col)):
                    if isinstance(item.get(col), (Decimal)):
                        record.append(float(item.get(col)))
                    elif isinstance(item.get(col), (np.int64, np.int32, np.int16, np.int8)):
                        record.append(int(item.get(col)))
                    else:
                        record.append(item.get(col))
                else:
                    record.append(None)
            records.append(record)
    elif type == "dict":
        data = df.to_dict(orient="records")
        for item in data:
            for key, value in item.items():
                if not np.isnan(value):
                    if isinstance(value, (Decimal)):
                        item[key] = float(value)
                    elif isinstance(value, (np.int64, np.int32, np.int16, np.int8)):
                        item[key] = int(value)
                else:
                    item[key] = None
            records.append(item)
    return records