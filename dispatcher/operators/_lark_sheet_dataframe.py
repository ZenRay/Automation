#coding:utf-8
"""Shared helpers: DataFrame → Lark Sheet values (row/column limits handled here, not at MaxCompute)."""

import logging
import time

import pandas as pd

from automation.client.lark.utils import (
    parse_column2index,
    parse_index2column,
    parse_sheet_cell,
)
from automation.utils.common.dataframe import dataframe2record

logger = logging.getLogger(__name__)


def apply_lark_sheet_date_column(df: pd.DataFrame, lark_sheets) -> None:
    """Align optional `日期` column to Lark serial day integers (mutates df)."""
    if "日期" in df.columns and df["日期"].dtype != "int64":
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce").apply(
            lambda x: x - lark_sheets._START_DATE if pd.notna(x) else x
        ).dt.days


def extract_dataframe_to_sheet_values(
    df: pd.DataFrame,
    columns,
    start_cell: str,
    sheet_title: str,
    lark_sheets,
    batch_size: int = 0,
    include_header: bool = True,
) -> None:
    """Write DataFrame to a Lark sheet using batch value updates (≤5000 rows × 100 cols per request).

    Args:
        df: Data to write (subset columns should match ``columns``).
        columns: Column names in order.
        start_cell: Top-left cell for this write (including after row offset for streaming chunks).
        sheet_title: Target sheet title.
        lark_sheets: LarkSheets client.
        batch_size: If >0, column batch width; if 0 and many columns, defaults to 20 inside column split.
        include_header: If True, first row is column headers; if False, only data rows (continuation chunks).
    """
    if len(df) == 0 and not include_header:
        return

    col_lim = lark_sheets._UPDATE_COL_LIMITATION
    if len(columns) > col_lim or batch_size > 0:
        logger.warning("Data column count exceeds limit or specified batch size, splitting required.")
        if batch_size == 0:
            batch_size = 20
        batch_indexes = list(range(0, len(columns) + 1, batch_size)) + [len(columns) + 1]
    else:
        batch_indexes = [0, len(columns) + 1]

    sheet_start_col, sheet_start_row = parse_sheet_cell(start_cell, parse_type="start")
    start_column_index = batch_indexes[0]
    for batch in batch_indexes[1:]:
        batch_columns = columns[start_column_index:batch]
        sub = df.loc[:, batch_columns]
        if include_header:
            data = [sub.columns.to_list()]
            data += dataframe2record(sub, type="raw")
        else:
            data = dataframe2record(sub, type="raw")

        cell = f"{parse_index2column(parse_column2index(sheet_start_col) + start_column_index)}{sheet_start_row}"
        end_cell = (
            f"{parse_index2column(parse_column2index(sheet_start_col) + start_column_index + len(batch_columns) - 1)}"
            f"{sheet_start_row + len(data) - 1}"
        )
        data_range = f"{cell}:{end_cell}"

        lark_sheets.batchupdate_values_single_sheet(
            data,
            data_range=data_range,
            sheet_id=lark_sheets.get_sheet_id(sheet_title),
        )
        logger.debug("Batch columns %s sent to range %s", batch_columns, data_range)
        start_column_index = batch
        time.sleep(2)
