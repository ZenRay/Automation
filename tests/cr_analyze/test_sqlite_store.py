# coding:utf8
"""tests/cr_analyze/test_sqlite_store.py -- SQLite 存储测试"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from workers.cr_analyze.sqlite_store import (
    write_tables,
    read_table,
    list_tables,
    table_exists,
)


@pytest.fixture
def tmp_db(tmp_path) -> str:
    return str(tmp_path / "test.db")


@pytest.fixture
def sample_data() -> dict[str, pd.DataFrame]:
    return {
        "table_a": pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]}),
        "table_b": pd.DataFrame({"id": [10, 20], "name": ["foo", "bar"]}),
    }


class TestWriteTables:
    def test_write_returns_count(self, tmp_db, sample_data):
        count = write_tables(tmp_db, sample_data)
        assert count == 2

    def test_write_creates_file(self, tmp_db, sample_data):
        write_tables(tmp_db, sample_data)
        assert Path(tmp_db).exists()

    def test_write_creates_all_tables(self, tmp_db, sample_data):
        write_tables(tmp_db, sample_data)
        tables = list_tables(tmp_db)
        assert "table_a" in tables
        assert "table_b" in tables


class TestReadTable:
    def test_read_roundtrip(self, tmp_db, sample_data):
        write_tables(tmp_db, sample_data)
        df = read_table(tmp_db, "table_a")
        assert len(df) == 3
        assert list(df.columns) == ["x", "y"]

    def test_read_missing_table_raises(self, tmp_db, sample_data):
        write_tables(tmp_db, sample_data)
        with pytest.raises(ValueError, match="not found"):
            read_table(tmp_db, "nonexistent")


class TestTableOverwrite:
    def test_overwrite_replaces_data(self, tmp_db):
        data_v1 = {"t": pd.DataFrame({"v": [1, 2, 3]})}
        data_v2 = {"t": pd.DataFrame({"v": [10, 20]})}

        write_tables(tmp_db, data_v1)
        write_tables(tmp_db, data_v2)

        df = read_table(tmp_db, "t")
        assert len(df) == 2
        assert df["v"].tolist() == [10, 20]


class TestListTables:
    def test_empty_db(self, tmp_db):
        # Non-existent db should return empty
        assert list_tables(tmp_db) == []

    def test_lists_all_tables(self, tmp_db, sample_data):
        write_tables(tmp_db, sample_data)
        tables = list_tables(tmp_db)
        assert sorted(tables) == ["table_a", "table_b"]


class TestTableExists:
    def test_nonexistent_db(self, tmp_db):
        assert table_exists(tmp_db, "any") is False

    def test_existing_table(self, tmp_db, sample_data):
        write_tables(tmp_db, sample_data)
        assert table_exists(tmp_db, "table_a") is True

    def test_nonexistent_table(self, tmp_db, sample_data):
        write_tables(tmp_db, sample_data)
        assert table_exists(tmp_db, "missing") is False
