# coding:utf8
"""临时脚本: 直接执行 MaxCompute SQL 并导出 CSV。

示例:
  source .venv/bin/activate && python scripts/export_sql_to_csv.py \
    --sql-file workers/cr_analyze/sql/order_fact_whole.sql \
    --out /tmp/order_fact_whole.csv

  source .venv/bin/activate && python scripts/export_sql_to_csv.py \
        --query "SELECT 1 AS `示例列`" \
    --out /tmp/fact_sample.csv
"""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path

import pandas as pd

from automation.conf import maxcomputer as mc_conf
from automation.client import MaxComputerClient
from automation import hints as MC_HINTS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute SQL on MaxCompute and export to CSV"
    )
    parser.add_argument(
        "--sql-file",
        help="Path to .sql file (use this or --query)",
    )
    parser.add_argument(
        "--query",
        help="Raw SQL query string (use this or --sql-file)",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output CSV path",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="CSV encoding, default utf-8-sig (Excel-friendly for Chinese)",
    )
    parser.add_argument(
        "--no-hints",
        action="store_true",
        help="Disable default MaxCompute hints",
    )
    parser.add_argument(
        "--direct-read",
        action="store_true",
        help="Read query result directly via Instance Tunnel (default uses temp table mode)",
    )
    parser.add_argument(
        "--temp-table-project",
        help="Project for temp table. Default is MaxCompute prod project",
    )
    parser.add_argument(
        "--temp-table-schema",
        help="Schema for temp table (optional)",
    )
    return parser.parse_args()


def load_query(sql_file: str | None, query: str | None) -> str:
    if bool(sql_file) == bool(query):
        raise ValueError("Exactly one of --sql-file or --query must be provided")

    if sql_file:
        p = Path(sql_file)
        if not p.exists():
            raise FileNotFoundError(f"SQL file not found: {p}")
        text = p.read_text(encoding="utf-8").strip()
        if not text:
            raise ValueError(f"SQL file is empty: {p}")
        return text

    assert query is not None
    text = query.strip()
    if not text:
        raise ValueError("--query is empty")
    return text


def _generate_temp_table_name(prefix: str = "tmp_export") -> str:
    ts = int(time.time())
    return f"_{prefix}_{ts}"


def _normalize_sql_for_ctas(sql: str) -> str:
    return re.sub(r";\s*$", "", sql.strip())


def _read_temp_table_to_df(client: MaxComputerClient, table_name: str, project: str, schema: str | None) -> pd.DataFrame:
    odps_client = client._client
    records = list(
        odps_client.read_table(
            table_name,
            project=project,
            schema=schema,
        )
    )
    if not records:
        return pd.DataFrame()

    columns = [c.name for c in records[0]._columns]
    data = [[r[c] for c in columns] for r in records]
    return pd.DataFrame(data, columns=columns)


def _drop_temp_table(client: MaxComputerClient, full_table_name: str) -> None:
    try:
        drop_sql = f"DROP TABLE IF EXISTS {full_table_name};"
        inst = client.execute_sql(drop_sql)
        inst.wait_for_success()
    except Exception as e:
        print(f"WARN: failed to drop temp table {full_table_name}: {e}")


def execute_via_temp_table(
    client: MaxComputerClient,
    sql: str,
    hints: dict | None,
    project: str,
    schema: str | None,
) -> pd.DataFrame:
    temp_table_name = _generate_temp_table_name()
    name_parts = [project]
    if schema:
        name_parts.append(schema)
    name_parts.append(temp_table_name)
    full_table_name = ".".join(name_parts)

    clean_sql = _normalize_sql_for_ctas(sql)
    ctas_sql = (
        f"DROP TABLE IF EXISTS {full_table_name};\n"
        f"CREATE TABLE {full_table_name} LIFECYCLE 1 AS\n"
        f"{clean_sql};"
    )

    print(f"Using temp table mode: {full_table_name}")
    inst = client.execute_sql(ctas_sql, hints=hints)
    inst.wait_for_success()

    try:
        df = _read_temp_table_to_df(client, temp_table_name, project, schema)
        return df
    finally:
        _drop_temp_table(client, full_table_name)


def main() -> int:
    args = parse_args()

    sql = load_query(args.sql_file, args.query)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    mc_client = MaxComputerClient(
        access_id=mc_conf.get("prod", "access_id"),
        secret_access_key=mc_conf.get("prod", "secret_access_key"),
        project=mc_conf.get("prod", "project"),
        endpoint=mc_conf.get("prod", "endpoint"),
    )

    hints = None if args.no_hints else MC_HINTS
    temp_project = args.temp_table_project or mc_conf.get("prod", "project")
    temp_schema = args.temp_table_schema

    if args.direct_read:
        instance = mc_client.execute_sql(sql, hints=hints)
        instance.wait_for_success()
        with instance.open_reader(tunnel=True) as reader:
            df = reader.to_pandas()
    else:
        df = execute_via_temp_table(
            mc_client,
            sql,
            hints,
            temp_project,
            temp_schema,
        )

    df.to_csv(out_path, index=False, encoding=args.encoding)

    print(f"Exported rows={len(df)} cols={len(df.columns)} -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
