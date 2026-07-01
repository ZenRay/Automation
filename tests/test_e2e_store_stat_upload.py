# coding:utf8
"""tests/test_e2e_store_stat_upload.py -- 门店维度统计表端到端上传测试

完整流程：
  1. 初始化客户端
  2. 执行 store_stat_query.sql（临时表模式）
  3. 运行 normalize_store_stat_df
  4. 构建 TARGET_STORE_STAT，写入飞书
  5. 验证写入行数

运行：
    source .venv/bin/activate
    python tests/test_e2e_store_stat_upload.py
    # 或通过 pytest（需要 integration 标记）：
    python -m pytest tests/test_e2e_store_stat_upload.py -v -m integration
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from automation import hints as MC_HINTS
from automation.client import LarkMultiDimTable, MaxComputerClient
from automation.conf import lark as lark_conf, maxcomputer as mc_conf

import dataclasses
from datetime import timedelta

from workers.lib import (
    DateRangeParams,
    FieldTypeCoercer,
    execute_all_queries,
    write_to_all_targets,
)
from workers.lib.models import CleanupCondition
from workers.upgrade_after_sale.config import (
    SQL_BASE_DIR,
    SQL_QUERIES,
    TARGET_STORE_STAT,
)
from workers.upgrade_after_sale.transformer import normalize_store_stat_df

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("test_e2e_store_stat_upload")


def _build_store_stat_params(
    date_value: str | None = None, start: int = -7, end: int = 0
) -> DateRangeParams:
    """构建 store_stat 的日期参数"""
    ref_date = (
        pd.to_datetime(date_value).date() if date_value else pd.Timestamp.today().date()
    )
    return DateRangeParams(
        start_offset=start,
        end_offset=end,
        date_param=f"DATE '{ref_date.isoformat()}'",
        reference_date=ref_date,
    )


@pytest.mark.integration
def test_e2e_store_stat_upload():
    """端到端验证 store_stat 完整链路"""
    print("=" * 70)
    print("门店维度统计表 端到端上传测试")
    print("=" * 70)

    # Step 1: 初始化客户端
    print("\n[Step 1] 初始化客户端...")
    lark_client = LarkMultiDimTable(
        app_id=lark_conf.get("prod", "APP_ID"),
        app_secret=lark_conf.get("prod", "APP_SECRET"),
        lark_host=lark_conf.get("prod", "LARK_HOST", fallback="https://open.feishu.cn"),
    )
    mc_client = MaxComputerClient(
        access_id=mc_conf.get("prod", "access_id"),
        secret_access_key=mc_conf.get("prod", "secret_access_key"),
        project=mc_conf.get("prod", "project"),
        endpoint=mc_conf.get("prod", "endpoint"),
    )
    coercer = FieldTypeCoercer()
    print("  客户端初始化完成")

    # Step 2: 执行 store_stat SQL（临时表模式）
    print("\n[Step 2] 执行 store_stat_query.sql（临时表模式）...")
    store_stat_cfg = next(q for q in SQL_QUERIES if q.name == "store_stat")
    params = _build_store_stat_params()
    print(
        f"  date_param={params.date_param}, window=[{params.start_offset}, {params.end_offset}]"
    )

    mc_data = execute_all_queries(
        mc_client,
        [store_stat_cfg],
        SQL_BASE_DIR,
        hints=MC_HINTS,
        params=params.sql_params(),
    )
    store_stat_df = mc_data["store_stat"]
    print(
        f"  查询结果: {store_stat_df.shape[0]} rows, {store_stat_df.shape[1]} columns"
    )
    print(f"  列名: {list(store_stat_df.columns)}")

    assert not store_stat_df.empty, "store_stat 查询结果为空"
    assert store_stat_df.shape[1] == 48, f"期望 48 列，实际 {store_stat_df.shape[1]} 列"

    # Step 3: 标准化
    print("\n[Step 3] 运行 normalize_store_stat_df...")
    store_stat_df = normalize_store_stat_df(store_stat_df)
    assert pd.api.types.is_datetime64_any_dtype(
        store_stat_df["日期"]
    ), "日期列未正确转换"
    print(f"  标准化完成: {store_stat_df.shape}")

    # Step 4: 写入飞书
    print("\n[Step 4] 写入飞书目标表...")
    print(f"  目标表: {TARGET_STORE_STAT.table_name}")
    print(f"  字段映射: {len(TARGET_STORE_STAT.field_mappings)} 个字段")

    # 替换 runtime_window 哨兵为实际日期窗口
    ref_date = params.reference_date
    start_date = ref_date + timedelta(days=params.start_offset)
    end_date = ref_date + timedelta(days=params.end_offset)
    new_cleanup = CleanupCondition.date_window("日期", start_date, end_date)
    new_target = dataclasses.replace(TARGET_STORE_STAT, cleanup_conditions=new_cleanup)
    print(f"  清理窗口: 日期 in [{start_date}, {end_date})")

    write_to_all_targets(
        client=lark_client,
        result_df=store_stat_df,
        targets=[new_target],
        coercer=coercer,
        validation_level="warn",
    )
    print("  写入完成")

    # 汇总
    print("\n" + "=" * 70)
    print("测试完成!")
    print(f"  - 目标表: '{TARGET_STORE_STAT.table_name}'")
    print(f"  - 写入行数: {len(store_stat_df)}")
    print(f"  - 写入列数: {len(TARGET_STORE_STAT.field_mappings)}")
    print("=" * 70)


if __name__ == "__main__":
    test_e2e_store_stat_upload()
