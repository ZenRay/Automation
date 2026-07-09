# coding:utf8
"""tests/upgrade_after_sale/test_e2e_after_sale_upload.py -- 售后明细表端到端上传测试

完整流程：
  1. 初始化客户端
  2. 执行 after_sale_item_query.sql（临时表模式）
  3. 构建 TARGET_AFTER_SALE，写入飞书
  4. 验证写入行数

运行：
    source .venv/bin/activate
    python -m pytest tests/upgrade_after_sale/test_e2e_after_sale_upload.py -v -m integration
"""

from __future__ import annotations

import dataclasses
import logging
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

from automation import hints as MC_HINTS
from automation.client import LarkMultiDimTable, MaxComputerClient
from automation.conf import lark as lark_conf, maxcomputer as mc_conf

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
    TARGET_AFTER_SALE,
)
from workers.upgrade_after_sale.main import _apply_attachment_bak_columns

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("test_e2e_after_sale_upload")


def _build_after_sale_params(
    date_value: str | None = None, start: int = -7, end: int = 0
) -> DateRangeParams:
    """构建 after_sale_item 的日期参数"""
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
def test_e2e_after_sale_upload():
    """端到端验证 after_sale_detail 完整链路"""
    print("=" * 70)
    print("售后明细表 端到端上传测试")
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

    # Step 2: 执行 after_sale_item SQL（临时表模式）
    print("\n[Step 2] 执行 after_sale_item_query.sql（临时表模式）...")
    after_sale_cfg = next(q for q in SQL_QUERIES if q.name == "after_sale_item")
    params = _build_after_sale_params()
    print(
        f"  date_param={params.date_param}, window=[{params.start_offset}, {params.end_offset}]"
    )

    mc_data = execute_all_queries(
        mc_client,
        [after_sale_cfg],
        SQL_BASE_DIR,
        hints=MC_HINTS,
        params=params.sql_params(),
    )
    after_sale_df = mc_data["after_sale_item"]
    print(
        f"  查询结果: {after_sale_df.shape[0]} rows, {after_sale_df.shape[1]} columns"
    )
    print(f"  列名: {list(after_sale_df.columns)}")

    assert not after_sale_df.empty, "after_sale_item 查询结果为空"

    # Step 2.5: 添加附件备份列
    after_sale_df = _apply_attachment_bak_columns(after_sale_df)
    print(f"  附件备份列已添加，当前列数: {after_sale_df.shape[1]}")

    # Step 3: 写入飞书
    print("\n[Step 3] 写入飞书目标表...")
    print(f"  目标表: {TARGET_AFTER_SALE.table_name}")
    print(f"  字段映射: {len(TARGET_AFTER_SALE.field_mappings)} 个字段")

    # 替换 runtime_window 哨兵为实际日期窗口
    ref_date = params.reference_date
    start_date = ref_date + timedelta(days=params.start_offset)
    end_date = ref_date + timedelta(days=params.end_offset)
    new_cleanup = CleanupCondition.date_window("申请日期", start_date, end_date)
    new_target = dataclasses.replace(
        TARGET_AFTER_SALE, cleanup_conditions=new_cleanup
    )
    print(f"  清理窗口: 申请日期 in [{start_date}, {end_date})")

    write_to_all_targets(
        client=lark_client,
        result_df=after_sale_df,
        targets=[new_target],
        coercer=coercer,
        validation_level="warn",
    )
    print("  写入完成")

    # 汇总
    print("\n" + "=" * 70)
    print("测试完成!")
    print(f"  - 目标表: '{TARGET_AFTER_SALE.table_name}'")
    print(f"  - 写入行数: {len(after_sale_df)}")
    print(f"  - 写入列数: {len(TARGET_AFTER_SALE.field_mappings)}")
    print("=" * 70)


if __name__ == "__main__":
    test_e2e_after_sale_upload()
