# coding:utf8
"""tests/test_quality_reject_e2e.py -- quality_reject_tasks 端到端测试

使用真实的飞书数据测试整个流程：
1. 拉取 quality_reject_tasks
2. 执行 _aggregate_quality_reject 聚合
3. 输出结果分析

运行：
    source .venv/bin/activate
    python tests/test_quality_reject_e2e.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
import pytest

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).parent.parent))

from automation.conf import lark as lark_conf
from automation.client import LarkMultiDimTable
from workers.lib import extract_single_source
from workers.okr.config import LARK_SOURCES
from workers.okr.transformer import _aggregate_quality_reject, preprocess_lark_dates

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def find_source(name: str):
    """从 LARK_SOURCES 中查找指定名称的配置"""
    for source in LARK_SOURCES:
        if source.name == name:
            return source
    return None


@pytest.mark.integration
def test_quality_reject_e2e():
    """端到端测试：拉取 quality_reject_tasks 并聚合"""
    # 1. 初始化飞书客户端
    app_id = lark_conf.get("prod", "APP_ID")
    app_secret = lark_conf.get("prod", "APP_SECRET")
    lark_host = lark_conf.get("prod", "LARK_HOST", fallback="https://open.feishu.cn")

    logger.info("=" * 60)
    logger.info("Step 1: 初始化飞书客户端")
    client = LarkMultiDimTable(app_id=app_id, app_secret=app_secret, lark_host=lark_host)
    logger.info("飞书客户端初始化成功")

    # 2. 拉取 quality_reject_tasks
    source = find_source("quality_reject_tasks")
    if source is None:
        pytest.fail("未找到 quality_reject_tasks 配置")

    logger.info("=" * 60)
    logger.info("Step 2: 拉取 quality_reject_tasks 数据")
    df = extract_single_source(client, source)
    logger.info(f"拉取成功: {df.shape[0]} 行, {df.shape[1]} 列")

    # 3. 查看原始数据结构
    logger.info("=" * 60)
    logger.info("Step 3: 原始数据结构分析")
    logger.info(f"列名: {df.columns.tolist()}")
    logger.info(f"数据类型:\n{df.dtypes}")

    # 日期列类型检测
    date_col = "日期"
    if date_col in df.columns:
        is_timestamp = pd.api.types.is_datetime64_any_dtype(df[date_col])
        logger.info(f"日期列类型: {'pd.Timestamp' if is_timestamp else '其他'}")

        # 显示日期范围
        if is_timestamp:
            date_min = df[date_col].min()
            date_max = df[date_col].max()
            logger.info(f"日期范围: {date_min} ~ {date_max}")

    # 4. 执行日期预处理（模拟 execute_okr_merge 的预处理步骤）
    logger.info("=" * 60)
    logger.info("Step 4: 执行日期预处理 preprocess_lark_dates")
    lark_data = {"quality_reject_tasks": df}
    lark_data = preprocess_lark_dates(lark_data)
    df = lark_data["quality_reject_tasks"]
    logger.info(f"预处理后日期类型: {df['日期'].dtype}")
    logger.info(f"预处理后日期示例: {df['日期'].iloc[0]}")

    # 5. 执行聚合
    logger.info("=" * 60)
    logger.info("Step 5: 执行 _aggregate_quality_reject 聚合")
    result = _aggregate_quality_reject(df)
    logger.info(f"聚合结果: {result.shape[0]} 行, {result.shape[1]} 列")
    logger.info(f"输出列: {result.columns.tolist()}")

    # 6. 查看聚合结果
    logger.info("=" * 60)
    logger.info("Step 6: 聚合结果详情")
    pd.set_option("display.max_rows", 100)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", lambda x: f"{x:.0f}" if abs(x) > 1 else f"{x:.2f}")

    print("\n聚合结果:")
    print(result.to_string(index=False))

    # 7. 指标统计
    logger.info("=" * 60)
    logger.info("Step 7: 指标统计")
    if "打回次数" in result.columns:
        total_reject = result["打回次数"].sum()
        avg_reject = result["打回次数"].mean()
        logger.info(f"总打回次数: {total_reject}")
        logger.info(f"日均打回次数: {avg_reject:.1f}")

    if "7日重复打回商品数" in result.columns:
        total_repeat = result["7日重复打回商品数"].sum()
        max_repeat = result["7日重复打回商品数"].max()
        logger.info(f"7日重复打回商品总数: {total_repeat}")
        logger.info(f"最大单日重复打回商品数: {max_repeat}")

        # 统计有重复打回的日期
        dates_with_repeat = result[result["7日重复打回商品数"] > 0]["日期"]
        if len(dates_with_repeat) > 0:
            logger.info(f"存在重复打回的日期数: {len(dates_with_repeat)}")
        else:
            logger.info("无重复打回日期")

    logger.info("=" * 60)
    logger.info("测试完成")


if __name__ == "__main__":
    sys.exit(test_quality_reject_e2e())