# coding:utf8
"""workers.cr_trail_pricing.main -- CLI 入口 & 流水线编排"""

import argparse
import dataclasses
import logging
import time
from datetime import date

from .config import LARK_SOURCES
from .transformer import (
    extract_sources,
    filter_trial_products,
    mark_trial_regions,
    associate_commission,
    associate_logistics_fee,
    compute_pricing,
    export_excel,
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s]:%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


def _init_lark_client():
    from automation.conf import lark as lark_conf
    from automation.client import LarkMultiDimTable

    app_id = lark_conf.get("prod", "APP_ID")
    app_secret = lark_conf.get("prod", "APP_SECRET")
    lark_host = lark_conf.get("prod", "LARK_HOST", fallback="https://open.feishu.cn")
    logger.info(
        "Initializing LarkMultiDimTable client (app_id=%s, host=%s)", app_id, lark_host
    )
    return LarkMultiDimTable(
        app_id=app_id,
        app_secret=app_secret,
        lark_host=lark_host,
    )


def _apply_target_date(sources, target_date):
    """为需要精确日期匹配的源注入 start/end date

    特殊处理：conf_商品信息 筛选昨日数据 (target_date - 1)，
    其余日期过滤源使用 target_date。
    """
    from datetime import timedelta

    yesterday = target_date - timedelta(days=1)
    result = []
    for src in sources:
        if src.date_filter_field:
            # conf_goods 筛选昨日数据，其余筛选今日数据
            effective_date = yesterday if src.name == "conf_goods" else target_date
            result.append(
                dataclasses.replace(
                    src,
                    date_filter_start_date=effective_date,
                    date_filter_end_date=effective_date,
                )
            )
        else:
            result.append(src)
    return result


def run_cr_trail_pricing_pipeline(target_date: date, output_path: str) -> int:
    logger.info("=" * 60)
    logger.info("试验区域抽佣调价方案生成器 — target_date=%s", target_date)
    logger.info("=" * 60)

    try:
        lark_client = _init_lark_client()
    except Exception as e:
        logger.error("Failed to init Lark client: %s", e)
        return 1

    sources = _apply_target_date(LARK_SOURCES, target_date)

    stages = [
        ("数据提取", lambda: extract_sources(lark_client, sources)),
        (
            "商品筛选",
            lambda: filter_trial_products(
                lark_data["conf_goods"], lark_data["conf_trial_goods"], target_date
            ),
        ),
        (
            "区域标记",
            lambda: mark_trial_regions(
                lark_data["conf_county"], lark_data["conf_trial_group"], target_date
            ),
        ),
        (
            "抽佣关联",
            lambda: associate_commission(
                regions_df, lark_data["conf_trial_commission"], target_date
            ),
        ),
        (
            "隐形物流费",
            lambda: associate_logistics_fee(
                regions_df, lark_data["conf_hidden_logistics"], target_date
            ),
        ),
        ("笛卡尔积计算", lambda: compute_pricing(products_df, regions_df)),
        ("Excel 输出", lambda: export_excel(result_df, output_path)),
    ]

    lark_data = {}
    products_df = None
    regions_df = None
    result_df = None

    for i, (name, fn) in enumerate(stages, 1):
        t0 = time.time()
        try:
            output = fn()
        except Exception as e:
            logger.error("[Stage %d/7] %s FAILED: %s", i, name, e)
            return 1
        elapsed = time.time() - t0

        if i == 1:
            lark_data = output
            rows = sum(len(v) for v in lark_data.values())
        elif i == 2:
            products_df = output
            rows = len(products_df)
        elif i == 3:
            regions_df = output
            rows = len(regions_df)
        elif i in (4, 5):
            regions_df = output
            rows = len(regions_df)
        elif i == 6:
            result_df = output
            rows = len(result_df)
        else:
            rows = len(output) if hasattr(output, "__len__") else 0

        logger.info("[Stage %d/7] %s: %d rows, %.2fs", i, name, rows, elapsed)

    logger.info("Pipeline complete: %s", output_path)
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="试验区域抽佣调价方案生成器",
    )
    parser.add_argument(
        "--date",
        default=str(date.today()),
        help="目标日期 (YYYY-MM-DD, 默认 today)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="输出 Excel 路径 (默认 cr_trail_pricing_{date}.xlsx)",
    )
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date)
    output_path = args.output or f"cr_trail_pricing_{args.date}.xlsx"

    return run_cr_trail_pricing_pipeline(target_date, output_path)


if __name__ == "__main__":
    raise SystemExit(main())
