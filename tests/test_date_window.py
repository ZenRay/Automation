# coding:utf8
"""日期窗口逻辑验证脚本

验证目标：
1. cleanup_window 与 data processing window 一致性
2. date_window filter 的 IS_GREATER/IS_LESS 边界准确性
3. runtime_window() 哨兵机制正确替换
4. 确认边界日期不会被误删（如之前的 June 1 问题）

测试表：https://bggc.feishu.cn/wiki/Il2TwHLZzi7BwwkErWKcsM4sngf?table=tblyPo1CcBj6NSDN&view=vewFdyNvhF
"""

import logging
import sys
import time

import pytest
from datetime import date, datetime, timedelta
from collections import Counter

from automation.conf import lark as lark_conf
from automation.client import LarkMultiDimTable
from workers.lib.models import (
    CleanupCondition, DateRangeParams, FilterOperator,
    LarkTargetConfig, FieldMapping, LarkFieldType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("test_date_window")

# ============================================================
# 配置
# ============================================================
TEST_URL = "https://bggc.feishu.cn/wiki/Il2TwHLZzi7BwwkErWKcsM4sngf?table=tblyPo1CcBj6NSDN&view=vewFdyNvhF"
TABLE_ID = "tblyPo1CcBj6NSDN"

# 测试场景：模拟 --date 2026-06-09 --start -7
# 预期 cleanup_window: (June 2, June 9)
# 写入数据范围: May 28 ~ June 12
# 预期保留: May 28 ~ June 1, June 10 ~ June 12
# 预期删除: June 2 ~ June 9
TEST_DATE = date(2026, 6, 9)
START_OFFSET = -7
END_OFFSET = 0

DATA_START = date(2026, 5, 28)
DATA_END = date(2026, 6, 12)


def init_client():
    app_id = lark_conf.get("prod", "APP_ID")
    app_secret = lark_conf.get("prod", "APP_SECRET")
    lark_host = lark_conf.get("prod", "LARK_HOST", fallback="https://open.feishu.cn")
    client = LarkMultiDimTable(app_id=app_id, app_secret=app_secret, lark_host=lark_host)
    client.extract_app_information(url=TEST_URL)
    client.extract_table_information(app_token=client.app_token)
    return client


def ensure_date_field(client):
    """确保表中有 '日期' 字段（Date 类型）"""
    fields = client.list_fields(table_id=TABLE_ID)
    for f in fields:
        if f["field_name"] == "日期" and f["type"] == 5:  # type 5 = DateTime
            logger.info("'日期' 字段已存在")
            return

    # 创建日期字段
    logger.info("创建 '日期' 字段...")
    client.create_field(
        field_def={
            "field_name": "日期",
            "type": 5,  # DateTime
            "property": {"date_formatter": "yyyy/MM/dd"},
        },
        table_id=TABLE_ID,
    )
    logger.info("'日期' 字段创建成功")


def write_test_data(client):
    """写入测试数据：May 28 ~ June 12 每天一条"""
    # 先清空表
    logger.info("清空测试表...")
    all_ids = []
    gen = client.request_records_generator(table_id=TABLE_ID, page_size=500)
    for resp in gen:
        if resp.get("code", -1) != 0:
            break
        items = resp.get("data", {}).get("items", [])
        all_ids.extend([item["record_id"] for item in items if "record_id" in item])

    if all_ids:
        for i in range(0, len(all_ids), 100):
            batch = all_ids[i:i + 100]
            client.delete_batch_records(batch, table_id=TABLE_ID)
            time.sleep(1)
        logger.info(f"  已删除 {len(all_ids)} 条旧记录")

    # 写入新数据
    records = []
    current = DATA_START
    while current <= DATA_END:
        ts_ms = int(datetime.combine(current, datetime.min.time()).timestamp() * 1000)
        # 飞书日期字段接受毫秒时间戳
        import calendar
        ts_utc = calendar.timegm(current.timetuple()) * 1000
        records.append({
            "fields": {
                "文本": f"test-{current.isoformat()}",
                "日期": ts_utc,
            }
        })
        current += timedelta(days=1)

    logger.info(f"写入 {len(records)} 条测试数据 ({DATA_START} ~ {DATA_END})...")
    for i in range(0, len(records), 100):
        batch = records[i:i + 100]
        client.add_batch_records(batch, table_id=TABLE_ID)
        time.sleep(1)
    logger.info("  写入完成")


def read_table_dates(client):
    """读取表中所有记录的日期，返回 Counter"""
    all_items = []
    gen = client.request_records_generator(table_id=TABLE_ID, page_size=500)
    for resp in gen:
        if resp.get("code", -1) != 0:
            break
        items = resp.get("data", {}).get("items", [])
        all_items.extend(items)

    date_counts = Counter()
    for item in all_items:
        fields = item.get("fields", {})
        val = fields.get("日期")
        if val is not None:
            if isinstance(val, (int, float)):
                ts = val / 1000 if val > 9999999999 else val
                d = datetime.utcfromtimestamp(ts).date()
            else:
                d = None
            date_counts[d] += 1
    return date_counts, len(all_items)


def test_date_window_filter():
    """验证 date_window filter 的边界正确性"""
    logger.info("=" * 60)
    logger.info("TEST 1: date_window filter 边界验证")
    logger.info("=" * 60)

    # cleanup_window = (June 2, June 9)
    dr = DateRangeParams(
        start_offset=START_OFFSET,
        end_offset=END_OFFSET,
        date_param=f"DATE '{TEST_DATE}'",
        reference_date=TEST_DATE,
    )
    cleanup_start, cleanup_end = dr.cleanup_window
    logger.info(f"cleanup_window: ({cleanup_start}, {cleanup_end})")
    assert cleanup_start == date(2026, 6, 2), f"Expected June 2, got {cleanup_start}"
    assert cleanup_end == date(2026, 6, 9), f"Expected June 9, got {cleanup_end}"
    logger.info("  ✓ cleanup_window 计算正确")

    # 生成 filter
    cc = CleanupCondition.date_window("日期", cleanup_start, cleanup_end)
    lf = cc.to_lark_filter()

    lower_ts = int(lf["conditions"][0]["value"][1])
    upper_ts = int(lf["conditions"][1]["value"][1])
    lower_dt = datetime.utcfromtimestamp(lower_ts / 1000)
    upper_dt = datetime.utcfromtimestamp(upper_ts / 1000)

    logger.info(f"Filter conditions:")
    logger.info(f"  IS_GREATER: ts={lower_ts} → UTC {lower_dt}")
    logger.info(f"  IS_LESS:    ts={upper_ts} → UTC {upper_dt}")

    # 验证：修复后 lower 应该是 June 2（不是 June 1）
    assert lower_dt.date() == date(2026, 6, 2), \
        f"IS_GREATER 应该是 June 2, 实际是 {lower_dt.date()}"
    assert upper_dt.date() == date(2026, 6, 10), \
        f"IS_LESS 应该是 June 10, 实际是 {upper_dt.date()}"
    logger.info("  ✓ filter 边界正确：IS_GREATER June 2, IS_LESS June 10")


def test_runtime_window_sentinel():
    """验证 runtime_window 哨兵机制"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("TEST 2: runtime_window() 哨兵机制验证")
    logger.info("=" * 60)

    # 1. 哨兵创建
    sentinel = CleanupCondition.runtime_window()
    assert sentinel.is_runtime is True
    assert sentinel.conditions == []
    logger.info("  ✓ runtime_window() 哨兵创建正确")

    # 2. 哨兵未被替换时调用 to_lark_filter 应抛出异常
    try:
        sentinel.to_lark_filter()
        pytest.fail("应该抛出 RuntimeError！")
    except RuntimeError as e:
        logger.info(f"  ✓ 未替换哨兵正确抛出异常: {str(e)[:60]}...")

    # 3. 模拟 _apply_date_range_to_routes 的替换逻辑
    from workers.okr.main import _apply_date_range_to_routes
    from workers.lib.models import DataRoute

    dr = DateRangeParams(
        start_offset=START_OFFSET,
        end_offset=END_OFFSET,
        date_param=f"DATE '{TEST_DATE}'",
        reference_date=TEST_DATE,
    )

    # 创建 mock route
    mock_target = LarkTargetConfig(
        name="test", url=TEST_URL, table_name="数据表",
        field_mappings=[],
        cleanup_conditions=CleanupCondition.runtime_window(),
    )
    mock_route = DataRoute(
        name="test_route",
        target=mock_target,
    )

    # 执行替换
    result = _apply_date_range_to_routes([mock_route], dr)
    replaced = result[0].target.cleanup_conditions

    assert replaced.is_runtime is False, "替换后不应是 runtime"
    assert len(replaced.conditions) == 2, "替换后应有 2 个条件"
    logger.info(f"  ✓ 哨兵被正确替换为 date_window")

    # 4. 替换后的 filter 应可正常生成
    lf = replaced.to_lark_filter()
    assert len(lf["conditions"]) == 2
    logger.info(f"  ✓ 替换后的 filter 正常生成: {lf['conditions'][0]['operator']} / {lf['conditions'][1]['operator']}")


@pytest.mark.integration
def test_cleanup_execution(client):
    """实际执行 cleanup 并验证结果"""
    logger.info("")
    logger.info("=" * 60)
    logger.info("TEST 3: 实际 cleanup 执行验证")
    logger.info("=" * 60)

    # 读取清理前的数据
    counts_before, total_before = read_table_dates(client)
    logger.info(f"清理前: {total_before} 条")
    for d in sorted(counts_before.keys()):
        logger.info(f"  {d}: {counts_before[d]} 条")

    # 生成 cleanup 参数
    dr = DateRangeParams(
        start_offset=START_OFFSET,
        end_offset=END_OFFSET,
        date_param=f"DATE '{TEST_DATE}'",
        reference_date=TEST_DATE,
    )
    cleanup_start, cleanup_end = dr.cleanup_window

    # 构造 target config
    target = LarkTargetConfig(
        name="test_cleanup",
        url=TEST_URL,
        table_name="数据表",
        field_mappings=[
            FieldMapping("文本", "文本", None),
            FieldMapping("日期", "日期", LarkFieldType.DATE),
        ],
        cleanup_conditions=CleanupCondition.runtime_window(),
    )

    # 模拟 _apply_date_range_to_routes 替换
    new_cleanup = CleanupCondition.date_window("日期", cleanup_start, cleanup_end)
    import dataclasses
    target = dataclasses.replace(target, cleanup_conditions=new_cleanup)

    # 执行 cleanup
    from workers.lib.lark_loader import cleanup_target_table
    logger.info(f"\n执行 cleanup: window=({cleanup_start}, {cleanup_end})")
    deleted = cleanup_target_table(client, target, TABLE_ID)
    logger.info(f"  删除了 {deleted} 条记录")

    # 读取清理后的数据
    time.sleep(2)  # 等待飞书 API 一致
    counts_after, total_after = read_table_dates(client)
    logger.info(f"\n清理后: {total_after} 条")
    for d in sorted(counts_after.keys()):
        in_window = cleanup_start <= d <= cleanup_end
        marker = " ← 窗口内（应已删除）" if in_window else " ← 窗口外（应保留）"
        logger.info(f"  {d}: {counts_after[d]} 条{marker}")

    # 验证
    expected_deleted_dates = set()
    d = cleanup_start
    while d <= cleanup_end:
        expected_deleted_dates.add(d)
        d += timedelta(days=1)

    expected_kept_dates = set()
    d = DATA_START
    while d <= DATA_END:
        if d not in expected_deleted_dates:
            expected_kept_dates.add(d)
        d += timedelta(days=1)

    # 检查窗口内日期是否全部删除
    deleted_ok = all(counts_after.get(d, 0) == 0 for d in expected_deleted_dates)
    # 检查窗口外日期是否全部保留
    kept_ok = all(counts_after.get(d, 0) > 0 for d in expected_kept_dates)

    logger.info("")
    if deleted_ok and kept_ok:
        logger.info("✓ 验证通过！")
        logger.info(f"  - 窗口内 {len(expected_deleted_dates)} 天全部删除 ✓")
        logger.info(f"  - 窗口外 {len(expected_kept_dates)} 天全部保留 ✓")

        # 特别检查之前的 bug 场景：June 1 是否保留
        june1 = date(2026, 6, 1)
        if counts_after.get(june1, 0) > 0:
            logger.info(f"  - June 1 保留 ✓（之前的 bug 已修复）")
        else:
            logger.error(f"  - June 1 丢失 ✗（bug 仍存在！）")
            return False
    else:
        logger.error("✗ 验证失败！")
        if not deleted_ok:
            for d in sorted(expected_deleted_dates):
                if counts_after.get(d, 0) > 0:
                    logger.error(f"  - {d} 应删除但存在 ✗")
        if not kept_ok:
            for d in sorted(expected_kept_dates):
                if counts_after.get(d, 0) == 0:
                    logger.error(f"  - {d} 应保留但丢失 ✗")
        return False

    return True


def main():
    logger.info("=" * 60)
    logger.info("日期窗口逻辑验证")
    logger.info(f"测试参数: --date {TEST_DATE} --start {START_OFFSET} --end {END_OFFSET}")
    logger.info(f"预期 cleanup_window: ({TEST_DATE + timedelta(days=START_OFFSET)}, {TEST_DATE})")
    logger.info(f"数据范围: {DATA_START} ~ {DATA_END}")
    logger.info("=" * 60)

    results = {}

    # TEST 1: filter 边界验证（纯逻辑，不需要网络）
    results["filter_boundary"] = test_date_window_filter()

    # TEST 2: runtime_window 哨兵机制（纯逻辑）
    results["sentinel"] = test_runtime_window_sentinel()

    # TEST 3: 实际 cleanup 执行
    client = init_client()
    ensure_date_field(client)
    write_test_data(client)
    results["cleanup_execution"] = test_cleanup_execution(client)

    # 汇总
    logger.info("")
    logger.info("=" * 60)
    logger.info("验证汇总")
    logger.info("=" * 60)
    all_pass = True
    for name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"  {name}: {status}")
        if not passed:
            all_pass = False

    if all_pass:
        logger.info("\n✓ 所有验证通过！日期窗口逻辑已正确修复。")
    else:
        logger.error("\n✗ 存在失败的验证项，请检查。")

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())

