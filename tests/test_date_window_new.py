# coding:utf8
"""日期窗口全链路验证脚本（新版）

测试表：https://bggc.feishu.cn/wiki/FbSPwno6ki9xHBkiDlQc2Csvnoh?table=tblJDpt1JTQtuSML&view=vewNSdPMQW

验证目标：
1. SQL 三参数 DateRangeParams 边界计算（纯逻辑）
2. runtime_window() 哨兵替换机制（纯逻辑）
3. Lark 源拉取窗口：默认窗口、lark_extra_start_days、lark_extra_end_days（需网络）
4. cleanup 双边窗口执行准确性（需网络）
"""

import calendar
import dataclasses
import logging
import sys
import time

import pytest
from collections import Counter
from datetime import date, datetime, timedelta

from automation.conf import lark as lark_conf
from automation.client import LarkMultiDimTable
from workers.lib.models import (
    CleanupCondition,
    DateRangeParams,
    FieldMapping,
    LarkFieldType,
    LarkSourceConfig,
    LarkTargetConfig,
)
from workers.lib.lark_extractor import extract_single_source

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s]: %(message)s",
)
logger = logging.getLogger("test_date_window_new")

# ============================================================
# 测试表配置
# ============================================================
TEST_URL = (
    "https://bggc.feishu.cn/wiki/FbSPwno6ki9xHBkiDlQc2Csvnoh"
    "?table=tblJDpt1JTQtuSML&view=vewNSdPMQW"
)
TABLE_NAME = "数据表"   # 飞书多维表格内的表名
TABLE_ID = "tblJDpt1JTQtuSML"

# 固定基准日（便于可重现）
REFERENCE_DATE = date(2026, 6, 9)
START_OFFSET = -7
END_OFFSET = 0

# 测试数据范围：T-14 到 T+7（覆盖所有偏移场景）
DATA_START = REFERENCE_DATE + timedelta(days=-14)
DATA_END = REFERENCE_DATE + timedelta(days=7)


# ============================================================
# 工具函数
# ============================================================

def init_client() -> LarkMultiDimTable:
    app_id = lark_conf.get("prod", "APP_ID")
    app_secret = lark_conf.get("prod", "APP_SECRET")
    lark_host = lark_conf.get("prod", "LARK_HOST", fallback="https://open.feishu.cn")
    client = LarkMultiDimTable(app_id=app_id, app_secret=app_secret, lark_host=lark_host)
    return client


def _ts_utc(d: date) -> int:
    """date -> 飞书所需毫秒时间戳（UTC 零点）"""
    return calendar.timegm(d.timetuple()) * 1000


def write_test_data(client: LarkMultiDimTable) -> int:
    """确保表中有日期字段，清空测试表并写入 DATA_START ~ DATA_END 每天一条记录，返回写入条数"""
    # --- 确保日期字段存在 ---
    fields = client.list_fields(table_id=TABLE_ID)
    field_names_existing = [f["field_name"] for f in fields]
    if "日期" not in field_names_existing:
        logger.info("创建 '日期' 字段...")
        client.create_field(
            field_def={"field_name": "日期", "type": 5,
                       "property": {"date_formatter": "yyyy/MM/dd"}},
            table_id=TABLE_ID,
        )
        logger.info("  '日期' 字段创建成功")
        time.sleep(1)
    # --- 清空 ---
    logger.info("清空测试表...")
    all_ids = []
    gen = client.request_records_generator(table_id=TABLE_ID, page_size=500)
    for resp in gen:
        if resp.get("code", -1) != 0:
            break
        items = resp.get("data", {}).get("items") or []
        all_ids.extend(item["record_id"] for item in items if "record_id" in item)
    if all_ids:
        for i in range(0, len(all_ids), 100):
            client.delete_batch_records(all_ids[i:i + 100], table_id=TABLE_ID)
            time.sleep(0.5)
        logger.info(f"  已删除 {len(all_ids)} 条旧记录")

    # --- 写入 ---
    records = []
    cur = DATA_START
    while cur <= DATA_END:
        records.append({
            "fields": {
                "文本": f"test-{cur.isoformat()}",
                "日期": _ts_utc(cur),
            }
        })
        cur += timedelta(days=1)

    logger.info(f"写入 {len(records)} 条测试数据（{DATA_START} ~ {DATA_END}）...")
    for i in range(0, len(records), 100):
        client.add_batch_records(records[i:i + 100], table_id=TABLE_ID)
        time.sleep(0.5)
    logger.info("  写入完成")
    return len(records)


def read_table_dates(client: LarkMultiDimTable) -> tuple[Counter, int]:
    """读取表中所有记录的日期，返回 (Counter, 总条数)"""
    all_items = []
    gen = client.request_records_generator(table_id=TABLE_ID, page_size=500)
    for resp in gen:
        if resp.get("code", -1) != 0:
            break
        all_items.extend(resp.get("data", {}).get("items") or [])

    date_counts: Counter = Counter()
    for item in all_items:
        val = item.get("fields", {}).get("日期")
        if val is not None and isinstance(val, (int, float)):
            ts = val / 1000 if val > 9_999_999_999 else val
            d = datetime.utcfromtimestamp(ts).date()
            date_counts[d] += 1
    return date_counts, len(all_items)


# ============================================================
# TEST 1: SQL DateRangeParams 边界（纯逻辑）
# ============================================================

def test_sql_date_params() -> bool:
    logger.info("=" * 60)
    logger.info("TEST 1: DateRangeParams SQL 参数生成验证")
    logger.info("=" * 60)

    dr = DateRangeParams(
        start_offset=START_OFFSET,
        end_offset=END_OFFSET,
        date_param=f"DATE '{REFERENCE_DATE}'",
        reference_date=REFERENCE_DATE,
    )

    params = dr.sql_params()
    assert params["date_param"] == f"DATE '{REFERENCE_DATE}'", f"date_param 异常: {params}"
    assert params["start_offset"] == str(START_OFFSET), f"start_offset 异常: {params}"
    assert params["end_offset"] == str(END_OFFSET), f"end_offset 异常: {params}"
    logger.info(f"  sql_params() = {params}")
    logger.info("  ✓ SQL 三参数生成正确")

    cleanup_start, cleanup_end = dr.cleanup_window
    assert cleanup_start == REFERENCE_DATE + timedelta(days=START_OFFSET)
    assert cleanup_end == REFERENCE_DATE + timedelta(days=END_OFFSET)
    logger.info(f"  cleanup_window = ({cleanup_start}, {cleanup_end})")
    logger.info("  ✓ cleanup_window 计算正确")

    # 默认值容错
    dr_default = DateRangeParams()
    p_default = dr_default.sql_params()
    assert p_default["date_param"] == "CURRENT_DATE()"
    assert p_default["start_offset"] == "-7"
    assert p_default["end_offset"] == "0"
    logger.info(f"  默认参数: {p_default}")
    logger.info("  ✓ 默认参数容错正常")


# ============================================================
# TEST 2: runtime_window() 哨兵（纯逻辑）
# ============================================================

def test_runtime_window_sentinel() -> bool:
    logger.info("")
    logger.info("=" * 60)
    logger.info("TEST 2: runtime_window() 哨兵机制验证")
    logger.info("=" * 60)

    sentinel = CleanupCondition.runtime_window()
    assert sentinel.is_runtime is True and sentinel.conditions == []
    logger.info("  ✓ 哨兵创建正确")

    # 未替换时调用 to_lark_filter 应抛出异常
    try:
        sentinel.to_lark_filter()
        pytest.fail("应该抛出 RuntimeError！")
    except RuntimeError as e:
        logger.info(f"  ✓ 未替换哨兵正确抛出: {str(e)[:60]}...")

    # 模拟 _apply_date_range_to_routes 替换
    from workers.okr.main import _apply_date_range_to_routes
    from workers.lib.models import DataRoute

    dr = DateRangeParams(
        start_offset=START_OFFSET,
        end_offset=END_OFFSET,
        date_param=f"DATE '{REFERENCE_DATE}'",
        reference_date=REFERENCE_DATE,
    )
    mock_target = LarkTargetConfig(
        name="test", url=TEST_URL, table_name=TABLE_NAME,
        field_mappings=[],
        cleanup_conditions=CleanupCondition.runtime_window(),
    )
    mock_route = DataRoute(name="test_route", target=mock_target)
    result = _apply_date_range_to_routes([mock_route], dr)
    replaced = result[0].target.cleanup_conditions

    assert replaced.is_runtime is False
    assert len(replaced.conditions) == 2
    lf = replaced.to_lark_filter()
    assert len(lf["conditions"]) == 2
    logger.info(f"  ✓ 哨兵正确替换为 date_window: "
                f"{lf['conditions'][0]['operator']} / {lf['conditions'][1]['operator']}")


# ============================================================
# TEST 3: Lark 源拉取窗口偏移（需网络）
# ============================================================

@pytest.mark.integration
def test_lark_source_date_filter(client: LarkMultiDimTable) -> bool:
    logger.info("")
    logger.info("=" * 60)
    logger.info("TEST 3: Lark 源拉取窗口偏移验证")
    logger.info("=" * 60)

    from workers.okr.main import _apply_date_range_to_lark_sources

    dr = DateRangeParams(
        start_offset=START_OFFSET,
        end_offset=END_OFFSET,
        date_param=f"DATE '{REFERENCE_DATE}'",
        reference_date=REFERENCE_DATE,
    )

    # 构建基础 source（不含额外偏移）
    base_source = LarkSourceConfig(
        name="test_lark_source",
        url=TEST_URL,
        table_name=TABLE_NAME,
        field_names=["文本", "日期"],
        date_filter_field="日期",
        date_filter_days=15,   # config 中的静态默认值，会被运行时覆盖
    )

    # ---- 场景 A: 默认（无额外偏移）----
    logger.info("\n--- 场景 A: 默认无额外偏移 ---")
    effective_sources = _apply_date_range_to_lark_sources([base_source], dr)
    src_a = effective_sources[0]
    expected_start_a = REFERENCE_DATE + timedelta(days=START_OFFSET)   # T-7 = 2026-06-02
    assert src_a.date_filter_start_date == expected_start_a, \
        f"期望 start_date={expected_start_a}，实际={src_a.date_filter_start_date}"
    assert src_a.date_filter_end_date is None
    df_a = extract_single_source(client, src_a)
    dates_a = set(df_a["日期"].dt.date) if not df_a.empty else set()

    expected_end_a = REFERENCE_DATE + timedelta(days=END_OFFSET)       # T

    # 验证：T-7 到 T 的日期都应该在结果中
    for d in [expected_start_a, expected_end_a]:
        assert d in dates_a, f"场景A: {d} 应在结果中，实际日期集={sorted(dates_a)}"
    # 验证：T-8（超出窗口）不应在结果中
    too_early = expected_start_a - timedelta(days=1)
    assert too_early not in dates_a, \
        f"场景A: {too_early} 不应在结果中（超出向前边界）"
    logger.info(f"  start_date={src_a.date_filter_start_date}, end_date=None")
    logger.info(f"  结果日期范围: {min(dates_a)} ~ {max(dates_a)} ({len(df_a)} 行)")
    logger.info(f"  ✓ 场景A: 默认窗口 [{expected_start_a}, {expected_end_a}] 拉取正确")

    # ---- 场景 B: lark_extra_start_days=3（向前多拉3天）----
    logger.info("\n--- 场景 B: lark_extra_start_days=3 ---")
    src_b_config = dataclasses.replace(base_source, lark_extra_start_days=3)
    effective_b = _apply_date_range_to_lark_sources([src_b_config], dr)
    src_b = effective_b[0]
    expected_start_b = REFERENCE_DATE + timedelta(days=START_OFFSET - 3)  # T-10
    assert src_b.date_filter_start_date == expected_start_b, \
        f"期望 start_date={expected_start_b}，实际={src_b.date_filter_start_date}"
    assert src_b.date_filter_end_date is None

    df_b = extract_single_source(client, src_b)
    dates_b = set(df_b["日期"].dt.date) if not df_b.empty else set()

    for d in [expected_start_b, expected_end_a]:
        assert d in dates_b, f"场景B: {d} 应在结果中，实际日期集={sorted(dates_b)}"
    assert expected_start_b - timedelta(days=1) not in dates_b, \
        f"场景B: {expected_start_b - timedelta(days=1)} 不应在结果中"
    logger.info(f"  start_date={src_b.date_filter_start_date}, end_date=None")
    logger.info(f"  结果日期范围: {min(dates_b)} ~ {max(dates_b)} ({len(df_b)} 行)")
    logger.info(f"  ✓ 场景B: 向前偏移3天 [{expected_start_b}, {expected_end_a}] 拉取正确")

    # ---- 场景 C: lark_extra_end_days=3（向后多拉3天）----
    logger.info("\n--- 场景 C: lark_extra_end_days=3 ---")
    src_c_config = dataclasses.replace(base_source, lark_extra_end_days=3)
    effective_c = _apply_date_range_to_lark_sources([src_c_config], dr)
    src_c = effective_c[0]
    assert src_c.date_filter_start_date == expected_start_a   # 向后偏移不影响下界
    expected_end_c = REFERENCE_DATE + timedelta(days=END_OFFSET + 3)  # T+3
    assert src_c.date_filter_end_date == expected_end_c, \
        f"期望 end_date={expected_end_c}，实际={src_c.date_filter_end_date}"

    df_c = extract_single_source(client, src_c)
    dates_c = set(df_c["日期"].dt.date) if not df_c.empty else set()

    for d in [expected_start_a, expected_end_c]:
        assert d in dates_c, f"场景C: {d} 应在结果中，实际日期集={sorted(dates_c)}"
    beyond_end = expected_end_c + timedelta(days=1)
    assert beyond_end not in dates_c, \
        f"场景C: {beyond_end} 不应在结果中（超出向后边界）"
    logger.info(f"  start_date={src_c.date_filter_start_date}, end_date={src_c.date_filter_end_date}")
    logger.info(f"  结果日期范围: {min(dates_c)} ~ {max(dates_c)} ({len(df_c)} 行)")
    logger.info(f"  ✓ 场景C: 向后偏移3天 [{expected_start_a}, {expected_end_c}] 拉取正确")

    # ---- 场景 D: 同时有前向+后向偏移 ----
    logger.info("\n--- 场景 D: lark_extra_start_days=3 + lark_extra_end_days=3 ---")
    src_d_config = dataclasses.replace(base_source, lark_extra_start_days=3, lark_extra_end_days=3)
    effective_d = _apply_date_range_to_lark_sources([src_d_config], dr)
    src_d = effective_d[0]
    assert src_d.date_filter_start_date == expected_start_b   # T-10
    assert src_d.date_filter_end_date == expected_end_c       # T+3

    df_d = extract_single_source(client, src_d)
    dates_d = set(df_d["日期"].dt.date) if not df_d.empty else set()

    for d in [expected_start_b, expected_end_c]:
        assert d in dates_d, f"场景D: {d} 应在结果中"
    assert expected_start_b - timedelta(days=1) not in dates_d, \
        f"场景D: {expected_start_b - timedelta(days=1)} 不应在结果中"
    assert beyond_end not in dates_d, \
        f"场景D: {beyond_end} 不应在结果中"
    logger.info(f"  start_date={src_d.date_filter_start_date}, end_date={src_d.date_filter_end_date}")
    logger.info(f"  结果日期范围: {min(dates_d)} ~ {max(dates_d)} ({len(df_d)} 行)")
    logger.info(f"  ✓ 场景D: 同时偏移 [{expected_start_b}, {expected_end_c}] 拉取正确")

    return True


# ============================================================
# TEST 4: cleanup 双边窗口执行（需网络）
# ============================================================

@pytest.mark.integration
def test_cleanup_execution(client: LarkMultiDimTable) -> bool:
    logger.info("")
    logger.info("=" * 60)
    logger.info("TEST 4: cleanup 双边窗口执行验证")
    logger.info("=" * 60)

    # --- 重新写入完整测试数据 ---
    write_test_data(client)
    time.sleep(2)

    dr = DateRangeParams(
        start_offset=START_OFFSET,
        end_offset=END_OFFSET,
        date_param=f"DATE '{REFERENCE_DATE}'",
        reference_date=REFERENCE_DATE,
    )
    cleanup_start, cleanup_end = dr.cleanup_window
    logger.info(f"cleanup_window = ({cleanup_start}, {cleanup_end})")

    counts_before, total_before = read_table_dates(client)
    logger.info(f"清理前: {total_before} 条")

    # 构造 target（使用精确窗口）
    target = LarkTargetConfig(
        name="test_cleanup",
        url=TEST_URL,
        table_name=TABLE_NAME,
        field_mappings=[
            FieldMapping("文本", "文本", LarkFieldType.TEXT),
            FieldMapping("日期", "日期", LarkFieldType.DATE),
        ],
        cleanup_conditions=CleanupCondition.date_window("日期", cleanup_start, cleanup_end),
    )

    from workers.lib.lark_loader import cleanup_target_table
    deleted = cleanup_target_table(client, target, TABLE_ID)
    logger.info(f"  删除了 {deleted} 条记录")

    time.sleep(2)
    counts_after, total_after = read_table_dates(client)
    logger.info(f"清理后: {total_after} 条")

    # 构建预期
    expected_deleted: set[date] = set()
    d = cleanup_start
    while d <= cleanup_end:
        expected_deleted.add(d)
        d += timedelta(days=1)

    expected_kept: set[date] = set()
    d = DATA_START
    while d <= DATA_END:
        if d not in expected_deleted:
            expected_kept.add(d)
        d += timedelta(days=1)

    deleted_ok = all(counts_after.get(d, 0) == 0 for d in expected_deleted)
    kept_ok = all(counts_after.get(d, 0) > 0 for d in expected_kept)

    if not deleted_ok:
        for d in sorted(expected_deleted):
            if counts_after.get(d, 0) > 0:
                logger.error(f"  ✗ {d} 应删除但仍存在")
    if not kept_ok:
        for d in sorted(expected_kept):
            if counts_after.get(d, 0) == 0:
                logger.error(f"  ✗ {d} 应保留但丢失")

    if deleted_ok and kept_ok:
        logger.info(f"  ✓ 窗口内 {len(expected_deleted)} 天全部删除")
        logger.info(f"  ✓ 窗口外 {len(expected_kept)} 天全部保留")
        return True
    else:
        logger.error("  ✗ cleanup 验证失败")
        return False


# ============================================================
# 入口
# ============================================================

def main() -> int:
    logger.info("=" * 60)
    logger.info("日期窗口全链路验证（新版测试表）")
    logger.info(f"测试参数: reference_date={REFERENCE_DATE}, "
                f"start_offset={START_OFFSET}, end_offset={END_OFFSET}")
    logger.info(f"测试数据范围: {DATA_START} ~ {DATA_END}")
    logger.info("=" * 60)

    results: dict[str, bool] = {}

    # 纯逻辑测试（无需网络）
    results["sql_date_params"] = test_sql_date_params()
    results["sentinel"] = test_runtime_window_sentinel()

    # 需要网络的测试
    client = init_client()
    client.extract_app_information(url=TEST_URL)
    client.extract_table_information(app_token=client.app_token)

    write_test_data(client)
    time.sleep(2)

    results["lark_source_filter"] = test_lark_source_date_filter(client)
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
        logger.info("\n✓ 所有验证通过！")
    else:
        logger.error("\n✗ 存在失败项，请检查日志。")

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
