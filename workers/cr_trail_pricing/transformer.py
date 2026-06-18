# coding:utf8
"""workers.cr_trail_pricing.transformer -- 7 阶段数据处理流水线

每个 stage 函数独立可测试，接收 DataFrame 输入并返回 DataFrame 输出。
"""

import json
import logging

import numpy as np
import pandas as pd

from .config import (
    COLUMN_RENAME_MAP,
    OUTPUT_COLUMNS,
    PRODUCT_KEEP_FIELDS,
    REGION_OUTPUT_FIELDS,
)

logger = logging.getLogger(__name__)


def _in_date_range(series_start, series_end, target_date):
    """pandas-level date range filter: start <= target <= end"""
    start = pd.to_datetime(series_start).dt.date
    end = pd.to_datetime(series_end).dt.date
    return (start <= target_date) & (end >= target_date)


# ---------------------------------------------------------------------------
# Stage 1: 数据提取
# ---------------------------------------------------------------------------

def extract_sources(lark_client, sources):
    """从飞书拉取 6 张配置表，返回 {name: DataFrame}

    性能优化：内联 extract_single_source 逻辑，缓存
    extract_app_information + extract_table_information 结果，
    避免同一 app_token 下重复调用（6 张表共享同一 app_token）。

    时区修正：飞书 DATE 字段以 UTC 毫秒时间戳传输，
    ``pd.Timestamp(value, unit='ms')`` 生成 UTC naive datetime。
    例如北京时间 2026-06-18 00:00:00 → API 返回 2026-06-17 16:00:00 UTC。
    lib 层的 _apply_date_filter 直接用 UTC datetime 与 target_date 比较，
    导致 +8h 偏移、记录被错误过滤。

    解决方案：跳过 lib 的日期过滤，在后处理中统一做 UTC→UTC+8 转换，
    然后以正确的北京时间执行日期筛选。
    """
    from datetime import timedelta
    from workers.lib.lark_extractor import _fetch_all_records, _records_to_dataframe

    _UTC_OFFSET = timedelta(hours=8)

    # 缓存 app_token 和 tables_map，避免每张表重复 2 次 API 调用
    _app_cache: dict[str, str] = {}      # url_prefix -> app_token
    _tables_cache: dict[str, dict] = {}  # app_token -> {table_name: table_id}

    data = {}
    for source in sources:
        logger.info(f"Extracting Lark source: {source.name}")

        # 1. 解析 URL，获取 app_token（缓存）
        url_prefix = source.url.split("?")[0]
        if url_prefix not in _app_cache:
            lark_client.extract_app_information(url=source.url)
            _app_cache[url_prefix] = lark_client.app_token
        app_token = _app_cache[url_prefix]

        # 2. 获取表格列表（缓存）
        if app_token not in _tables_cache:
            _tables_cache[app_token] = lark_client.extract_table_information(app_token=app_token)
        tables_map = _tables_cache[app_token]

        # 3. 按 table_name 匹配 table_id
        table_id = tables_map.get(source.table_name)
        if table_id is None:
            available = list(tables_map.keys())
            raise ValueError(
                f"Table '{source.table_name}' not found in app_token={app_token}. "
                f"Available tables: {available}"
            )

        logger.info(
            f"Source '{source.name}': app_token={app_token}, "
            f"table_id={table_id}, view_id={source.view_id}"
        )

        # 4. 获取字段元数据 + 分页拉取所有记录
        field_type_map, all_records = _fetch_all_records(
            lark_client,
            table_id=table_id,
            view_id=source.view_id,
            field_names=source.field_names,
        )

        # 5. 转换为 DataFrame（跳过 lib 的 _apply_date_filter，
        #    因为它的 UTC naive datetime 与 target_date 比较有 8h 偏移）
        df = _records_to_dataframe(all_records, field_type_map)
        data[source.name] = df
        logger.info(f"Lark source '{source.name}' extracted: {len(df)} rows")

    # ── 后处理：UTC+8 时区修正 + 日期转换 + 日期过滤 ──────────────
    for name, df in data.items():
        src = next((s for s in sources if s.name == name), None)
        if not src or not src.date_fields:
            continue
        for col in src.date_fields:
            if col not in df.columns:
                continue
            # 将 pd.Timestamp (UTC naive) 转为 datetime64，加 +8h 得到北京时间，再取 .date
            df[col] = (
                pd.to_datetime(df[col]) + _UTC_OFFSET
            ).dt.date

        # UTC+8 时区感知的日期过滤
        if (
            src.date_filter_field
            and src.date_filter_field in df.columns
            and (
                src.date_filter_start_date is not None
                or src.date_filter_end_date is not None
            )
        ):
            original_len = len(df)
            mask = pd.Series(True, index=df.index)
            if src.date_filter_start_date is not None:
                mask = mask & (df[src.date_filter_field] >= src.date_filter_start_date)
            if src.date_filter_end_date is not None:
                mask = mask & (df[src.date_filter_field] <= src.date_filter_end_date)
            df = df[mask].reset_index(drop=True)
            data[name] = df
            logger.info(
                f"Date filter (UTC+8) on '{src.date_filter_field}': "
                f"kept {len(df)}/{original_len} rows "
                f"(start={src.date_filter_start_date}, end={src.date_filter_end_date})"
            )

    # ── 统一转为 numpy 后端 ──────────────────────────────────
    # 飞书 API 返回的数据可能使用 pyarrow 后端 (ArrowDtype)，
    # 导致下游 merge / np.where 等操作出现类型冲突。
    # 在此统一转为 numpy dtype，消除后续所有 stage 的隐患。
    for name in data:
        df = data[name]
        convert_cols = {}
        for col in df.columns:
            dtype = df[col].dtype
            if hasattr(dtype, "pyarrow_dtype"):
                if dtype.kind in ("i", "f"):
                    convert_cols[col] = "float64" if dtype.kind == "f" else "int64"
                elif dtype.kind in ("U", "O", "S"):
                    convert_cols[col] = "object"
                else:
                    convert_cols[col] = "object"
        if convert_cols:
            data[name] = df.astype(convert_cols)

    return data


# ---------------------------------------------------------------------------
# Stage 2: 商品筛选
# ---------------------------------------------------------------------------

def filter_trial_products(conf_goods, conf_trial_goods, target_date):
    """INNER JOIN on 商品id，筛选试验商品

    - conf_trial_goods: 已由 API 过滤到 日期=target_date
    - conf_goods: 已由 API 过滤到 日期=target_date-1
    - 非试验区域抽佣率 取自 conf_trial_goods（suffixes 处理同名冲突）
    - 新增 是否试验区域 = 1
    """
    if conf_goods.empty or conf_trial_goods.empty:
        return pd.DataFrame(columns=PRODUCT_KEEP_FIELDS)

    # 仅保留 conf_trial_goods 的 商品id + 非试验区域抽佣率
    trial_cols = ["商品id", "非试验区域抽佣率"]
    trial_subset = conf_trial_goods[[c for c in trial_cols if c in conf_trial_goods.columns]]

    result = conf_goods.merge(
        trial_subset,
        on="商品id",
        how="inner",
        suffixes=("_goods", "_trial"),
    )

    # 丢弃 conf_goods 的非试验区域抽佣率，保留 conf_trial_goods 的
    if "非试验区域抽佣率_goods" in result.columns:
        result = result.drop(columns=["非试验区域抽佣率_goods"])
    if "非试验区域抽佣率_trial" in result.columns:
        result = result.rename(columns={"非试验区域抽佣率_trial": "非试验区域抽佣率"})

    result["是否试验区域"] = 1
    return result[[c for c in PRODUCT_KEEP_FIELDS if c in result.columns]].copy()


# ---------------------------------------------------------------------------
# Stage 3: 区域标记
# ---------------------------------------------------------------------------

def mark_trial_regions(conf_county, conf_trial_group, target_date):
    """LEFT JOIN on 市id=区域id, 标记是否试验区域"""
    city_groups = conf_trial_group[
        (conf_trial_group["区域类型"] == "CITY")
        & _in_date_range(
            conf_trial_group["试验起始日期"],
            conf_trial_group["试验结束日期"],
            target_date,
        )
    ]
    join_cols = ["区域id", "区域类型", "试验分组",
                 "试验起始日期", "试验结束日期"]
    city_groups = city_groups[[c for c in join_cols if c in city_groups.columns]]

    overlap = set(city_groups.columns) & set(conf_county.columns)
    county_clean = conf_county.drop(columns=[c for c in overlap if c in conf_county.columns],
                                    errors="ignore")

    result = county_clean.merge(
        city_groups, left_on="市id", right_on="区域id", how="left",
    )
    result["是否试验区域"] = result["试验分组"].notna().astype(int)

    for col in ["试验分组"]:
        if col in result.columns:
            result[col] = result[col].fillna("")

    keep = [c for c in list(county_clean.columns) + ["是否试验区域", "试验分组"]
            if c in result.columns]
    return result[keep].copy()


# ---------------------------------------------------------------------------
# Stage 4: 抽佣关联
# ---------------------------------------------------------------------------

def associate_commission(regions_df, conf_trial_commission, target_date):
    """LEFT JOIN on 试验分组 + 运营类型, 关联抽佣率

    每个试验分组有 2 条记录（自营区域/代理人区域），
    必须将运营类型纳入 JOIN 条件以避免行数翻倍。
    """
    active = conf_trial_commission[
        _in_date_range(
            conf_trial_commission["试验起始日期"],
            conf_trial_commission["试验结束日期"],
            target_date,
        )
    ]
    join_cols = ["试验分组", "运营类型"]
    result = regions_df.merge(
        active[[c for c in join_cols + ["抽佣率"] if c in active.columns]],
        on=join_cols,
        how="left",
    )
    # 不填充 0，保留 NaN 以便未匹配行在下游计算中自然传播
    return result[[c for c in REGION_OUTPUT_FIELDS if c in result.columns]].copy()


# ---------------------------------------------------------------------------
# Stage 5: 隐形物流费
# ---------------------------------------------------------------------------

def _parse_mapping(mapping_str):
    """解析单条区县费率映射 JSON，失败返回 None"""
    if pd.isna(mapping_str) or mapping_str == "":
        return None
    try:
        return json.loads(str(mapping_str))
    except (json.JSONDecodeError, TypeError):
        return None


def associate_logistics_fee(regions_df, conf_hidden_logistics, target_date):
    """LEFT JOIN on 市id, 计算隐形物流费率（向量化实现）"""
    # 日期列已是 UTC+8 date 对象，直接比较
    logistics = conf_hidden_logistics[
        conf_hidden_logistics["日期"] == target_date
    ].copy()
    logistics = logistics.rename(columns={"费率": "_fee_rate"})

    # 将 pyarrow 后端列转为 object dtype，避免 merge 时类型冲突
    for col in ["_fee_rate", "区县费率映射"]:
        if col in logistics.columns:
            logistics[col] = logistics[col].astype(object)

    result = regions_df.merge(
        logistics[["市id", "_fee_rate", "区县费率映射"]],
        on="市id", how="left",
    )

    # 向量化 4-branch 逻辑
    mapping_parsed = result["区县费率映射"].apply(_parse_mapping)
    county_ids = result["区县id"].apply(
        lambda x: str(int(x)) if pd.notna(x) else ""
    )

    # 默认：使用城市级别费率
    rates = result["_fee_rate"].copy()

    # 区县级别覆盖：mapping 存在且包含该区县id
    has_override = pd.Series([
        m is not None and cid != "" and cid in m
        for m, cid in zip(mapping_parsed, county_ids)
    ], index=result.index)
    # 逐行应用覆盖（仅对有的行）
    for idx in result.index[has_override]:
        m = mapping_parsed.loc[idx]
        cid = county_ids.loc[idx]
        rates.loc[idx] = m[cid]

    # 不填充 0：未匹配物流表的行保留 NaN
    result["隐形物流费率"] = rates
    result = result.drop(columns=["_fee_rate", "区县费率映射"],
                         errors="ignore")
    return result


# ---------------------------------------------------------------------------
# Stage 6: 笛卡尔积计算
# ---------------------------------------------------------------------------

def compute_pricing(products_df, regions_df):
    """笛卡尔积 + 计算抽佣比例/货值/调价方向"""
    if products_df.empty or regions_df.empty:
        return pd.DataFrame()

    # 是否试验区域 同时存在于 products 和 regions 中，
    # 保留 regions 的版本（来自 Stage 3 标记），移除 products 的以避免列名冲突
    if "是否试验区域" in products_df.columns:
        products_df = products_df.drop(columns=["是否试验区域"])

    products_df = products_df.assign(_key=1)
    regions_df = regions_df.assign(_key=1)
    result = products_df.merge(regions_df, on="_key").drop(columns=["_key"])

    is_trial = result["是否试验区域"] == 1

    # 固定抽佣比例 = 抽佣率 - 非试验区域抽佣率（无 abs）
    # 负值 = 试验区域抽佣更低 → 降价；正值 = 试验区域抽佣更高 → 涨价
    raw_diff = result["抽佣率"] - result["非试验区域抽佣率"]
    result["固定抽佣比例"] = np.where(is_trial, raw_diff, np.nan)
    result["固定抽佣货值"] = np.where(
        ~is_trial, result["隐形物流费率"] * result["毛重"], np.nan,
    )

    def _direction(row):
        if row["是否试验区域"] == 0:
            return "涨价"
        diff = row["抽佣率"] - row["非试验区域抽佣率"]
        if diff < 0:
            return "降价"
        elif diff == 0:
            return "不变"
        return "涨价"

    result["调价方向"] = result.apply(_direction, axis=1)

    # 方向已由 sign 决定，最终输出取绝对值
    result["固定抽佣比例"] = result["固定抽佣比例"].abs()

    # 过滤：只保留 固定抽佣比例 / 固定抽佣货值 至少一个非空的行
    has_value = result["固定抽佣比例"].notna() | result["固定抽佣货值"].notna()
    result = result[has_value].reset_index(drop=True)

    result["调价幅度"] = np.nan
    result["设置状态"] = "启用"
    return result


# ---------------------------------------------------------------------------
# Stage 7: Excel 输出
# ---------------------------------------------------------------------------

def export_excel(df, output_path):
    """重命名列、选择输出列、导出 Excel"""
    if df.empty:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
        out.to_excel(output_path, index=False)
        return out
    out = df.rename(columns=COLUMN_RENAME_MAP).copy()
    out = out[[c for c in OUTPUT_COLUMNS if c in out.columns]]
    # 将比例类列从小数转为百分比显示（0.05 → 5）
    _RATE_COLS = [
        "固定抽佣比例", "平台基础抽佣率", "平台总抽佣率",
        "总抽佣率", "隐形物流费率",
    ]
    for col in _RATE_COLS:
        if col in out.columns:
            out[col] = out[col] * 100
    if "调价幅度" in out.columns:
        out["调价幅度"] = out["调价幅度"].fillna("")
    out.to_excel(output_path, index=False)
    return out
