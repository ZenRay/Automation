# coding:utf8
"""workers.okr.transformer -- OKR 业务融合逻辑

从 lib 借用 DataTransformer 基础设施，在此注册 OKR 特定的：
1. merge_config：定义飞书数据与 SQL 数据如何关联合并
2. 源级清洗步骤（pre-merge）：通过 build_okr_source_cleaners 注册
3. 输出清洗步骤（post-merge）：通过 build_okr_transformer 注册

两阶段注册架构：
  build_okr_source_cleaners()  → per-source DataTransformer，聚合前执行
  build_okr_transformer()      → 单一 DataTransformer，merge 后执行

lib 的 DataTransformer 本身不含任何业务知识，
本文件是 OKR 数据融合逻辑的唯一注入点。
"""

import logging

import pandas as pd

from workers.lib import DataTransformer
from workers.okr.config import LARK_SOURCES

logger = logging.getLogger("workers.okr.transformer")


# -------------------------------------------------------------------------
# OKR 专属 merge_config：多步 merge 配置
#
# 数据融合流程：
#   Step 0: 源级清洗（pre-merge，通过 build_okr_source_cleaners 注册）
#   Step 1: quality_reject_tasks 按「日期」聚合（品控打回统计指标）
#   Step 2: ka_cat4_operate + store_cat4_stat 按「日期」merge + 统计处理
#   Step 3: mall_stat + (Step1 结果) + (Step2 结果) 按「日期」merge
#   Step 4: 输出清洗（post-merge，通过 build_okr_transformer 注册）
#
# 数据源引用格式：
#   "lark:<name>" -> 飞书拉取的 DataFrame
#   "mc:<name>"   -> MaxCompute 查询结果
# --------------------------------------------------------------------------
OKR_MERGE_CONFIG: dict = {
    "left":  "lark:quality_reject_tasks",
    "right": "lark:ka_cat4_operate",
    "how":   "left",
    "on":    "日期",
}

# --------------------------------------------------------------------------
# 飞书日期字段预处理（merge 前统一调用）
# --------------------------------------------------------------------------


def preprocess_lark_dates(
    lark_data: dict[str, pd.DataFrame],
    lark_sources: list = None,
) -> dict[str, pd.DataFrame]:
    """统一预处理飞书数据源的日期字段

    根据 LarkSourceConfig 中的 date_fields 和 datetime_fields 声明，
    对飞书拉取的 DataFrame 进行日期规范化：
    - date_fields: 去掉时间部分，只保留日期（.dt.normalize()）
    - datetime_fields: 保留时间，统一格式化为字符串

    设计原则：
    - 在 merge 前统一调用，确保所有飞书数据源的日期字段格式一致
    - 业务语义（哪些字段是日期）由 config 声明，预处理函数只负责执行
    - 不影响 MaxCompute 数据源（mc_data 不参与此处理）

    Args:
        lark_data: 飞书拉取的 DataFrame 字典 {source_name: df}
        lark_sources: LarkSourceConfig 列表，用于查找 date_fields 声明

    Returns:
        dict[str, pd.DataFrame]: 日期字段已规范化的 DataFrame 字典
    """
    if lark_sources is None:
        lark_sources = LARK_SOURCES

    # 构建 name -> config 映射
    source_map = {src.name: src for src in lark_sources}

    result = {}
    for name, df in lark_data.items():
        config = source_map.get(name)
        if config is None or df.empty:
            result[name] = df
            continue

        df_processed = df.copy()
        modified = False

        # 处理日期类型字段：去掉时间部分，只保留日期
        if config.date_fields:
            for col in config.date_fields:
                if col in df_processed.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_processed[col]):
                        # 已是 datetime 类型，直接截断时间
                        df_processed[col] = df_processed[col].dt.normalize()
                    elif pd.api.types.is_integer_dtype(df_processed[col]) or pd.api.types.is_float_dtype(df_processed[col]):
                        # 飞书可能返回 Excel 序列号格式的日期（如 46167 = 2026-06-01）
                        # 需要用 origin='1899-12-30', unit='D' 转换
                        df_processed[col] = pd.to_datetime(
                            df_processed[col], origin="1899-12-30", unit="D", errors="coerce"
                        ).dt.normalize()
                        logger.info(f"  [{name}] 日期字段 '{col}' 从 Excel 序列号转换")
                    else:
                        # 兼容字符串格式日期
                        df_processed[col] = pd.to_datetime(
                            df_processed[col], errors="coerce"
                        ).dt.normalize()
                    modified = True
                    logger.debug(f"  [{name}] 日期字段 '{col}' 已规范化（去掉时间）")

        # 处理日期时间类型字段：保留时间，统一格式化
        if config.datetime_fields:
            for col in config.datetime_fields:
                if col in df_processed.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_processed[col]):
                        df_processed[col] = df_processed[col].dt.strftime(
                            config.datetime_format
                        )
                    else:
                        # 尝试转换后再格式化
                        df_processed[col] = pd.to_datetime(
                            df_processed[col], errors="coerce"
                        ).dt.strftime(config.datetime_format)
                    modified = True
                    logger.debug(f"  [{name}] 日期时间字段 '{col}' 已格式化")

        if modified:
            logger.info(f"  [{name}] 日期预处理完成")
        result[name] = df_processed

    return result


# --------------------------------------------------------------------------
# 多步融合函数（main.py Step 4 调用）
# --------------------------------------------------------------------------


def _aggregate_quality_reject(
    df: pd.DataFrame,
    group_by_cols: list[str] = None,
) -> pd.DataFrame:
    """Step 1: 聚合 quality_reject_tasks（支持多维度）

    当前实现：
    1. 每一天的商品打回数量（count 商品id）
    2. 7日内重复打回商品数：每天统计 T0到T-6日内商品打回天数>1的商品打回数量

    架构设计：
    - group_by_cols 参数控制分组维度，默认按「日期」
    - 如需增加「日期+四级类目」维度，调用时传入 ["日期", "四级类目名称"]
    - 聚合指标统一在 agg_funcs 中管理，便于扩展

    Args:
        df: quality_reject_tasks 原始数据
        group_by_cols: 分组维度列，默认为 ["日期"]

    Returns:
        pd.DataFrame: 聚合后的 DataFrame
    """
    if group_by_cols is None:
        group_by_cols = ["日期"]

    # 日期字段已由 preprocess_lark_dates 规范化（去掉时间部分）
    # 此处直接使用即可，无需再做 .dt.normalize()
    df_work = df.copy()

    # 聚合指标定义（可扩展）
    agg_funcs = {
        "打回水果次数": ("商品id", "count"),
        "打回水果商品数": ("商品id", "nunique"),  # 每天有多少个不同的商品被打回
    }

    df_agg = df_work.groupby(group_by_cols[0], as_index=False).agg(**agg_funcs)
    df_agg = df_agg.rename(columns={group_by_cols[0]: "日期"})

    # 7日内重复打回商品数（仅在 group_by_cols 为 ["日期"] 时计算）
    if group_by_cols == ["日期"]:
        # 简化实现：直接遍历每个日期，计算该日期的 7 日重复打回商品数
        def _calc_repeat_reject(target_date: pd.Timestamp, df: pd.DataFrame) -> int:
            """计算某一天的 7 日重复打回商品数"""
            window_start = target_date - pd.Timedelta(days=6)  # T-6
            mask = (df["日期"] >= window_start) & (df["日期"] <= target_date)
            window_df = df[mask]

            if window_df.empty:
                return 0

            # 统计每个商品的打回天数 > 1 的商品数量
            goods_reject_days = window_df.groupby("商品id")["日期"].nunique()
            repeat_goods = goods_reject_days[goods_reject_days > 1]
            return len(repeat_goods)

        repeat_data = []
        for target_date in df_agg["日期"]:
            repeat_count = _calc_repeat_reject(target_date, df_work)
            repeat_data.append(repeat_count)

        df_agg["7日重复打回商品数"] = repeat_data
    return df_agg


def _widen_ka_to_long(ka_df: pd.DataFrame) -> pd.DataFrame:
    """将 ka_cat4_operate 从宽表转为长表

    原始宽表：日期, 店铺ID, 是否下单香蕉, 是否下单麒麟西瓜
    转换后长表：日期, 店铺ID, 四级类目名称, 是否ka已运营

    转换逻辑：
    - "是否下单香蕉"=是 → 四级类目名称="香蕉"
    - "是否下单麒麟西瓜"=是 → 四级类目名称="麒麟西瓜"
    - 只保留"是"的行（即该店铺当日确实下单了该品类）
    - 是否ka已运营=1：标记该行为 KA 已运营记录，
      left join 后 store_cat4_stat 中未匹配的行该列为 NaN，
      可用于区分"KA 已运营"与"未运营"的店铺品类组合

    Args:
        ka_df: ka_cat4_operate 原始 DataFrame

    Returns:
        pd.DataFrame: 长表格式，列为 [日期, 店铺ID, 四级类目名称, 是否ka已运营]
    """
    # 字段名 → 四级类目名称 映射
    col_to_cat4 = {
        "是否下单香蕉": "香蕉",
        "是否下单麒麟西瓜": "麒麟西瓜",
    }

    records = []
    for col, cat4_name in col_to_cat4.items():
        if col not in ka_df.columns:
            logger.warning(f"ka_cat4_operate 缺少字段: {col}")
            continue

        # 支持 "是/否" 文本和 1/0 数字两种格式
        # 使用 is_string_dtype 兼容 object 和 StringDtype 两种字符串类型
        if pd.api.types.is_string_dtype(ka_df[col]):
            mask = ka_df[col].astype(str).str.strip() == "是"
        else:
            mask = ka_df[col] == 1

        logger.debug(f"  字段 '{col}': dtype={ka_df[col].dtype}, 唯一值={ka_df[col].unique().tolist()[:5]}, mask.sum()={mask.sum()}")

        sub = ka_df[mask][["日期", "店铺ID"]].copy()
        sub["四级类目名称"] = cat4_name
        records.append(sub)

    if not records:
        logger.warning("ka_cat4_operate 宽表转长表失败：无有效品类字段")
        return pd.DataFrame(columns=["日期", "店铺ID", "四级类目名称", "是否ka已运营"])

    long_df = pd.concat(records, ignore_index=True)
    long_df["是否ka已运营"] = 1
    logger.info(f"ka_cat4_operate 宽表转长表: {ka_df.shape} -> {long_df.shape}")
    return long_df


def _aggregate_ka_operate(
    store_cat4_df: pd.DataFrame,
    ka_df: pd.DataFrame,
) -> pd.DataFrame:
    """Step 2: ka_cat4_operate + store_cat4_stat merge + 聚合

    流程：
      1. ka_cat4_operate 宽表转长表（日期+店铺ID+四级类目名称）
      2. 与 store_cat4_stat 按 日期+店铺id+四级类目名称 left join
      3. 按日期聚合，输出统计指标

    聚合指标：
    - 直营区域送达金额: 网格运营类型=直营区域，送达金额求和
    - 代理人区域送达金额: 网格运营类型=代理人区域，送达金额求和
    - 实验区域送达金额: 试验区域类型=试验区域试验组，送达金额求和
    - 蔬菜干货直营下单店铺数: 一级类目=蔬菜/干货 + 直营区域，店铺id去重
    - 代理人区域榴莲下单数量: 代理人区域 + 特殊品类=榴莲，下单数量求和
    - 榴莲下单店铺数: 特殊品类=榴莲 + 下单数量>0，店铺id去重
    - 试点区域对比送达金额: 试验区域类型=试验区域对照组，送达金额求和
    - 试点品类售后店铺数: ka试点品类=1 + 当日售后=1，店铺id去重
    - 试点品类8日售后复购店铺数: ka试点品类=1 + 当日售后=1 + 后八日下单=1，店铺id去重
    - 试点KA品类门店数: 是否ka已运营 求和（left join 匹配行数，即 KA 已运营的店铺品类组合数）
    - 试点KA品类门店8日复购品类门店数: 是否ka已运营=1 + 后八日下单=1，店铺id去重
    - 直营区域水果抽佣金额: 一级类目=水果 + 直营区域，抽佣金额求和
    - 代理人区域水果抽佣金额: 一级类目=水果 + 代理人区域，抽佣金额求和
    - 直营区域水果送达金额: 一级类目=水果 + 直营区域，送达金额求和
    - 代理人区域水果送达金额: 一级类目=水果 + 代理人区域，送达金额求和
    - 直营区域水果抽佣率: 直营区域水果抽佣金额 / 直营区域水果送达金额
    - 代理人区域水果抽佣率: 代理人区域水果抽佣金额 / 代理人区域水果送达金额
    - 以上 6 项均有「剔除特殊运营品类」版本（排除榴莲类四级类目）

    Args:
        store_cat4_df: store_cat4_stat MaxCompute 查询结果
        ka_df: ka_cat4_operate 飞书拉取数据

    Returns:
        pd.DataFrame: 按日期聚合后的统计结果
    """
    # 1. ka_cat4_operate 宽表转长表
    ka_long = _widen_ka_to_long(ka_df)
    ka_is_empty = ka_long.empty
    if ka_is_empty:
        logger.warning(
            "ka_cat4_operate 长表为空，仍以 store_cat4_stat 为主表执行 left join，"
            "KA 专属指标（是否ka已运营）将为 NaN"
        )

    # 2. 与 store_cat4_stat merge
    #    store_cat4_stat 列名: 店铺id（小写 id，可能是 int64）
    #    ka_cat4_operate 列名: 店铺ID（大写 ID，str 类型）
    #    统一转为 str 类型以确保 merge 兼容
    ka_long_merge = ka_long.copy()
    store_merge = store_cat4_df.copy()
    ka_long_merge["店铺ID"] = ka_long_merge["店铺ID"].astype(str).str.strip()
    store_merge["店铺id"] = store_merge["店铺id"].astype(str).str.strip()

    ka_merged = pd.merge(
        store_merge,
        ka_long_merge,
        left_on=["日期", "店铺id", "四级类目名称"],
        right_on=["日期", "店铺ID", "四级类目名称"],
        how="left",  # store_cat4_stat 是主表，保留全部行，KA 运营标记仅在匹配时填充
        suffixes=("", "_ka"),
    )
    logger.info(f"ka_cat4_operate + store_cat4_stat merged: {ka_merged.shape}")

    if ka_merged.empty:
        logger.warning("merge 结果为空，检查关联字段是否匹配")
        return pd.DataFrame(columns=["日期"])

    # 调试：输出关键列的数据分布
    logger.info(f"  网格运营类型分布: {ka_merged['网格运营类型'].value_counts().to_dict()}")
    logger.info(f"  是否试验区域分布: {ka_merged['是否试验区域'].value_counts().to_dict()}")
    logger.info(f"  一级类目名称分布: {ka_merged['一级类目名称'].value_counts().head(5).to_dict()}")
    logger.info(f"  送达金额 dtype={ka_merged['送达金额'].dtype}, 范围={ka_merged['送达金额'].min()}~{ka_merged['送达金额'].max()}")

    # 3. 确保数值列为 numeric 类型（MC read_table 可能返回 object 类型的 Decimal）
    numeric_cols = ["送达金额", "赔付金额", "下单数量", "抽佣金额"]
    for col in numeric_cols:
        if col in ka_merged.columns:
            ka_merged[col] = pd.to_numeric(ka_merged[col], errors="coerce").fillna(0)

    # 布尔/标志列也统一转 numeric（MC 可能返回 Decimal 或 str）
    flag_cols = ["是否ka试点品类", "是否当日售后", "是否后八日下单"]
    for col in flag_cols:
        if col in ka_merged.columns:
            ka_merged[col] = pd.to_numeric(ka_merged[col], errors="coerce").fillna(0)

    # 4. 预计算指标列（向量化计算，避免 apply lambda）
    is_direct = ka_merged["网格运营类型"] == "直营区域"
    is_agent = ka_merged["网格运营类型"] == "代理人区域"
    is_trial_treatment = ka_merged["试验区域类型"] == "试验区域试验组"

    ka_merged["_直营送达金额"] = ka_merged["送达金额"].where(is_direct, 0)
    ka_merged["_代理人送达金额"] = ka_merged["送达金额"].where(is_agent, 0)
    ka_merged["_实验送达金额"] = ka_merged["送达金额"].where(is_trial_treatment, 0)

    # 蔬菜/干货 + 直营的店铺id（非匹配行设为 NaN，nunique 会忽略）
    is_veg_direct = (
        ka_merged["一级类目名称"].isin(["蔬菜", "干货"]) & is_direct
    )
    ka_merged["_蔬菜干货直营"] = ka_merged["店铺id"].where(is_veg_direct)

    # --- 新增指标条件 ---
    is_durian = ka_merged["特殊品类运营类型"] == "榴莲"
    is_trial_control = ka_merged["试验区域类型"] == "试验区域对照组"
    is_ka_pilot = ka_merged["是否ka试点品类"] == 1
    is_after_sale = ka_merged["是否当日售后"] == 1
    is_rebuy_8d = ka_merged["是否后八日下单"] == 1

    # 代理人区域榴莲下单数量
    ka_merged["_代理人榴莲下单量"] = ka_merged["下单数量"].where(
        is_agent & is_durian, 0
    )
    # 榴莲下单店铺数（下单数量>0）
    is_durian_ordered = is_durian & (ka_merged["下单数量"] > 0)
    ka_merged["_榴莲下单店铺"] = ka_merged["店铺id"].where(is_durian_ordered)
    # 试点区域对比送达金额（试验区域对照组）
    ka_merged["_试点对比送达金额"] = ka_merged["送达金额"].where(is_trial_control, 0)
    # 试点品类售后店铺数
    is_pilot_aftersale = is_ka_pilot & is_after_sale
    ka_merged["_试点售后店铺"] = ka_merged["店铺id"].where(is_pilot_aftersale)
    # 试点品类8日售后复购店铺数
    is_pilot_rebuy = is_ka_pilot & is_after_sale & is_rebuy_8d
    ka_merged["_试点复购店铺"] = ka_merged["店铺id"].where(is_pilot_rebuy)

    # --- 水果抽佣/送达金额（一级类目=水果）---
    is_fruit = ka_merged["一级类目名称"] == "水果"
    ka_merged["_直营水果抽佣"] = ka_merged["抽佣金额"].where(is_fruit & is_direct, 0)
    ka_merged["_代理人水果抽佣"] = ka_merged["抽佣金额"].where(is_fruit & is_agent, 0)
    ka_merged["_直营水果送达"] = ka_merged["送达金额"].where(is_fruit & is_direct, 0)
    ka_merged["_代理人水果送达"] = ka_merged["送达金额"].where(is_fruit & is_agent, 0)

    # --- 水果抽佣/送达金额（剔除特殊运营品类：一级类目=水果 且 非榴莲类）---
    is_not_special = ~is_durian  # 是否特殊品类运营=0（排除榴莲类）
    ka_merged["_直营水果抽佣_ex"] = ka_merged["抽佣金额"].where(is_fruit & is_direct & is_not_special, 0)
    ka_merged["_代理人水果抽佣_ex"] = ka_merged["抽佣金额"].where(is_fruit & is_agent & is_not_special, 0)
    ka_merged["_直营水果送达_ex"] = ka_merged["送达金额"].where(is_fruit & is_direct & is_not_special, 0)
    ka_merged["_代理人水果送达_ex"] = ka_merged["送达金额"].where(is_fruit & is_agent & is_not_special, 0)

    # --- KA 已运营指标（基于 left join 后的 是否ka已运营 字段）---
    # 是否ka已运营: matched=1, unmatched=NaN → fillna(0) 后 sum 即为 KA 已运营的店铺品类组合数
    ka_merged["_ka运营标记"] = ka_merged["是否ka已运营"].fillna(0)
    # 试点KA品类8日复购：ka已运营=1 且 后八日下单=1
    is_ka_operated_rebuy = (ka_merged["_ka运营标记"] == 1) & is_rebuy_8d
    ka_merged["_ka运营复购店铺"] = ka_merged["店铺id"].where(is_ka_operated_rebuy)

    # 5. 按日期聚合
    ka_agg = ka_merged.groupby("日期", as_index=False).agg(
        **{
            "直营区域送达金额": ("_直营送达金额", "sum"),
            "代理人区域送达金额": ("_代理人送达金额", "sum"),
            "实验区域送达金额": ("_实验送达金额", "sum"),
            "蔬菜干货直营下单店铺数": ("_蔬菜干货直营", "nunique"),
            "代理人区域榴莲下单数量": ("_代理人榴莲下单量", "sum"),
            "榴莲下单店铺数": ("_榴莲下单店铺", "nunique"),
            "试点区域对比送达金额": ("_试点对比送达金额", "sum"),
            "试点品类售后店铺数": ("_试点售后店铺", "nunique"),
            "试点品类8日售后复购店铺数": ("_试点复购店铺", "nunique"),
            "试点KA品类门店数": ("_ka运营标记", "sum"),
            "试点KA品类门店8日复购品类门店数": ("_ka运营复购店铺", "nunique"),
            "直营区域水果抽佣金额": ("_直营水果抽佣", "sum"),
            "代理人区域水果抽佣金额": ("_代理人水果抽佣", "sum"),
            "直营区域水果送达金额": ("_直营水果送达", "sum"),
            "代理人区域水果送达金额": ("_代理人水果送达", "sum"),
            "直营区域水果抽佣金额【剔除特殊运营品类】": ("_直营水果抽佣_ex", "sum"),
            "代理人区域水果抽佣金额【剔除特殊运营品类】": ("_代理人水果抽佣_ex", "sum"),
            "直营区域水果送达金额【剔除特殊运营品类】": ("_直营水果送达_ex", "sum"),
            "代理人区域水果送达金额【剔除特殊运营品类】": ("_代理人水果送达_ex", "sum"),
        }
    )
    # 6. 计算比率指标（除法需后置，在聚合完成后计算）
    ka_agg["直营区域水果抽佣率"] = (
        ka_agg["直营区域水果抽佣金额"] / ka_agg["直营区域水果送达金额"]
    ).replace([float("inf"), float("-inf")], 0).fillna(0)
    ka_agg["代理人区域水果抽佣率"] = (
        ka_agg["代理人区域水果抽佣金额"] / ka_agg["代理人区域水果送达金额"]
    ).replace([float("inf"), float("-inf")], 0).fillna(0)
    # 剔除特殊运营品类的抽佣率
    ka_agg["直营区域水果抽佣率【剔除特殊运营品类】"] = (
        ka_agg["直营区域水果抽佣金额【剔除特殊运营品类】"] / ka_agg["直营区域水果送达金额【剔除特殊运营品类】"]
    ).replace([float("inf"), float("-inf")], 0).fillna(0)
    ka_agg["代理人区域水果抽佣率【剔除特殊运营品类】"] = (
        ka_agg["代理人区域水果抽佣金额【剔除特殊运营品类】"] / ka_agg["代理人区域水果送达金额【剔除特殊运营品类】"]
    ).replace([float("inf"), float("-inf")], 0).fillna(0)

    logger.info(f"ka_operate aggregated by date: {ka_agg.shape}")
    return ka_agg


def execute_okr_merge(
    lark_data: dict[str, pd.DataFrame],
    mc_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """执行 OKR 完整多步数据融合

    数据融合流程：
      Step 0: 源级清洗（pre-merge，通过 build_okr_source_cleaners 注册）
      Step 1: quality_reject_tasks 按「日期」聚合（品控打回统计指标）
      Step 2: ka_cat4_operate + store_cat4_stat 按「日期」merge + 统计处理
      Step 3: mall_stat + (Step1 结果) + (Step2 结果) 按「日期」merge
      Step 4: 应用输出清洗步骤（post-merge，通过 build_okr_transformer 注册）

    Returns:
        pd.DataFrame: 融合后的 DataFrame
    """
    logger.info("Starting OKR multi-step merge...")

    # 预处理：统一规范化飞书数据源的日期字段
    lark_data = preprocess_lark_dates(lark_data)
    logger.info("Date preprocessing done for all Lark sources")

    quality_df = lark_data.get("quality_reject_tasks")
    ka_df = lark_data.get("ka_cat4_operate")
    store_cat4_df = mc_data.get("store_cat4_stat")
    mall_stat_df = mc_data.get("mall_stat")

    # 检查数据源
    if quality_df is None:
        raise ValueError("Missing lark source: quality_reject_tasks")
    if ka_df is None:
        raise ValueError("Missing lark source: ka_cat4_operate")
    if store_cat4_df is None:
        raise ValueError("Missing mc source: store_cat4_stat")
    if mall_stat_df is None:
        raise ValueError("Missing mc source: mall_stat")

    # 统一「日期」列类型：飞书源可能是 str，MC 源是 datetime64，统一为 datetime64
    def _ensure_datetime(df: pd.DataFrame, col: str = "日期") -> pd.DataFrame:
        if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    for name in lark_data:
        lark_data[name] = _ensure_datetime(lark_data[name])
    for name in mc_data:
        mc_data[name] = _ensure_datetime(mc_data[name])

    # 重新获取引用（类型转换后）
    quality_df = lark_data["quality_reject_tasks"]
    ka_df = lark_data["ka_cat4_operate"]
    store_cat4_df = mc_data["store_cat4_stat"]
    mall_stat_df = mc_data["mall_stat"]

    # Step 0: 源级清洗（pre-merge，通过注册的 DataTransformer 执行）
    source_cleaners = build_okr_source_cleaners()
    for name, cleaner in source_cleaners.items():
        if name in lark_data:
            lark_data[name] = cleaner.transform(lark_data[name])
        elif name in mc_data:
            mc_data[name] = cleaner.transform(mc_data[name])
        else:
            logger.warning(f"Source cleaner registered for '{name}' but not found in data")

    # 重新获取引用（源级清洗后）
    quality_df = lark_data["quality_reject_tasks"]

    # Step 1: 聚合 quality_reject_tasks（占位，待业务确认）
    quality_agg = _aggregate_quality_reject(quality_df)
    logger.info(f"Step 1: quality_reject_tasks aggregated -> {quality_agg.shape}")

    # Step 2: ka_cat4_operate + store_cat4_stat merge + 聚合
    #   2a: ka_cat4_operate 宽表转长表（日期+店铺ID+四级类目名称）
    #   2b: 与 store_cat4_stat 按 日期+店铺id+四级类目名称 merge
    #   2c: 按日期聚合，输出统计指标
    ka_agg = _aggregate_ka_operate(store_cat4_df, ka_df)
    logger.info(f"Step 2: ka_operate aggregated -> {ka_agg.shape}")

    # Step 3: mall_stat + quality_stats + ka_stats 按「日期」merge
    # 3a: mall_stat + quality_stats
    step3a = pd.merge(
        mall_stat_df,
        quality_agg,
        on="日期",
        how="left",
        suffixes=("_mall", "_qa"),
    )
    logger.info(f"Step 3a: mall_stat + quality_stats -> {step3a.shape}")

    # 3b: step3a + ka_stats
    final_df = pd.merge(
        step3a,
        ka_agg,
        on="日期",
        how="left",
        suffixes=("_mall", "_ka"),
    )
    logger.info(f"Step 3b: mall_stat + quality_stats + ka_stats -> {final_df.shape}")

    # Step 4: 应用业务清洗步骤
    transformer = build_okr_transformer()
    result_df = transformer.transform(final_df)
    logger.info(f"OKR merge completed: {result_df.shape}")
    return result_df


# -------------------------------------------------------------------------
# OKR 源级清洗步骤（pre-merge，由 build_okr_source_cleaners 注册）
#
# 在 execute_okr_merge 中，日期预处理之后、聚合之前执行。
# 每个数据源可注册独立的 DataTransformer，互不干扰。
# -------------------------------------------------------------------------

def _dedup_quality_reject(df: pd.DataFrame) -> pd.DataFrame:
    """quality_reject_tasks 去重：同一日期+商品id 只保留一条

    飞书多维表格中可能存在重复录入的打回记录，
    不去重会导致 count(商品id) 被膨胀，7日重复打回计算也会失真。
    """
    before = len(df)
    df = df.drop_duplicates(subset=["日期", "商品id"])
    after = len(df)
    if before != after:
        logger.info(f"quality_reject_tasks dedup: {before} -> {after} rows (removed {before - after} duplicates)")
    return df


def build_okr_source_cleaners() -> dict[str, DataTransformer]:
    """构建并返回各数据源的 pre-merge 清洗器

    Returns:
        dict[str, DataTransformer]：{source_name -> transformer}
        只有需要清洗的源才出现在字典中，其余源直接跳过。

    使用方式：
        cleaners = build_okr_source_cleaners()
        for name, cleaner in cleaners.items():
            lark_data[name] = cleaner.transform(lark_data[name])
    """
    cleaners: dict[str, DataTransformer] = {}

    # quality_reject_tasks: 按日期+商品id 去重
    quality_cleaner = DataTransformer()
    quality_cleaner.register_step("dedup_by_date_product", _dedup_quality_reject)
    cleaners["quality_reject_tasks"] = quality_cleaner

    total_steps = sum(len(c._steps) for c in cleaners.values())
    logger.info(
        f"OKR source cleaners built: {len(cleaners)} source(s), "
        f"{total_steps} total step(s)"
    )
    return cleaners


# -------------------------------------------------------------------------
# OKR 输出清洗步骤（post-merge，由 build_okr_transformer 注册）
# -------------------------------------------------------------------------

def _drop_internal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """删除融合过程中产生的中间计算列（以 _ 开头的列）

    这些列（如 _直营送达金额、_代理人送达金额 等）仅用于聚合计算，
    不应出现在最终输出中。
    """
    internal_cols = [c for c in df.columns if c.startswith("_")]
    if internal_cols:
        logger.info(f"Dropping {len(internal_cols)} internal column(s): {internal_cols}")
        df = df.drop(columns=internal_cols)
    return df


def build_okr_transformer() -> DataTransformer:
    """构建并返回注册了 OKR 业务步骤的 DataTransformer 实例

    使用方式：
        transformer = build_okr_transformer()
        result_df = transformer.transform(merged_df)
    """
    t = DataTransformer()
    t.register_step("drop_internal_columns", _drop_internal_columns)

    logger.info(
        f"OKR transformer built with {len(t._steps)} registered step(s)"
    )
    return t