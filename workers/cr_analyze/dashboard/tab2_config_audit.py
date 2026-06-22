# coding:utf8
"""Tab 2: 配置核查 (H-1 抽佣率 + H-2 供货价)"""

import pandas as pd
import numpy as np
import streamlit as st

from workers.cr_analyze.config import TARGET_R0_REFERENCE


def render(data: dict[str, pd.DataFrame]):
    adj = data.get("conf_commission_adjustment", pd.DataFrame())
    period_rate = data.get("conf_trial_period_rate", pd.DataFrame())
    product_info = data.get("conf_product_info", pd.DataFrame())

    # H-1: 抽佣率配置核查
    st.subheader("H-1: 抽佣率配置核查")
    if not adj.empty and not period_rate.empty:
        check_df = adj.copy()

        # 标记参与试验类型
        if "参与试验类型" in check_df.columns:
            check_df["is_trial"] = check_df["参与试验类型"].apply(
                lambda x: (
                    "试验区域" in str(x) and "非" not in str(x)
                    if pd.notna(x)
                    else False
                )
            )
            non_trial = check_df[~check_df["is_trial"]]
            if not non_trial.empty:
                st.warning(f"发现 {len(non_trial)} 条非试验区域记录")

        # 构建目标 r₀ 映射表 (当前阶段 × 试验分组 → target_r)
        # 使用 conf_trial_group 获取试验分组
        trial_group = data.get("conf_trial_group", pd.DataFrame())
        if not trial_group.empty and "区县名称" in check_df.columns:
            # 通过区县名称 → conf_county_info → 市名称 → conf_trial_group → 试验分组
            county_info = data.get("conf_county_info", pd.DataFrame())
            if not county_info.empty:
                county_to_city = county_info[["区县名称", "市名称"]].drop_duplicates()
                tg_city = trial_group[["市名称", "试验分组"]].drop_duplicates()
                county_to_group = county_to_city.merge(tg_city, on="市名称", how="left")

                check_df = check_df.merge(
                    county_to_group[["区县名称", "试验分组"]],
                    on="区县名称",
                    how="left",
                )

        # 计算配置偏差: 使用固定抽佣率调整作为 configured_r
        if "固定抽佣率调整" in check_df.columns:
            # 获取当前阶段的目标 r₀
            from datetime import date as dt_date

            today = dt_date.today()
            current_stage = None
            if not period_rate.empty:
                pr = period_rate.copy()
                for col in ["试验起始日期", "试验结束日期"]:
                    if col in pr.columns:
                        pr[col] = pd.to_datetime(pr[col], errors="coerce").dt.date
                for _, row in pr.iterrows():
                    s, e = row.get("试验起始日期"), row.get("试验结束日期")
                    if pd.notna(s) and pd.notna(e) and s <= today <= e:
                        current_stage = row.get("试验阶段")
                        break

            # 查找 target_r: 按 (stage, trial_group) 从 TARGET_R0_REFERENCE 获取
            if current_stage and current_stage in TARGET_R0_REFERENCE:
                ref = TARGET_R0_REFERENCE[current_stage]
                # 取自营区域的目标值
                self_targets = ref.get("自营区域", {})

                def get_target_r(group):
                    if pd.isna(group):
                        return np.nan
                    return self_targets.get(group, np.nan)

                check_df["target_r"] = check_df["试验分组"].apply(get_target_r)
                check_df["r_deviation"] = (
                    check_df["固定抽佣率调整"] - check_df["target_r"]
                )

                # 高亮偏差 > 0.5%
                check_df["alert"] = check_df["r_deviation"].apply(
                    lambda x: "🔴" if pd.notna(x) and abs(x) > 0.005 else "✅"
                )

        # 显示偏差表
        display_cols = [
            c
            for c in [
                "日期",
                "商品id",
                "区县名称",
                "试验分组",
                "固定抽佣率调整",
                "target_r",
                "r_deviation",
                "alert",
                "参与试验类型",
            ]
            if c in check_df.columns
        ]
        if display_cols:
            st.dataframe(check_df[display_cols].head(100), use_container_width=True)
        else:
            st.dataframe(check_df.head(100), use_container_width=True)
    elif not adj.empty:
        st.dataframe(adj.head(100), use_container_width=True)
    else:
        st.info("暂无抽佣率调整数据")

    # 目标配置值参考
    st.subheader("各阶段目标 r₀ 参考值")
    ref_rows = []
    for phase, region_groups in TARGET_R0_REFERENCE.items():
        for region_type, groups in region_groups.items():
            for group, r0 in groups.items():
                ref_rows.append(
                    {
                        "阶段": phase,
                        "区域类型": region_type,
                        "试验分组": group,
                        "目标 r₀": f"{r0:.1%}",
                    }
                )
    st.dataframe(pd.DataFrame(ref_rows), use_container_width=True)

    # H-2: 商家供货价核查
    st.subheader("H-2: 非试验区域供货价趋势")
    if not product_info.empty:
        prod = product_info.copy()

        # 计算隐含抽佣率
        price_col = "非试验区域平台销售斤单价"
        supply_col = "非试验区域商家供货斤单价"
        if price_col in prod.columns and supply_col in prod.columns:
            prod["implied_r"] = np.where(
                prod[price_col] > 0,
                (prod[price_col] - prod[supply_col]) / prod[price_col],
                np.nan,
            )

            # 按商品和日期排序
            if "日期" in prod.columns and "商品id" in prod.columns:
                prod["日期"] = pd.to_datetime(prod["日期"], errors="coerce")
                prod = prod.sort_values(["商品id", "日期"])

                # 趋势图
                for sku_id in prod["商品id"].unique()[:3]:
                    sku_data = prod[prod["商品id"] == sku_id]
                    if not sku_data.empty and "implied_r" in sku_data.columns:
                        fig_data = sku_data[["日期", "implied_r"]].dropna()
                        if not fig_data.empty:
                            st.write(f"**SKU {sku_id}** 隐含抽佣率趋势")
                            st.line_chart(fig_data.set_index("日期")["implied_r"])

        # 新品上架标记
        if "是否当日上架" in prod.columns:
            new_listings = prod[prod["是否当日上架"] == 1]
            if not new_listings.empty:
                st.info(f"发现 {len(new_listings)} 条新品上架记录")
    else:
        st.info("暂无商品信息数据")
