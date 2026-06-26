# coding:utf8
"""Tab: 功效分析页面"""

import pandas as pd
import streamlit as st


def render(data: dict[str, pd.DataFrame]):
    power_df = data.get("power_analysis", pd.DataFrame())
    cross_df = data.get("power_cross_correlation", pd.DataFrame())

    if power_df.empty:
        st.warning(
            "暂无功效分析结果。请先运行功效分析：\n\n"
            "```bash\npython -m workers.cr_analyze.main --power\n```"
        )
        return

    st.subheader("σ / ρ 计算结果")

    # σ/ρ 表格
    display_cols = [
        c
        for c in [
            "sku_id",
            "sigma_raw",
            "sigma_adjusted",
            "rho_pre",
            "rho_post",
            "rho_main",
        ]
        if c in power_df.columns
    ]
    if display_cols:
        formatted = power_df[display_cols].copy()
        for col in ["sigma_raw", "sigma_adjusted", "rho_pre", "rho_post", "rho_main"]:
            if col in formatted.columns:
                formatted[col] = formatted[col].apply(
                    lambda x: f"{x:.4f}" if pd.notna(x) else "—"
                )
        zh_cols = {
            "sku_id": "商品ID",
            "sigma_raw": "σ_raw",
            "sigma_adjusted": "σ_adjusted",
            "rho_pre": "ρ_pre",
            "rho_post": "ρ_post",
            "rho_main": "ρ_main",
        }
        st.dataframe(formatted.rename(columns=zh_cols), use_container_width=True)

    # 参考值对比
    st.subheader("参考值对比")
    ref_text = (
        "| SKU | σ_raw 参考 | ρ 参考 |\n"
        "|-----|-----------|--------|\n"
        "| 10184690 | 0.194 | 0.993 |\n"
        "| 20519020 | — | — |\n"
        "| 20588413 | — | — |\n"
    )
    st.markdown(ref_text)

    # 功效验证
    st.subheader("功效验证")
    if "n_required" in power_df.columns and "n_actual" in power_df.columns:
        verify_df = power_df[
            [
                c
                for c in [
                    "sku_id",
                    "sigma_adjusted",
                    "rho_main",
                    "n_required",
                    "n_actual",
                    "power_sufficient",
                    "fallback",
                    "n_weeks_available",
                ]
                if c in power_df.columns
            ]
        ].copy()

        if "power_sufficient" in verify_df.columns:
            verify_df["结论"] = verify_df["power_sufficient"].apply(
                lambda x: "✅ 功效充足" if x else "❌ 功效不足"
            )
        if "n_required" in verify_df.columns:
            verify_df["n_required"] = verify_df["n_required"].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else "—"
            )
        if "fallback" in verify_df.columns:
            verify_df["数据来源"] = verify_df["fallback"].apply(
                lambda x: "回退(归一化预备期+摸底期)" if bool(x) else "历史基线4周"
            )

        keep_cols = [
            c
            for c in [
                "sku_id",
                "sigma_adjusted",
                "rho_main",
                "n_required",
                "n_actual",
                "n_weeks_available",
                "数据来源",
                "结论",
            ]
            if c in verify_df.columns
        ]
        zh_cols = {
            "sku_id": "商品ID",
            "sigma_adjusted": "σ_adjusted",
            "rho_main": "ρ_main",
            "n_required": "所需样本组数(n_required)",
            "n_actual": "实际样本组数(n_actual)",
            "n_weeks_available": "可用周数",
        }
        st.dataframe(verify_df[keep_cols].rename(columns=zh_cols), use_container_width=True)

    # 回退提示
    if "fallback" in power_df.columns and power_df["fallback"].astype(bool).any():
        fb_skus = power_df[power_df["fallback"].astype(bool)]["sku_id"].astype(str).tolist()
        st.warning(
            "以下SKU历史基线有效周不足(<3)，已回退使用“归一化预备期+摸底期”数据："
            + ", ".join(fb_skus)
            + "。该结果置信度较标准历史基线方案更低。"
        )

    # SKU 交叉相关
    st.subheader("SKU 交叉相关分析")
    if not cross_df.empty and {"sku_a", "sku_b", "rho"}.issubset(cross_df.columns):
        corr_disp = cross_df.copy()
        corr_disp["rho"] = corr_disp["rho"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "—")
        if "risk_flag" in corr_disp.columns:
            corr_disp["风险提示"] = corr_disp["risk_flag"].apply(
                lambda x: "有效样本量存在打折风险" if bool(x) else "正常"
            )
        keep_cols = [c for c in ["sku_a", "sku_b", "rho", "风险提示"] if c in corr_disp.columns]
        st.dataframe(
            corr_disp[keep_cols].rename(
                columns={"sku_a": "商品ID_A", "sku_b": "商品ID_B", "rho": "相关系数ρ"}
            ),
            use_container_width=True,
        )

        matrix_src = cross_df[["sku_a", "sku_b", "rho"]].copy()
        sku_ids = sorted(
            set(matrix_src["sku_a"].dropna().astype(int).tolist())
            | set(matrix_src["sku_b"].dropna().astype(int).tolist())
        )
        if sku_ids:
            mat = pd.DataFrame(1.0, index=sku_ids, columns=sku_ids)
            for _, row in matrix_src.iterrows():
                if pd.notna(row["rho"]):
                    a = int(row["sku_a"])
                    b = int(row["sku_b"])
                    mat.loc[a, b] = float(row["rho"])
                    mat.loc[b, a] = float(row["rho"])
            st.write("**SKU 相关系数矩阵**")
            st.dataframe(mat.round(4), use_container_width=True)

        if "risk_flag" in cross_df.columns and cross_df["risk_flag"].astype(bool).any():
            risky = cross_df[cross_df["risk_flag"].astype(bool)]
            pairs = [f"({int(r['sku_a'])}, {int(r['sku_b'])})" for _, r in risky.iterrows()]
            st.warning("检测到高相关SKU对（ρ>0.5）: " + ", ".join(pairs) + "。有效样本量存在打折风险。")
        else:
            st.success("当前SKU两两相关性未超过0.5，未见明显样本量打折风险。")
    else:
        st.info("暂无SKU交叉相关结果。请先执行功效分析CLI刷新数据。")

    # 解读结论
    st.subheader("分析结论")
    if "power_sufficient" in power_df.columns:
        all_sufficient = power_df["power_sufficient"].all()
        if all_sufficient:
            st.success(
                "所有 SKU 的功效验证均通过（n_required ≤ n_actual = 2），"
                "试验设计具有 ≥ 80% 的统计功效来检测 10% 的效应量 (MDE)。"
            )
        else:
            insufficient = power_df[~power_df["power_sufficient"]]
            st.warning(
                f"以下 SKU 功效不足: {', '.join(insufficient['sku_id'].astype(str).tolist())}。"
                "建议增大 MDE 或标注结论的不确定性。"
            )
