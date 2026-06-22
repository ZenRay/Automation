# coding:utf8
"""Tab: 功效分析页面"""

import pandas as pd
import streamlit as st


def render(data: dict[str, pd.DataFrame]):
    power_df = data.get("power_analysis", pd.DataFrame())

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
        st.dataframe(formatted, use_container_width=True)

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

        st.dataframe(verify_df, use_container_width=True)

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
