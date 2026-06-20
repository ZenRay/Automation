# coding:utf8
"""workers.cr_trail.transformer -- CR试验商品配置 数据转换

本文件是 CR试验商品配置 数据转换逻辑的唯一注入点。

与 OKR 模块的区别：
  - OKR 需要多源融合（飞书 + MaxCompute merge），转换逻辑复杂
  - CR试验商品配置 仅从 MaxCompute 提取，SQL 已产出最终字段，无需 merge
  - 转换器仅保留扩展点，当前为 pass-through

lib 的 DataTransformer 本身不含任何业务知识，
本文件是 CR试验商品 数据转换逻辑的唯一注入点。
"""

import logging

import pandas as pd

from workers.lib import DataTransformer

logger = logging.getLogger("workers.cr_trail.transformer")


def build_cr_trail_transformer() -> DataTransformer:
    """构建并返回注册了 CR试验商品配置 业务步骤的 DataTransformer 实例

    当前为 pass-through（SQL 已产出最终字段，无需额外转换）。
    如需后续增加清洗步骤，在此注册即可：
        t.register_step("my_step", my_clean_func)

    使用方式：
        transformer = build_cr_trail_transformer()
        result_df = transformer.transform(df)
    """
    t = DataTransformer()
    # 当前无需注册步骤，SQL 已产出最终所需字段
    logger.info(f"CR trail transformer built with {len(t._steps)} registered step(s)")
    return t
