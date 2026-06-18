# coding:utf8
"""workers.lib.transformer -- DataTransformer 管道基础设施

提供通用的数据融合与清洗管道。
本模块本身不注册任何业务步骤，只是一个步骤执行器。
具体业务步骤由应用层（如 workers.okr.transformer）负责注册。

设计要点：
- merge() 支持从多个数据源（飞书 / MaxCompute）中选取两个 DataFrame 进行合并
- transform() 按注册顺序依次执行清洗步骤，每步可观察和修改 DataFrame
- 所有步骤均带日志，便于调试和审计
"""

import logging
from typing import Callable

import pandas as pd

logger = logging.getLogger("workers.lib.transformer")


class DataTransformer:
    """通用数据融合与清洗管道

    使用方式（应用层）：
        t = DataTransformer()
        t.register_step("drop_nulls", lambda df: df.dropna(subset=["key"]))
        t.register_step("fill_defaults", _fill_defaults)

        merged = t.merge(lark_data, mc_data, merge_config)
        result = t.transform(merged)
    """

    def __init__(self):
        self._steps: list[tuple[str, Callable[[pd.DataFrame], pd.DataFrame]]] = []

    # ------------------------------------------------------------------
    # 步骤注册
    # ------------------------------------------------------------------

    def register_step(
        self,
        name: str,
        func: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> "DataTransformer":
        """注册一个清洗/转换步骤

        Args:
            name: 步骤名称，用于日志标识
            func: 转换函数，签名 (df: DataFrame) -> DataFrame

        Returns:
            self，支持链式调用：t.register_step(...).register_step(...)
        """
        self._steps.append((name, func))
        logger.debug(f"Registered transform step: {name}")
        return self

    # ------------------------------------------------------------------
    # 融合
    # ------------------------------------------------------------------

    def merge(
        self,
        lark_data: dict[str, pd.DataFrame],
        mc_data: dict[str, pd.DataFrame],
        merge_config: dict,
    ) -> pd.DataFrame:
        """从多个数据源中选取两个 DataFrame 进行合并

        merge_config 格式：
        {
            "left":    "lark:<source_name>" 或 "mc:<query_name>",
            "right":   "lark:<source_name>" 或 "mc:<query_name>",
            "how":     "left" | "right" | "inner" | "outer" | "cross",
            "on":      "<公共字段名>",           # 左右同名字段时使用
            # 或
            "left_on":  "<左表字段名>",
            "right_on": "<右表字段名>",
            # 可选
            "suffixes": ("_lark", "_mc"),
        }

        Args:
            lark_data:    {source.name -> DataFrame} 飞书数据源字典
            mc_data:      {query.name -> DataFrame} MaxCompute 查询结果字典
            merge_config: 合并配置字典

        Returns:
            pd.DataFrame：合并后的 DataFrame

        Raises:
            KeyError: 数据源名称不存在
            ValueError: merge_config 格式错误
        """
        left_ref = merge_config.get("left")
        right_ref = merge_config.get("right")
        how = merge_config.get("how", "left")
        suffixes = tuple(merge_config.get("suffixes", ("_lark", "_mc")))

        if not left_ref or not right_ref:
            raise ValueError(
                f"merge_config must contain 'left' and 'right' keys. Got: {list(merge_config.keys())}"
            )

        # 解析数据源引用
        left_df = self._resolve_data_ref(left_ref, lark_data, mc_data)
        right_df = self._resolve_data_ref(right_ref, lark_data, mc_data)

        logger.info(
            f"Merging: left='{left_ref}' ({left_df.shape}) "
            f"right='{right_ref}' ({right_df.shape}) how='{how}'"
        )

        # 构建 pandas merge 参数
        merge_kwargs = {"how": how, "suffixes": suffixes}

        if "on" in merge_config:
            merge_kwargs["on"] = merge_config["on"]
        elif "left_on" in merge_config and "right_on" in merge_config:
            merge_kwargs["left_on"] = merge_config["left_on"]
            merge_kwargs["right_on"] = merge_config["right_on"]
        else:
            raise ValueError(
                "merge_config must contain either 'on' or both 'left_on' and 'right_on'"
            )

        result = pd.merge(left_df, right_df, **merge_kwargs)
        logger.info(f"Merge result: {result.shape}")
        return result

    # ------------------------------------------------------------------
    # 转换
    # ------------------------------------------------------------------

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """依次执行所有已注册的清洗/转换步骤

        Args:
            df: 输入 DataFrame

        Returns:
            pd.DataFrame：经过所有步骤处理后的 DataFrame
        """
        if not self._steps:
            logger.info("No transform steps registered, returning input as-is")
            return df

        logger.info(f"Starting transform pipeline with {len(self._steps)} steps")
        for name, step in self._steps:
            before_shape = df.shape
            try:
                df = step(df)
                logger.info(
                    f"Step '{name}' completed: {before_shape} -> {df.shape}"
                )
            except Exception as e:
                logger.error(f"Step '{name}' failed: {e}")
                raise

        logger.info(f"Transform pipeline completed. Final shape: {df.shape}")
        return df

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_data_ref(
        ref: str,
        lark_data: dict[str, pd.DataFrame],
        mc_data: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """解析数据源引用字符串，返回对应的 DataFrame

        ref 格式：
          "lark:<source_name>" -> 从 lark_data 中查找
          "mc:<query_name>"    -> 从 mc_data 中查找

        Raises:
            ValueError: ref 格式错误
            KeyError:   数据源名称不存在
        """
        if ":" not in ref:
            raise ValueError(
                f"Data reference must be in format 'lark:<name>' or 'mc:<name>'. Got: '{ref}'"
            )

        source_type, source_name = ref.split(":", 1)

        if source_type == "lark":
            if source_name not in lark_data:
                raise KeyError(
                    f"Lark source '{source_name}' not found. "
                    f"Available: {list(lark_data.keys())}"
                )
            return lark_data[source_name]

        elif source_type == "mc":
            if source_name not in mc_data:
                raise KeyError(
                    f"MaxCompute query '{source_name}' not found. "
                    f"Available: {list(mc_data.keys())}"
                )
            return mc_data[source_name]

        else:
            raise ValueError(
                f"Unknown data source type '{source_type}'. Must be 'lark' or 'mc'."
            )
