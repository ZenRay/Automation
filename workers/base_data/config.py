# coding:utf8
"""workers.base_data.config -- 基础数据 ETL 专属配置实例

本文件是长沙项目门店基础数据任务的唯一「业务知识注入点」，
所有 SQL 文件名、目标表名均在此定义，供 main.py 使用。

任务性质说明：
    a3_store.sql 是 SQL-to-SQL ETL（INSERT OVERWRITE 写入 MaxCompute
    目标表分区），无结果集返回、不涉及飞书写入。因此本模块：
      - 使用 SQLQueryConfig(execute_only=True) 仅触发执行
      - 不定义 LarkTargetConfig / DataRoute

lib 层不引用本文件，保证 lib 的通用性。
"""

from pathlib import Path

from workers.lib import SQLQueryConfig

# --------------------------------------------------------------------------
# SQL 文件目录：基础数据专属 SQL 文件存放位置
# --------------------------------------------------------------------------
SQL_BASE_DIR = Path(__file__).parent / "sql"

# --------------------------------------------------------------------------
# MaxCompute 目标表（SQL 内 INSERT OVERWRITE 写入该表的 dt 分区）
# --------------------------------------------------------------------------
TARGET_TABLE = "changsha_project_store_info_daily_asc"

# --------------------------------------------------------------------------
# MaxCompute SQL 查询配置
#
# a3_store.sql 仅接受两个模板参数：
#   ${date_param}   基准日期表达式（由 DateRangeParams.sql_params() 渲染）
#   ${end_offset}   结束偏移量（目标分区 dt = DATEADD(date_param, end_offset)）
# --------------------------------------------------------------------------
SQL_QUERIES: list[SQLQueryConfig] = [
    SQLQueryConfig(
        name="a3_store",
        sql_file="a3_store.sql",
        depends_on=[],
        execute_only=True,  # INSERT OVERWRITE 无结果集，仅执行不下载
    ),
]
