# coding:utf8
"""workers.base_data -- 基础数据 ETL 模块

长沙项目门店基础数据同步（SQL-to-SQL ETL）：
SQL 在 MaxCompute 内部完成 提取 → 转换 → 写入 全流程，
Python 侧仅负责参数渲染与任务触发。
"""
