# coding:utf8
"""workers.cr_trail -- CR试验商品配置 ETL 应用层

调用 workers.lib 通用功能层，实现 CR试验商品配置数据的 ETL 处理。
本模块是 CR试验商品配置 业务逻辑的唯一注入点，lib 不依赖本模块。

使用方式：
    from workers.cr_trail import run_cr_trail_pipeline
    run_cr_trail_pipeline()
"""

from .main import run_cr_trail_pipeline

__all__ = ["run_cr_trail_pipeline"]
