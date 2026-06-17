# coding:utf8
"""workers.okr -- OKR 数据处理应用层

调用 workers.lib 通用功能层，实现 OKR 双月数据的 ETL 处理。
本模块是 OKR 业务逻辑的唯一注入点，lib 不依赖本模块。

使用方式：
    from workers.okr import run_okr_pipeline
    run_okr_pipeline()
"""

from .main import run_okr_pipeline

__all__ = ["run_okr_pipeline"]
