# coding:utf8
"""Workers package - 数据处理任务集合"""

import logging

# 配置 workers.* logger，确保 workers.lib.* 的 INFO 日志可见
# （之前只有 automation logger 有 handler，workers 日志会丢失）
logger = logging.getLogger("workers")
logger.setLevel(logging.DEBUG)

_formatter = logging.Formatter(
    fmt="[%(levelname)s]:%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_handler = logging.StreamHandler()
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
