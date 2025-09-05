#coding:utf8
"""Lark Client Common Module.

Management Lark Request Server
"""

from enum import Enum

from ....conf import lark

if lark.has_section("prod"):
    _lark_host = lark.get("prod", "lark_host")
elif lark.has_section("dev"):
    _lark_host = lark.get("dev", "lark_host")

else:
    _lark_host = None

if _lark_host is None:
    _lark_host = "https://open.feishu.cn"


class LarkImURL(Enum):
    """ Lark Instant Message API URL.
    """
    UPLOAD_IMAGE = f"{_lark_host}/open-apis/im/v1/images"
    UPLOAD_FILE = f"{_lark_host}/open-apis/im/v1/files"
    
    
    
    

__all__ = [
    "LarkImURL"
]