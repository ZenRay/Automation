#coding:utf8
"""Exceptions Class"""

class RegexException(Exception):
    """Regular Expession Exception"""

    

class LarkException(Exception):
    def __init__(self, code=0, msg=None):
        self.code = code
        self.msg = msg

    def __str__(self) -> str:
        return "{}:{}".format(self.code, self.msg)