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

        
        
class LarkSheetException(Exception):
    def __init__(self, msg=None):
        self.msg = msg

    def __str__(self) -> str:
        return "{}".format(self.msg)



class LarkMessageException(Exception):
    def __init__(self, msg=None, code=0):
        self.msg = msg
        self.code = code
        
        
        qps_reference = "https://open.feishu.cn/document/ukTMukTMukTM/uUzN04SN3QjL1cDN"
        if self.code != 0:
            self._appendix_msg = " (code: {})".format(self.code)
            
        elif self.code == 230020:

            self._appendix_msg = " (code: {}, 发送消息（V1）接口触发群维度的发消息限流 参考: {})".format(self.code, qps_reference)
        elif self.code == 11232:
            self._appendix_msg = " (code: {}, 发消息接口（V4）触发消息系统内部的整体限流)".format(self.code)
        elif self.code == 11233:
            self._appendix_msg = " (code: {}, 发消息接口（V4）触发群维度的发消息限流 参考: {})".format(self.code, qps_reference)
        elif self.code == 11247:
            self._appendix_msg = " (code: {}, 批量发送消息触发每日额度限制)".format(self.code)
        elif self.code == 99991400:
            self._appendix_msg = " (code: {}, 触发接口频控，请稍后再试 参考: {})".format(self.code, qps_reference)
        else:
            self._appendix_msg = ""

    def __str__(self) -> str:
        return "{}{}".format(self._appendix_msg, self.msg)