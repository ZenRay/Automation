#coding:utf8
"""
Lark Instant Message Base Module
* message: Multi-type Message Class

"""

from ._message import (
    Message,
    ImageMessage,
    FileMessage,
    TextMessage,
    StaticInteractiveMessage
)


from ._content import (
    ContentItem, SendContentItem
)


from ._user import (
    AllUser,
    User
)