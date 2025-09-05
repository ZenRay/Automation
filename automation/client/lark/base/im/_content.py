#coding:utf8
"""Message Content for Lark IM.
"""

import json

from ._user import BaseUser
        
        
class MentionUser:
    """Mention User
    """

    def __init__(self, user: BaseUser, key: int = 0):
        """Initialize MentionUser.

        Args:
            user (BaseUser): The user to be mentioned.

        """
        if not isinstance(user, BaseUser):
            raise ValueError("user must be an instance of BaseUser or its subclass.")
        self.user = user
        self._key = key
    


    @property
    def id(self):
        """Open ID"""
        return self.user.open_id

    @id.setter
    def id(self, value):
        self.user.open_id = value
    
    @property
    def name(self):
        """User Name"""
        return self.user.user_name


    @name.setter
    def name(self, value):
        self.user.user_name = value

    @property
    def key(self):
        """Key"""
        return f"@_user_{self._key}"

    @key.setter
    def key(self, value):
        if not isinstance(value, int) or value < 0:
            raise ValueError("key must be a non-negative integer.")
        
        self._key = value
        
    @property
    def tenant_key(self):
        """Tenant Key"""
        return self.user.tenant_key
    
    @tenant_key.setter
    def tenant_key(self, value):
        self.user.tenant_key = value

    def to_dict(self):
        """Convert the mention user to a dictionary representation.

        Returns:
            dict: The dictionary representation of the mention user.

        """
        return {
            "key": self.key,
            "id": self.id,
            "name": self.name,
            "tenant_key": self.tenant_key
        }

    def from_config(self, config: dict):
        """Load mention user information from a configuration dictionary.

        Args:
            config (dict): The configuration dictionary containing mention user information.

        """
        self.id = config.get("id", self.id)
        self.name = config.get("name", self.name)
        self.tenant_key = config.get("tenant_key", self.tenant_key)
        self.key = config.get("key", self._key)
        
    
    def to_json(self):
        """Convert the mention user to a JSON representation.

        Returns:
            str: The JSON representation of the mention user.
        """
        return json.dumps(self.to_dict(), ensure_ascii=False)
    
        
    def to_rich_text(self):
        """Convert the mention user to a rich text representation.

        Returns:
            dict: The rich text representation of the mention user.

        """
        return self.to_json()

    
    
        
class ContentItem:
    """Content Item
    """
    
    MSG_TYPES = (
        "text"
        ,"post"
        ,"image"
        ,"file"
        ,"audio"
        ,"media"
        ,"sticker"
        ,"interactive"
        ,"share_chat"
        ,"share_user"
        ,"system"
    )
    def __init__(
        self, message_id: str = None, chat_id: str = None, root_id: str = None,
        parent_id: str = None, msg_type: str = "text", create_time: int = None,
        update_time: int = None, sender: BaseUser = None, deleted: bool = False,
        updated: bool = False, thread_id: str = None, upper_message_id: str = None,
        mentions: list[MentionUser] = None
    ):
        """Initialize ContentItem.

        Args:
            message_id (str, optional): The ID of the message this content item belongs to.
            chat_id (str, optional): The ID of the chat this content item belongs to.
                If 
            root_id (str, optional): The ID of the root message in the thread.
            parent_id (str, optional): The ID of the parent message in the thread.
            msg_type (str, optional): The type of the message content. Defaults to "text
            create_time (int, optional): The creation time of the message content in milliseconds since epoch.
            update_time (int, optional): The last update time of the message content in milliseconds since epoch.
            sender (Sender, optional): The sender of the message content.
            deleted (bool, optional): Whether the message content has been deleted. Defaults to False.
            updated (bool, optional): Whether the message content has been updated. Defaults to False.
            thread_id (str, optional): The ID of the thread this message content belongs to.
            upper_message_id (str, optional): The ID of the upper message in the thread.
            mentions (list[MentionUser], optional): A list of users mentioned in the message content.
        """
        self.message_id = message_id
        self.chat_id = chat_id
        self.root_id = root_id
        self.parent_id = parent_id
        self._msg_type = msg_type
        self.create_time = create_time
        self.update_time = update_time
        self.sender = sender
        self.deleted = deleted
        self.updated = updated
        self.thread_id = thread_id
        self.upper_message_id = upper_message_id
        self.mentions = mentions or []
        
    
    @property
    def msg_type(self):
        """Message Type"""
        return self._msg_type
    
    @msg_type.setter
    def msg_type(self, value):
        """Message Type"""
        if value in self.MSG_TYPES:
            self._msg_type = value
        else:
            raise ValueError(f"Invalid msg_type. Must be one of [{self.MSG_TYPES}].")

    def to_dict(self):
        """Convert the content item to a dictionary representation.

        Returns:
            dict: The dictionary representation of the content item.

        """
        return {
            "message_id": self.message_id
        }
        
    
    def to_dict(self):
        """Convert the content item to a dictionary representation.

        Returns:
            dict: The dictionary representation of the content item.

        """
        keys = ["message_id", "chat_id", "root_id", "parent_id", "msg_type",
               "create_time", "update_time", "sender", "deleted", "updated",
               "thread_id", "upper_message_id", "mentions"]
        list_keys = ["mentions"]

        base_dict = {}
        for key in keys:
            if hasattr(self, key):
                value = getattr(self, key)
                if key in list_keys and isinstance(value, list):
                    value = [item.to_dict() for item in value]
                else:
                    value = []
                base_dict[key] = value

        return base_dict
    

    
    def from_config(self, config: dict):
        """Load content item information from a configuration dictionary.

        Args:
            config (dict): The configuration dictionary containing content item information.

        """
        self.message_id = config.get("message_id", self.message_id)
        self.chat_id = config.get("chat_id", self.chat_id)
        self.root_id = config.get("root_id", self.root_id)
        self.parent_id = config.get("parent_id", self.parent_id)
        self.msg_type = config.get("msg_type", self.msg_type)
        self.create_time = config.get("create_time", self.create_time)
        self.update_time = config.get("update_time", self.update_time)
        
        # TODO: fix sender type
        # if "sender" in config and isinstance(config["sender"], dict):
            # if self.sender is None:
            #     self.sender = Sender()
            # self.sender.from_config(config["sender"])
            
        self.deleted = config.get("deleted", self.deleted)
        self.updated = config.get("updated", self.updated)
        self.thread_id = config.get("thread_id", self.thread_id)
        self.upper_message_id = config.get("upper_message_id", self.upper_message_id)
        if "mentions" in config and isinstance(config["mentions"], list):
            self.mentions = []
            for idx, mention_cfg in enumerate(config["mentions"]):
                if isinstance(mention_cfg, dict):
                    mention_user = MentionUser(user=None, key=idx)
                    mention_user.from_config(mention_cfg)
                    self.mentions.append(mention_user)

        
    def to_json(self):
        """Convert the content item to a JSON representation.

        Returns:
            str: The JSON representation of the content item.
        """
        return json.dumps(self.to_dict(), ensure_ascii=False)
    
    

class SendContentItem(ContentItem):
    """Send Content Item
    """
    RECEIVER_ID_TYPES = ("open_id", "union_id", "user_id", "user_id", "email", "chat_id")

    def __init__(
        self, message_id: str = None, chat_id: str = None, root_id: str = None,
        parent_id: str = None, msg_type: str = "text", create_time: int = None,
        update_time: int = None, sender: BaseUser = None, deleted: bool = False,
        updated: bool = False, thread_id: str = None, upper_message_id: str = None,
        mentions: list[MentionUser] = None, content: dict = None, receiver: BaseUser = None
    ):
        """Initialize SendContentItem.

        Args:
            content (dict): send message content

        """
        super().__init__(
            message_id=message_id, chat_id=chat_id, root_id=root_id,
            parent_id=parent_id, msg_type=msg_type, create_time=create_time,
            update_time=update_time, sender=sender, deleted=deleted,
            updated=updated, thread_id=thread_id,
            upper_message_id=upper_message_id, mentions=mentions
        )
        
        self._content = content
        if isinstance(receiver, BaseUser):
            self._receiver = receiver
        else:
            raise ValueError("receiver must be an instance of User or its subclass.")

        
    @property
    def content(self):
        """Content"""
        if self._content is not None:
            return json.dumps(self._content, ensure_ascii=False)
        
        return None
        
    @content.setter
    def content(self, value):
        if isinstance(value, dict) \
                and all([key not in value for key in ["zn_cn", "en_us"]]) \
                and "text" in value and self.msg_type == "text":
            self._content = value
        elif isinstance(value, dict) \
                and any([key in value for key in ["zn_cn", "en_us"]]) \
                and self.msg_type == "post":
            self._content = value
        elif isinstance(value, dict) and "image_key" in value \
                and self.msg_type == "image":
            self._content = value
        # Interactive Card Content
        elif isinstance(value, dict) and (
                value.get("type", "") in ("card", "template") or "schema" in value
                ) \
                and self.msg_type == "interactive":
            self._content = value
        # Share Chat Content
        elif isinstance(value, dict) and "chat_id" in value and self.msg_type == "share_chat":
            self._content = value
        # Share User Content
        elif isinstance(value, dict) and "user_id" in value and self.msg_type == "share_user":
            self._content = value
        # Audio, media, file, sticker Content
        elif isinstance(value, dict) and "file_key" in value and self.msg_type in ("audio", "media", "file", "sticker"):
            self._content = value
        elif isinstance(value, dict) and value.get("type", "") == "driver" and self.msg_type in ("system",):
            self._content = value
        elif not isinstance(value, dict):
            raise ValueError("Content must be a dictionary.")
        else:
            raise ValueError("Invalid content format, please check the msg_type and content structure.")

    @property
    def receiver(self):
        """Receiver"""
        return self._receiver
    
    @receiver.setter
    def receiver(self, value):
        if not isinstance(value, BaseUser):
            raise ValueError("Receiver must be an instance of User or its subclass.")
        self._receiver = value

    def to_dict(self):
        """Convert the send content item to a dictionary representation.

        Returns:
            dict: The dictionary representation of the send content item.

        """
        base_dict = super().to_dict()
        base_dict["content"] = json.dumps(self.content if self.content else {}, ensure_ascii=False)
        return base_dict
    


    def from_config(self, config: dict):
        """Load send content item information from a configuration dictionary.

        Args:
            config (dict): The configuration dictionary containing send content item information.

        """
        super().from_config(config)
        if "content" in config and isinstance(config["content"], dict):
            self.content = config["content"]
        elif "content" in config and isinstance(config["content"], str):
            try:
                content_dict = json.loads(config["content"])
                if isinstance(content_dict, dict):
                    self.content = content_dict
            except json.JSONDecodeError:
                pass
            
    
    def to_json(self):
        return super().to_json()
    
    
    
    def to_rich_text(self):
        return self.to_json()


    def send_message(self, func, receive_id_type, *, content, **kwargs):
        """Send Message

        Args:
            func (callable): The function to send the message.
            **kwargs: Additional keyword arguments to pass to the send function.

        Returns:
            dict: The response from the send function.

        """
        if not callable(func):
            raise ValueError("func must be a callable function.")
        
        uuid = kwargs.pop("uuid", None)
        
        receiver_id = None
        if receive_id_type not in self.RECEIVER_ID_TYPES:
            raise ValueError(f"Invalid receive_id_type. Must be one of {self.RECEIVER_ID_TYPES}.")
        elif receive_id_type == "open_id" and self.receiver.open_id is not None:
            receiver_id = self.receiver.open_id
        elif receive_id_type == "union_id" and self.receiver.union_id is not None:
            receiver_id = self.receiver.union_id
        elif receive_id_type == "user_id" and self.receiver.user_id is not None:
            receiver_id = self.receiver.user_id
        elif receive_id_type == "email" and self.receiver.user_name is not None:
            receiver_id = self.receiver.email
        elif receive_id_type == "chat_id" and self.chat_id is not None:
            receiver_id = self.chat_id
        else:
            raise ValueError(f"Receiver does not have a valid ID for receive_id_type '{receive_id_type}'.")
        
        
        if content is not None and isinstance(content, dict):
            self.content = content
        else:
            raise ValueError("Content must be provided as a dictionary.")

        if self.content is  None:
            raise ValueError("Content is not set or invalid.")
        

        result = func(
            receive_id_type=receive_id_type,
            content=self.content,
            msg_type=self.msg_type,
            receive_id=receiver_id,
            uuid=uuid,
            **kwargs
        )
        return result