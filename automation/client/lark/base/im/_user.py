#coding:utf8
"""Lark Instant Messaging (IM) Message Template.
"""
import json
from abc import ABC, abstractmethod


class BaseUser(ABC):
    """ Lark IM User Template.
    """
    def __init__(self, user_id: str = None, open_id: str = None, union_id: str = None, user_name: str = None, tenant_key: str = None):
        self.user_id = user_id
        self.open_id = open_id
        self.union_id = union_id
        self.user_name = user_name
        self.tenant_key = tenant_key
        self._style = None


    @property
    def style(self):
        if isinstance(self._style, str):
            return [self._style]
        elif isinstance(self._style, (list, tuple)):
            return list(self._style)
        return []
    
    @style.setter
    def style(self, value):
        types = ("bold", "italic", "underline", "linethrough")
        
        if isinstance(value, str) and value.lower() in types:
            self._style = [value]
        elif isinstance(value, (list, tuple)) \
             and all(isinstance(v, str) and v.lower() in types for v in value):
            self._style = list(value)
        else:
            self._style = None
            
            

    @abstractmethod
    def __str__(self):
        NotImplemented


    @abstractmethod
    def to_rich_text(self):
        NotImplemented
    

    @abstractmethod
    def to_json(self):
        NotImplemented


class AllUser(BaseUser):
    """ Lark IM All User Template.
    """
    def __init__(self):
        super().__init__(user_id="all", user_name="所有人")
        
    
    def __str__(self):
        return "at所有人<at user_id='all'>所有人</at>"
    
    
    def __repr__(self):
        return f"<AllUser user_id='{self.user_id}' user_name={self.user_name} at {hex(id(self))}>"
    
    
    def __format__(self, format_spec):
        if format_spec == "repr":
            return self.__repr__()
        elif format_spec in ("md", "markdown", "rich", "rich_text"):
            return self.to_rich_text()
        return super().__format__(format_spec)
    
    
    def to_rich_text(self):
        return {
            "tag": "at",
            "user_id": self.user_id,
            "user_name": self.user_name
        }
       
     
    def to_json(self):
        return json.dumps(self.to_rich_text(), ensure_ascii=False)

class User(BaseUser):
    """ Lark IM User Template.
    """
    def __init__(
        self, user_id: str = None, open_id: str = None, union_id: str = None,
        user_name: str = None, tenant_key: str = None, email: str = None
    ):
        super().__init__(
            user_id=user_id, open_id=open_id, union_id=union_id, user_name=user_name, 
            tenant_key=tenant_key
        )
        self._email = email
        
    @property
    def email(self):
        return self._email
    
    @email.setter
    def email(self, value):
        if isinstance(value, str) and "@" in value:
            self._email = value
        else:
            self._email = None
    
    def __str__(self):
        return "<at user_id='{user_id}'>{name}</at>".format(
            user_id=self.user_id, name=self.user_name
        )


    def __repr__(self):
        return f"<User user_id='{self.user_id}' user_name={self.user_name} at {hex(id(self))}>"
    
    
    def __format__(self, format_spec):
        if format_spec == "repr":
            return self.__repr__()

        elif format_spec in ("md", "markdown", "rich", "rich_text"):
            return self.to_rich_text()
        return super().__format__(format_spec)


    def to_rich_text(self):
        return {
            "tag": "at",
            "user_id": self.user_id,
            "user_name": self.user_name
        }


    def to_json(self):
        return json.dumps(self.to_rich_text(), ensure_ascii=False)
    