#coding:utf8
"""Instant Messaging (IM) base classes for Lark.

Main function is used to deal with IM messages.
"""

import logging
import json
from abc import ABC, abstractmethod
from enum import Enum
from os import path



from ...exceptions import LarkMessageException


from .....utils import check_function_arg

from ...common import MIMEType

logger = logging.getLogger("automation.lark.base.im.message")

class Message(ABC):
    """ Lark IM messages.
    """
    def __init__(self, message_type: str = None):
        self._message_key = None
        self._msg_type = message_type

    @property
    @abstractmethod
    def file_type(self):
        """File Type Property"""
        NotImplemented
    
    @file_type.setter
    @abstractmethod
    def file_type(self, value):
        """Set File Type Property"""
        NotImplemented
        
        

    @property
    @abstractmethod
    def file_name(self):
        """File Name Property"""
        NotImplemented
        
    
    
    @file_name.setter
    @abstractmethod
    def file_name(self, value):
        """Set File Name Property"""
        NotImplemented
        
    @property
    @abstractmethod
    def msg_type(self):
        """Message Type Property"""
        NotImplemented

    @msg_type.setter
    @abstractmethod
    def msg_type(self, value):
        """Set Message Type Property"""
        NotImplemented

    @property
    def is_raw(self):
        """Check Message Whether Raw"""
        return self.message_key is None

    @property
    def message_key(self):
        """Message Key Property"""
        return self._message_key

    @message_key.setter
    def message_key(self, value):
        """Set Message Key Property"""
        self._message_key = value

    @property
    def is_can_upload(self):
        """Check Message Whether Can Upload
        
        Check whether the message can be uploaded, which is file exits and 
            don't uploaded before.
        """
        if hasattr(self, "_file"):
            return self.is_raw and self._file is not None
        else:
            return False


    @property
    def file(self):
        """Public proxy for the protected `_file` attribute."""
        return getattr(self, "_file", None)

    @file.setter
    def file(self, value):
        setattr(self, "_file", value)

    @abstractmethod
    def check_validate(self, *args, **kwargs):
        """Check Message Validate"""
        raise NotImplementedError
    
    
    @abstractmethod
    def upload_file(self, *args, **kwargs):
        """Upload File With API"""
        raise NotImplementedError
    
    @abstractmethod
    def send_message(self, *args, **kwargs):
        """Send Message With API"""
        raise NotImplementedError

    def _extract_file_info(self, file: str):
        """Extract File Name from File Path

        Extract filename and extension from file path. The file name contains extension type.
        
        Args:
            file (str): The file path.
        Returns:
            str: The file name.
        """
        if not path.exists(file):
            raise FileNotFoundError("File not found: {}".format(file))
        
        if not (path.isfile(file) and not path.islink(file)):
            raise ValueError("Invalid file: {}".format(file))
            
        filename = path.basename(file)
        _, extension = path.splitext(filename)
        return filename, extension.lower().replace(".", "")


class TextMessage(Message):
    """Simple Text Message
    
    Simple Text Message, only support text message. Can deal with text str or dict.
    """
    def __init__(self, text: str ):
        """Initialize TextMessage.

        Keyword arguments:
            text: str, The text content of the message.
        """ 
        super().__init__(message_type="text")
        self.text = text
        
    @property
    def file_type(self):
        """File Type Property"""
        raise NotImplementedError("TextMessage has no file type.")
    
    @file_type.setter
    def file_type(self, value):
        """Set File Type Property"""
        raise NotImplementedError("TextMessage has no file type.")
    
    @property
    def file_name(self):
        """File Name Property"""
        raise NotImplementedError("TextMessage has no file name.")
    
    @file_name.setter
    def file_name(self, value):
        """Set File Name Property"""
        raise NotImplementedError("TextMessage has no file name.")
    
    
    @property
    def msg_type(self):
        """Message Type Property"""
        return self._msg_type
    
    @msg_type.setter
    def msg_type(self, value):
        """Set Message Type Property"""
        raise NotImplementedError("TextMessage msg_type is fixed to 'text'.")

    @property
    def message_key(self):
        return None
    
    
    @message_key.setter
    def message_key(self, value):
        """Set Message Key Property"""
        self._message_key = value


    @property
    def is_raw(self):
        """Check Message Whether Raw"""
        return True
    
    @property
    def is_can_upload(self):
        """Check Message Whether Can Upload"""
        return False
    
    
    @property
    def content(self):
        """Get Text Content"""
        # Parse text to dict if possible
        try:
            result = json.loads(self.text)
        except (json.JSONDecodeError, TypeError):
            result =  {}
            
        if isinstance(self.text, str) and result.get("text", False):
            return result
        elif isinstance(self.text, str):
            return {"text": self.text}
        elif isinstance(self.text, dict) and self.text.get("text", False):
            return self.text
        elif isinstance(self.text, dict):
            return {"text": self.text}
        else:
            return {"text": json.dumps(self.text, ensure_ascii=False)}


    def check_validate(self, *args, **kwargs):
        """Check Message Validate"""
        if not isinstance(self.text, str) or len(self.text) == 0:
            raise LarkMessageException("Text message content must be a non-empty string.")
        return True
    
    def upload_file(self, *args, **kwargs):
        """Upload File With API"""
        raise NotImplementedError("TextMessage has no file to upload.")
    
   
    
    def send_message(
        self, func, receive_id_type:str="chat_id", receive_id:str=None, uuid:str=None, *args, **kwargs
    ):
        """Send Message With API
        
        Args:
            func: A callable object that will handle the message sending
            *args: Additional arguments to pass to the sender
            **kwargs: Additional keyword arguments to pass to the sender
            
        Returns:
            The result of the callable function.
        """
        if not callable(func):
            raise TypeError("sender 'func' must be a callable object")
        
        if not check_function_arg(func, "msg_type"):
            raise TypeError("sender 'func' must accept a 'msg_type' keyword argument")
        
        if not check_function_arg(func, "content"):
            raise TypeError("sender 'func' must accept a 'content' keyword argument")

        content = self.content
        if kwargs.get("content", None) is not None:
            content = kwargs.pop("content")
            
            
        result = func(
            msg_type=self.msg_type, content=content, receive_id_type=receive_id_type,
            receive_id=receive_id, uuid=uuid, *args, **kwargs
        )
        return result
    


class ImageMessage(Message):
    """ Lark IM Image Message.
    """
    def __init__(self, image_key: str = None, image_type: str = None, file: str = None):
        """Initialize ImageMessage.

        Keyword arguments:
            image_key: str, The key of the image message, if `None`, file isn't uploaded
            image_type: str, The type of the image, lark image type,
                'message': it's message image type
                'avatar': it's user avatar image type
            file: str, The file path of the image
        """ 
        super().__init__(message_type="image")

        if file is not None and self.check_validate(file):
            self.image_key = image_key
            self._image_type = image_type
            self._file_name, self._file_type = self._extract_file_info(file)
            self._file = file

            logger.debug(f"Image file provided: {file}, ready to upload.")

        elif file is None and image_key is not None:
            self.image_key = image_key
            self._image_type = image_type
            self._file_name = None
            self._file_type = None
            self._file = None

            logger.debug(f"Image key provided: {image_key}, no file to upload.")
        else:
            logger.error("Either 'file' or 'image_key' must be provided.")
            raise LarkMessageException("Either 'file' or 'image_key' must be provided.")
        
    @property
    def message_key(self):
        return self.image_key
    
    @message_key.setter
    def message_key(self, value):
        self.image_key = value
        
        
    @property
    def image_type(self):
        return self._image_type

    @image_type.setter
    def image_type(self, value):
        if isinstance(value, str) and value.lower() in ("message", "avatar"):
            self._image_type = value.lower()
        else:
            raise LarkMessageException(
                "Invalid image type, must be 'message' or 'avatar'."
            )
        
    @property
    def file_name(self):
        """File Name Property"""
        return self._file_name
    
    
    @file_name.setter
    def file_name(self, value):
        """Set File Name Property"""
        if isinstance(value, str):
            self._file_name = value
        else:
            raise LarkMessageException("File name must be a string.")
       
       
    @property
    def file_type(self):
        """File Type Property"""
        if hasattr(self, "_file_type") and self._file_type is not None:
            return self._file_type
        else:
            raise NotImplementedError("File type is not set.") 
        
        
    @file_type.setter
    def file_type(self, value):
        """Set File Type Property"""
        if isinstance(value, str):
            self._file_type = value.lower()
        else:
            raise LarkMessageException("File type must be a string.")
        
    @property
    def msg_type(self):
        """Message Type Property"""
        return self._msg_type
    
    @msg_type.setter
    def msg_type(self, value):
        """Set Message Type Property"""
        if isinstance(value, str) and value.lower() == "image":
            self._msg_type = value.lower()
        else:
            raise LarkMessageException("Message type must be 'image'.")

    def check_validate(self, file: str):
        """Check Image File Validate
        
        Check image file type, must be one of: jpg, jpeg, png, webp, gif, bmp, ico, tiff, heic.
        
        Args:
            file (str): The image file path.
        """

        file_types = (
            "jpg", "jpeg", "png", "webp", "gif", "bmp", "ico", "tiff", "heic"
        )
        _, extension = self._extract_file_info(file)
        
        if extension not in file_types:
            raise LarkMessageException(
                "Invalid image file type, must be one of: {}".format(", ".join(file_types))
            )
        return True


    def upload_file(self, func, file:str=None, image_type:str="message", *args, **kwargs):
        """Upload File With API
        
        Args:
            func: A callable object that will handle the file upload
            file (str, optional): The file path to upload. If None, use the instance's file.
            image_type (str, optional): The type of the image, "message" or "avatar", default is "message".
            *args: Additional arguments to pass to the uploader
            **kwargs: Additional keyword arguments to pass to the uploader
            
        Returns:
            The result of the callable function.
        """
        if not callable(func):
            raise TypeError("uploader 'func' must be a callable object")

        if not check_function_arg(func, "file"):
            raise TypeError("uploader 'func' must accept a 'file' keyword argument")

        if not check_function_arg(func, "need_binary"):
            raise TypeError("uploader 'func' must accept a 'need_binary' keyword argument")


        # parse image_type from kwargs or use current
        image_type = self._image_type
        file = self.file
        file_name = self.file_name
        use_arg = False
        if file is not None:
            use_arg = True
            file = file

            file_name, _ = self._extract_file_info(file)
        if use_arg:
            if image_type in ("message", "avatar"):
                image_type = image_type
            else:
                image_type = "message"
            
        if self.is_can_upload:
            result = func(file=file, need_binary=True, image_type=image_type, *args, **kwargs)
            logger.debug(f"Image uploaded via uploader callable; local_file={self.file}")
            if isinstance(result, dict) and result.get("code", -1) == 0:
                # update image_key after successful upload
                self.image_key = result.get("data", {}).get("image_key")
                logger.info(f"Image file ({file_name}) key updated")
            return result
        else:
            logger.debug("Image upload skipped: no local file or already uploaded.")

    @staticmethod
    def static_upload_image2key(func, file:str, image_type:str="message",  *args, **kwargs):
        """Static Upload Image To Get Image Key
        Upload image file via provided callable function to get image_key.
        
        
        Args:
            func: A callable object that will handle the file upload
            file (str): The file path to upload
            image_type (str): The type of the image, "message" or "avatar", default is "message".
            *args: Additional arguments to pass to the uploader
            **kwargs: Additional keyword arguments to pass to the uploader
            
        Returns:
            The result of the callable function.
        """
        if not callable(func):
            raise TypeError("uploader 'func' must be a callable object")

        if not check_function_arg(func, "file"):
            raise TypeError("uploader 'func' must accept a 'file' keyword argument")

        if not check_function_arg(func, "need_binary"):
            raise TypeError("uploader 'func' must accept a 'need_binary' keyword argument")



        result = func(file=file, need_binary=True, image_type=image_type, *args, **kwargs)
        logger.debug(f"Image uploaded via uploader callable; local_file={file}")
        if isinstance(result, dict) and result.get("code", -1) == 0:
            # update image_key after successful upload
            image_key = result.get("data", {}).get("image_key")
            logger.info(f"Image file ({file}) key updated to: {image_key}")
        else:
            raise LarkMessageException("Image upload failed or invalid response.")
        return image_key
    
    
    def send_message(
        self, func, receive_id_type:str="open_id", receive_id:str=None, uuid:str=None, *args, **kwargs
    ):
        """Send Single Image Message
        Send image message via provided callable function. Can deal with current
            file image_key or new image_key of uploaded file

        Args:
            func: A callable object that will handle the message sending
            receive_id_type (str): The type of the receiver ID, default is "open_id".
            receive_id (str): The ID of the receiver.
            uuid (str, optional): The UUID of the message
            *args: Additional arguments to pass to the sender
            **kwargs: Additional keyword arguments to pass to the sender
            
        Returns:
            The result of the callable function.
        """
        if not callable(func):
            raise TypeError("sender 'func' must be a callable object")
        
        if not check_function_arg(func, "msg_type"):
            raise TypeError("sender 'func' must accept a 'msg_type' keyword argument")
        
        if not check_function_arg(func, "content"):
            raise TypeError("sender 'func' must accept a 'content' keyword argument")

        content = {
            "image_key": self.image_key
        }
        if kwargs.get("image_key", None) is not None:
            content["image_key"] = kwargs.pop("image_key")

        result = func(
            msg_type=self.msg_type, content=content, receive_id_type=receive_id_type,
            receive_id=receive_id, uuid=uuid, *args, **kwargs
        )
        return result


    
class FileMessage(Message):
    """ Lark IM File Message.
    """
    
    SPECIFIC_FILE_TYPES = (
        "opus", "mp4", "pdf", "doc", "xls", "ppt"
    )
    AUDIO_MESSAGE_TYPES = (
        "opus",
    )
    MEDIA_MESSAGE_TYPES = (
        "mp4",
    )
    
    def __init__(self, file: str = None):
        """Common File Message

        Common File Message, Like doc, xls, pdf, ppt.
        """

        if file is not None and self.check_validate(file):
            super().__init__(message_type="stream")
            self._file_key = None
            self._file = file
            self._file_name, self._file_extension = self._extract_file_info(file)
            self._file_type = self._file_extension

            if self._file_type in self.SPECIFIC_FILE_TYPES:
                self._file_type = self._file_extension
            else:
                logger.warning(f"File type '{self._file_extension}' is not a specific type, defaulting to 'stream'.")
            
            self.msg_type = self._file_type
        else:
            self._file_key = None
            self._file = None
            self._file_name = None
            self._file_type = None
            self._file_extension = None
            self._msg_type = None
        self._media_coverage_key = None  # For media type coverage file key

    @property
    def message_key(self):
        return self._file_key
    
    
    @message_key.setter
    def message_key(self, value):
        self._file_key = value
        


    @property
    def file_name(self):
        """File Name Property"""
        return self._file_name
    
    @file_name.setter
    def file_name(self, value):
        """Set File Name Property"""
        if isinstance(value, str):
            self._file_name = value
        else:
            raise LarkMessageException("File name must be a string.")
        
    @property
    def file_type(self):
        """File Type Property"""
        if hasattr(self, "_file_type") and self._file_type is not None:
            return self._file_type
        else:
            raise NotImplementedError("File type is not set.")
    
    @file_type.setter
    def file_type(self, value):
        """Set File Type Property"""
        if isinstance(value, str):
            if value.lower() in self.SPECIFIC_FILE_TYPES:
                self._file_type = value.lower()
            else:
                self._file_type = "stream"
                logger.warning(f"File type '{value}' is not a specific type, defaulting to 'stream'.")
        else:
            raise LarkMessageException("File type must be a string.")
        


    @property
    def file_extension(self):
        """File Extension Property"""
        return self._file_extension
    
    @property
    def mime_type(self):
        """MIME Type Property"""
        
        if self._file_extension is None:
            extension = "stream"
        else:
            extension = self._file_extension
            
        return MIMEType.__members__.get(extension.upper(), MIMEType.STREAM).value


    @property
    def msg_type(self):
        """Message Type Property"""
        return self._msg_type
    
    @msg_type.setter
    def msg_type(self, value):
        """Set Message Type Property"""
        if value.lower() in self.AUDIO_MESSAGE_TYPES:
            self._msg_type = "audio"
        elif value.lower() in self.MEDIA_MESSAGE_TYPES:
            self._msg_type = "media"
        elif value.lower() in self.SPECIFIC_FILE_TYPES or  value.lower() == "stream":
            self._msg_type = "file"
        else:
            raise LarkMessageException(
                "Message type must be one of: stream, {}".format(", ".join(self.SPECIFIC_FILE_TYPES))
            )

    @property
    def media_coverage_key(self):
        """Media Coverage Key Property"""
        return self._media_coverage_key
    
    
    @media_coverage_key.setter
    def media_coverage_key(self, value):
        """Set Media Coverage Key Property"""
        if self.msg_type not in self.MEDIA_MESSAGE_TYPES:
            raise LarkMessageException("Media coverage key can only be set for media message type.")
        
        if isinstance(value, ImageMessage):
            if value.image_key is None:
                raise LarkMessageException("Media coverage ImageMessage must have a valid image_key.")
            self._media_coverage_key = value.image_key
        elif isinstance(value, str):
            self._media_coverage_key = value
        else:
            raise LarkMessageException("Media coverage key must be a image key string or ImageMessage instance.")


    @property
    def file_key(self):
        """File Key Property"""
        return self._file_key
    
    @file_key.setter
    def file_key(self, value):
        """Set File Key Property"""
        self._file_key = value
    
    def check_validate(self, file: str):
        """Check File Validate

        Check file type, must be one of: doc, xls, pdf, ppt

        Args:
            file (str): The file path.
        """

        
        _, extension = self._extract_file_info(file)
        if len(extension) == 0:
            raise LarkMessageException(
                "Invalid file type, file must have an extension."
            )
        return True


    def upload_file(self, func, *args, **kwargs):
        """Upload File With API
        
        Args:
            func: A callable object that will handle the file upload
            *args: Additional arguments to pass to the uploader
            **kwargs: Additional keyword arguments to pass to the uploader
            
        Returns:
            The result of the callable function.
        """
        if not callable(func):
            raise TypeError("uploader 'func' must be a callable object")
        
        if not check_function_arg(func, "file"):
            raise TypeError("uploader 'func' must accept a 'file' keyword argument")

        if not check_function_arg(func, "need_binary"):
            raise TypeError("uploader 'func' must accept a 'need_binary' keyword argument")

        if not check_function_arg(func, "file_type"):
            raise TypeError("uploader 'func' must accept a 'file_type' keyword argument")

        if not check_function_arg(func, "file_name"):
            raise TypeError("uploader 'func' must accept a 'file_name' keyword argument")
        

        if self.is_can_upload:
            result = func(
                file=self._file, need_binary=True, file_type=self.file_type, file_name=self._file_name,
                mime_type=self.mime_type, *args, **kwargs
            )
            logger.info(f"File uploaded via uploader callable; local_file={self.file}")
            if isinstance(result, dict) and result.get("code", -1) == 0:
                # update file_key after successful upload
                self._file_key = result.get("data", {}).get("file_key")
                logger.info(f"File ({self._file_name}) key updated to: {self._file_key}")
            return result
        else:
            logger.debug("File upload skipped: no local file or already uploaded.")

    

    def send_message(
        self, func, receive_id_type:str="open_id", receive_id:str=None, uuid:str=None, 
        need_covarage_file:bool=False, *, file_key:str=None, msg_type:str=None, **kwargs
    ):
        """Send Single File Message
        Send file message via provided callable function. Can deal with current
            file file_key or new file_key of uploaded file

        Args:
            func: A callable object that will handle the message sending
            receive_id_type (str): The type of the receiver ID, default is "open_id".
            receive_id (str): The ID of the receiver.
            uuid (str, optional): The UUID of the message
            need_covarage_file (bool): Whether a coverage file is needed for media type,
                if True, an 'image_key' must be provided via kwargs or current object attribute.
            file_key (str, optional): The file_key to use for sending the message
            msg_type (str, optional): The message type to use when file_key is provided,
            *args: Additional arguments to pass to the sender
            **kwargs: Additional keyword arguments to pass to the sender
            
            If want send media type message and need coverage file, must provide 'image_key' via kwargs,
                else will use current media_coverage_key attribute.
        Returns:
            The result of the callable function.
        """
        if not callable(func):
            raise TypeError("sender 'func' must be a callable object")
        
        if not check_function_arg(func, "msg_type"):
            raise TypeError("sender 'func' must accept a 'msg_type' keyword argument")
        
        if not check_function_arg(func, "content"):
            raise TypeError("sender 'func' must accept a 'content' keyword argument")

        msg_type = self.msg_type
        
        use_args = False
        if file_key is not None:
            content = {
                "file_key": file_key
            }
            use_args = True
            
            # Use provided file_key directly, must get msg_type from kwargs
            if msg_type is not None:
                msg_type = msg_type.lower()
            else:
                raise LarkMessageException("When providing 'file_key' via kwargs, 'msg_type' must also be provided.")
        elif self.msg_type in ("audio", "file"):
            content = {
                "file_key": self.file_key
            }

        elif self.msg_type in ("media",):
            content = {
                "file_key": self.file_key
            }
        else:
            raise LarkMessageException("Invalid message type for file message, or missing file_key.")
        
        # handle coverage file for media type
        if need_covarage_file and msg_type in ("media", ):
            if use_args and kwargs.get("image_key", None) is not None:
                content["image_key"] = kwargs.pop("image_key")
            elif hasattr(self, "media_coverage_key") and self.image_key is not None:
                content["image_key"] = self.media_coverage_key
            else:
                raise LarkMessageException("Media message coverage file required but no image_key provided.")

        result = func(
            msg_type=self.msg_type, content=content, receive_id_type=receive_id_type,
            receive_id=receive_id, uuid=uuid,  **kwargs
        )
        return result
    

class StaticInteractiveMessage(Message):
    """ Lark IM Static Interactive Message.
    Static Interactive Message, only support card dict.
    """
    def __init__(self, card: dict = None):
        """Initialize InteraciiveMessage.

        Keyword arguments:
            card: dict, The card content of the interactive message.
        """ 
        super().__init__(message_type="interactive")
        self._card = card if isinstance(card, dict) else {}
        self._msg_type = "interactive"
        
    @property
    def file_type(self):
        """File Type Property"""
        raise NotImplementedError("InteraciiveMessage has no file type.")
    
    @file_type.setter
    def file_type(self, value):
        """Set File Type Property"""
        raise NotImplementedError("InteraciiveMessage has no file type.")
    
    @property
    def file_name(self):
        """File Name Property"""
        raise NotImplementedError("InteraciiveMessage has no file name.")
    
    @file_name.setter
    def file_name(self, value):
        """Set File Name Property"""
        raise NotImplementedError("InteraciiveMessage has no file name.")
    
    
    @property
    def msg_type(self):
        """Message Type Property"""
        return self._msg_type
    
    @msg_type.setter
    def msg_type(self, value):
        """Set Message Type Property"""
        raise NotImplementedError("InteraciiveMessage msg_type is fixed to 'interactive'.")

    @property
    def message_key(self):
        return None
    
    
    @message_key.setter
    def message_key(self, value):
        """Set Message Key Property"""
        self._message_key = value


    @property
    def is_raw(self):
        """Check Message Whether Raw"""
        return True
    
    @property
    def is_can_upload(self):
        """Check Message Whether Can Upload"""
        return False
    
    
    @property
    def content(self):
        """Get Card Content"""
        return self._card


    def check_validate(self, *args, **kwargs):
        """Check Message Validate"""
        if not isinstance(self._card, dict) or len(self._card) == 0:
            raise LarkMessageException("Interactive message card content must be a non-empty dict.")
        return True
    
    def upload_file(self, *args, **kwargs):
        """Upload File With API"""
        raise NotImplementedError("InteraciiveMessage has no file to upload.")
    
   
    
    def send_message(self, func, receive_id_type:str="open_id", receive_id:str=None, uuid:str=None, *args, **kwargs):
        """Send Message With API
        
        Args:
            func: A callable object that will handle the message sending
            *args: Additional arguments to pass to the sender
            **kwargs: Additional keyword arguments to pass to the sender
            
        Returns:
            The result of the callable function.
        """
        if not callable(func):
            raise TypeError("sender 'func' must be a callable object")
        
        if not check_function_arg(func, "msg_type"):
            raise TypeError("sender 'func' must accept a 'msg_type' keyword argument")
        
        if not check_function_arg(func, "content"):
            raise TypeError("sender 'func' must accept a 'content' keyword argument")

        content = self.content
        if kwargs.get("content", None) is not None:
            content = kwargs.pop("content")
            
            
        result = func(
            msg_type=self.msg_type, content=content, receive_id_type=receive_id_type,
            receive_id=receive_id, uuid=uuid, *args, **kwargs
        )
        return result