#coding:utf8
"""Lark Instant Messaging (IM) module.

This module provides functionalities to interact with Lark's IM services,
including sending messages, managing chats, and handling user interactions.

"""


import logging
import json

from requests_toolbelt import MultipartEncoder

from ..utils import request
from ....utils.common import parse_file_size
from ..exceptions import LarkException, LarkMessageException
from ..base import LarkClient
from ..base.im import (
    ImageMessage
)



from ..common import LarkImURL, LarkContactURL



logger = logging.getLogger("automation.lark.api.im")


class LarkIM(LarkClient):
    """Lark Instant Messaging (IM) client.

    This class extends the base LarkClient to provide IM-specific functionalities.

    """

    _FILE_LIMIT_MB = 30  # 30 MB limit for file uploads

    def __init__(self, app_id: str = None, app_secret: str = None, lark_host: str="https://open.feishu.cn"):
        """Initialize the LarkIM client with optional app credentials.

        Args:
            app_id (str, optional): The application ID for authentication.
            app_secret (str, optional): The application secret for authentication.

        """
        super().__init__(app_id=app_id, app_secret=app_secret, lark_host=lark_host)
        
        


    def query_single_user(self, user_id: str = None, user_id_type: str = None, department_id_type: str = None):
        """Query a single user's information from Lark's Contact service.

        This method is a placeholder for querying user information and should be implemented.

        Raises:
            NotImplementedError: Always, as this method is not yet implemented.

        """
        url = LarkContactURL.QUERY_SINGLE_USER.value.format(user_id=user_id)
        
        if user_id_type is not None and user_id_type in ("user_id", "open_id", "union_id"):
            param = {
                "user_id_type": user_id_type
            }
        else:
            param = {}
        
        if department_id_type is not None and department_id_type in ("department_id", "open_department_id"):
            param["department_id_type"] = department_id_type

        headers = {
            "Authorization": f"Bearer {self.tenant_access_token}"
        }
        
        
        resp = request(
            method="GET",
            url=url,
            headers=headers,
            params=param
        )
        if resp.get("code", -1) == 0:
            logger.info(f"User({user_id}) information queried successfully:")
        else:
            logger.error(f"Failed to query user({user_id}) information: {resp.get('msg', '')}")
            raise LarkMessageException(f"Failed to query user({user_id}) information: {resp.get('msg', '')}")
        return resp
        

    def upload_image(self, file=None,  image_type="message", need_binary=True):
        """Upload an image to Lark's IM service.

        This method uploads an image file or uses an existing image key.

        Args:
            file (str, optional): The path to the image file to upload.
            image_type (str, optional): The type of image, either "message" or "avatar". Defaults to "message".
            need_binary (bool, optional): Whether to read the file as binary. Defaults to True.

        Returns:
            dict: The response from the upload function.

        Raises:
            ValueError: If neither file nor image_key is provided.

        """
        # Prepare the file for upload
        if need_binary:
            with open(file, "rb") as f:
                file = f.read()
                
        data = {
            "image_type": image_type,
            "image": file
        }
        
        url = LarkImURL.UPLOAD_IMAGE.value

        headers = {
            'Authorization': f'Bearer {self.tenant_access_token}',
        }
        form = MultipartEncoder(fields=data)

        headers['Content-Type'] = form.content_type
        
        resp = request(
            method="POST",
            url=url,
            headers=headers,
            data=form
        )
        
        if resp.get("code", -1) == 0:
            logger.info(f"Image file({file}) uploaded successfully:")
        else:
            logger.error(f"Failed to upload image file({file}): {resp.get('msg', '')}")
            raise LarkMessageException(f"Failed to upload image file({file}): {resp.get('msg', '')}")
        return resp
    

    def upload_file(self, file=None, file_name=None, file_type="stream", mime_type=None, need_binary=True):
        """Upload a file to Lark's IM service.

        This method uploads a file to Lark's IM service.

        Args:
            file (str, optional): The path to the file to upload.
            file_type (str, optional): The type of file, Defaults to "stream". 
                "stream" is for general file uploads. Other specified file types:
                * "opus"
                * "mp4"
                * "pdf"
                * "doc"
                * "xls"
                * "ppt"
            need_binary (bool, optional): Whether to read the file as binary. Defaults to True.

        Returns:
            dict: The response from the upload function.

        Raises:
            ValueError: If file is not provided.

        """
        if file is None:
            raise ValueError("File path must be provided for upload.")
            
        # Raise Excelption if file size exceeds limit
        file_size = parse_file_size(file, unit='mb')
        
        if file_size > self._FILE_LIMIT_MB:
            raise LarkException(f"File size {file_size} MB exceeds the limit of {self._FILE_LIMIT_MB} MB.")
        
        # Prepare the file for upload
        if need_binary:
            with open(file, "rb") as f:
                file = f.read()


        url = LarkImURL.UPLOAD_FILE.value
        data = {
            "file": (file_name, file, mime_type),
            "file_type": file_type,
            "file_name": file_name
        }
        
        headers = {
            'Authorization': f'Bearer {self.tenant_access_token}',
        }
        form = MultipartEncoder(fields=data)

        headers['Content-Type'] = form.content_type
        
        resp = request(
            method="POST",
            url=url,
            headers=headers,
            data=form
        )
        
        if resp.get("code", -1) == 0:
            logger.info(f"File({file}) uploaded successfully:")
        else:
            logger.error(f"Failed to upload file({file}): {resp.get('msg', '')}")
            raise LarkMessageException(f"Failed to upload file({file}): {resp.get('msg', '')}")
        return resp

        


    def send_message(self, receive_id_type:str="chat_id", content:dict=None, msg_type:str="text", receive_id:str=None, uuid:str=None):
        """Send a message via Lark's IM service.

        This method is a placeholder for sending messages and should be implemented.

        Raises:
            NotImplementedError: Always, as this method is not yet implemented.

        """
        url = LarkImURL.SEND_MESSAGE.value
        param = {
            "receive_id_type": receive_id_type
        }

        if not isinstance(content, str):
            content = json.dumps(content, ensure_ascii=False)
        
        payload = {
            "receive_id": receive_id,
            "msg_type": msg_type,
            "content": content
        }
        

        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Authorization': f'Bearer {self.tenant_access_token}'
        }
        
        resp = request(
            method="POST",
            url=url,
            headers=headers,
            params=param,
            payload=payload
        )
        
        if uuid is not None:
            resp["uuid"] = uuid
            
        if resp.get("code", -1) == 0:
            logger.info(f"Message sent successfully to {receive_id}:")
        else:
            logger.error(f"Failed to send message to {receive_id}: {resp.get('msg', '')}")
            raise LarkMessageException(f"Failed to send message to {receive_id}: {resp.get('msg', '')}")
        
        return resp