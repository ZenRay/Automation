#coding:utf8
"""Lark Instant Messaging (IM) module.

This module provides functionalities to interact with Lark's IM services,
including sending messages, managing chats, and handling user interactions.

"""


import logging


from requests_toolbelt import MultipartEncoder

from ..utils import request
from ....utils.common import parse_file_size
from ..exceptions import LarkException, LarkMessageException
from ..base import LarkClient
from ..base.im import (
    ImageMessage
)



from ..common import LarkImURL



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
            # 'Content-Type': 'application/json; charset=utf-8',
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