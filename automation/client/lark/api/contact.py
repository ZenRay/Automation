#coding:utf-8
"""Lark Contact API
1. Search User
"""

import logging
import requests

from datetime import datetime, timedelta
from typing import Optional

from ..base import LarkClient, UserAccessToken, TokenStatus

from ..utils import lark_request
from ..common import LarkContactURL


logger = logging.getLogger("automation.client.lark.api.contact")


class LarkContact(LarkClient):
    """Lark Contact API
    """
    # Initialize UserAccessToken database on first use
    UserAccessToken.init_database()
    
    def __init__(self, *, app_id, app_secret, master_info={"user_name": "", "user_email":""}, lark_host="https://open.feishu.cn"):
        """Init Lark Contact Object
        
        Args:
        ---------
            app_id: Lark App ID
            app_secret: Lark App Secret
            master_info: dict with keys "user_name" and "user_email" for Lark Client App Host Name(User Name) and Email, default is {"user_name": "", "user_email":""} that
                is useless for Lark Open Platform
        """
        super().__init__(app_id=app_id, app_secret=app_secret, lark_host=lark_host)

        # Initialize User Access Token storage which is Master specific
        users = UserAccessToken.where(**master_info)
        if len(users) != 1:
            logger.warning(f"Master user not found or multiple found for info: {master_info}. User access token functions may not work properly.")
            self.__master_user_access_token = None
        else:
            self.__master_user_access_token = users[0]
            logger.info(f"Master user access token loaded: {self.__master_user_access_token}")
            
        
        
        
    @property
    def user_access_token(self) -> str:
        """Get User Access Token string
        
        Automatically refreshes token if expired and refresh_token is available.

        Returns:
        ---------
            str: User Access Token string, or None if not set
        """
        if self.__user_access_token is None:
            logger.warning("User access token is not set")
            return None
            
        # Check if token needs refresh
        if self.__user_access_token.needs_refresh():
            logger.info(f"Token for user {self.__user_access_token.user_id} needs refresh")
            # Auto-refresh would require a refresh_func - caller should handle refresh
            
        if self.__user_access_token.is_valid:
            return self.__user_access_token.access_token
        else:
            logger.warning(f"User access token is not valid (status: {self.__user_access_token.status.value})")
            return None
    
    @user_access_token.setter
    def user_access_token(self, token: UserAccessToken):
        """Set User Access Token instance
        
        Args:
        ----------
            token: UserAccessToken instance
        """
        if not isinstance(token, UserAccessToken):
            raise TypeError("token must be a UserAccessToken instance")
        self.__user_access_token = token
    
    
    
    def query_user_info(self, user_access_token: str) -> dict:
        """Query user information using user_access_token
        Reference doc:
        https://open.feishu.cn/document/server-docs/authentication-management/login-state-management/get
        
        Args:
        ---------
            user_access_token: User access token string
            
        Returns:
            Dict with user information: {
                'user_id': str,
                'name': str,
                'en_name': str,
                'email': str,
                'mobile': str,
                'tenant_key': str,
                ...
            }
            
        Raises:
            Exception: If query fails
        """
        url = LarkContactURL.QUERY_USER_INFO.value
        
        headers = {
            "Authorization": f"Bearer {user_access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        response = lark_request.request(
            method="GET",
            url=url,
            headers=headers
        )
        
        result = response
        
        if result.get("code") != 0:
            error_msg = result.get("msg") or "Unknown error"
            logger.error(f"Failed to query user info: {error_msg}")
            raise Exception(f"Query user info failed: {error_msg}")
        
        user_info = result.get("data", {})
        logger.info(f"Successfully retrieved user info for user_id: {user_info.get('user_id')}")
        return user_info
    
        
        
    def search_user(self, keyword: str, page_size: int = 10, page_token: str = None) -> dict:
        """Search User by Keyword

        Args:
            keyword (str): Search keyword
            page_size (int, optional): Page Size. Defaults to 10.
            page_token (str, optional): Page Token. Defaults to None.
        Returns:
            dict: API Response
        """
        url = LarkContactURL.SEARCH_USER_BY_KEYWORD.value

        # Create Headers and Params
        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token()}",
            "Content-Type": "application/json; charset=utf-8"
        }
        params = {
            "keyword": keyword,
            "page_size": page_size,
        }
        
        if page_token:
            params["page_token"] = page_token
        
        response = lark_request.request(
            method="GET",
            url=url,
            headers=headers,
            params=params
        )
        
        return response