#coding:utf-8
"""Lark Contact API
1. Search User
"""

import logging

from datetime import datetime, timedelta


from ..base import LarkClient, UserAccessToken, TokenStatus

from ..utils.lark_request import request
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
    
    
    def get_authorization_url(self, redirect_uri: str, scope: str = "contact:contact", state: str = None) -> str:
        """Generate OAuth authorization URL for user to grant permissions
        
        Args:
            redirect_uri: Application callback URL (must be configured in Developer Console)
            scope: Space-separated permissions required (e.g., "contact:contact bitable:app:readonly")
            state: Optional state parameter for CSRF protection
            
        Returns:
            str: Full authorization URL to redirect user to
            
        Example:
            >>> url = contact.get_authorization_url(
            ...     redirect_uri="https://example.com/callback",
            ...     scope="contact:contact offline_access",
            ...     state="random_state_string"
            ... )
        """
        import urllib.parse
        
        auth_base_url = LarkContactURL.AUTH_CODE.value
        
        params = {
            "client_id": self.app_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            # "scope": scope
        }
        
        if state:
            params["state"] = state
            
        query_string = urllib.parse.urlencode(params)
        authorization_url = f"{auth_base_url}?{query_string}"
        
        logger.info(f"Generated authorization URL with scope: {scope}")
        return authorization_url
    
    def exchange_code_for_token(self, code: str, redirect_uri: str = None) -> UserAccessToken:
        """Exchange authorization code for user_access_token
        
        Args:
            code: Authorization code obtained from callback (valid for 5 minutes, single use)
            redirect_uri: The same redirect_uri used when getting the authorization code
            
        Returns:
            UserAccessToken: Token instance with access_token, refresh_token, etc.
            
        Raises:
            Exception: If token exchange fails
            
        Example:
            >>> # After user authorizes and callback with code
            >>> token = contact.exchange_code_for_token(
            ...     code="a61hb967bd094dge949h79bbexd16dfe",
            ...     redirect_uri="https://example.com/callback"
            ... )
            >>> contact.user_access_token = token
            >>> contact.save_user_access_token()
        """
        from datetime import datetime, timedelta
        import requests
        
        url = "https://open.feishu.cn/open-apis/authen/v2/oauth/token"
        
        headers = {
            "Content-Type": "application/json; charset=utf-8"
        }
        
        body = {
            "grant_type": "authorization_code",
            "client_id": self.app_id,
            "client_secret": self.app_secret,
            "code": code
        }
        
        if redirect_uri:
            body["redirect_uri"] = redirect_uri
        
        response = requests.post(
            url=url,
            headers=headers,
            json=body
        )
        
        result = response.json()
        
        if result.get("code") != 0:
            error_msg = result.get("error_description") or result.get("msg") or "Unknown error"
            logger.error(f"Failed to exchange code for token: {error_msg}")
            raise Exception(f"Token exchange failed: {error_msg}")
        
        # Create UserAccessToken instance
        expires_in = result.get("expires_in", 7200)
        expire_time = datetime.now() + timedelta(seconds=expires_in)
        
        # scopes = result.get("scope", "").split()
        import ipdb; ipdb.set_trace()
        token = UserAccessToken(
            access_token=result["access_token"],
            refresh_token=result.get("refresh_token"),
            expire_time=expire_time,
            # scopes=scopes,
            status=TokenStatus.ACTIVE,
            source="oauth_authorization"
        )
        
        # logger.info(f"Successfully exchanged code for user_access_token with scopes: {scopes}")
        return token
    
    def refresh_user_access_token(self, refresh_token: str = None) -> UserAccessToken:
        """Refresh user_access_token using refresh_token
        
        Args:
            refresh_token: Refresh token (uses current token's refresh_token if not provided)
            
        Returns:
            UserAccessToken: New token instance
            
        Raises:
            Exception: If refresh fails
        """
        if refresh_token is None:
            if self.__user_access_token is None or self.__user_access_token.refresh_token is None:
                raise ValueError("No refresh_token available")
            refresh_token = self.__user_access_token.refresh_token
        
        url = LarkContactURL.AUTH_USER_TOKEN.value
        
        headers = {
            "Content-Type": "application/json; charset=utf-8"
        }
        
        body = {
            "grant_type": "refresh_token",
            "client_id": self.app_id,
            "client_secret": self.app_secret,
            "refresh_token": refresh_token
        }
        
        response = request(
            method="POST",
            url=url,
            headers=headers,
            json=body
        )
        
        result = response.json()
        
        if result.get("code") != 0:
            error_msg = result.get("error_description") or result.get("msg") or "Unknown error"
            logger.error(f"Failed to refresh token: {error_msg}")
            raise Exception(f"Token refresh failed: {error_msg}")
        
        expires_in = result.get("expires_in", 7200)
        expire_time = datetime.now() + timedelta(seconds=expires_in)
        
        # scopes = result.get("scope", "").split()
        
        # Keep original user info if available
        token = UserAccessToken(
            user_id=self.__user_access_token.user_id if self.__user_access_token else None,
            user_name=self.__user_access_token.user_name if self.__user_access_token else None,
            user_email=self.__user_access_token.user_email if self.__user_access_token else None,
            access_token=result["access_token"],
            refresh_token=result.get("refresh_token"),
            expire_time=expire_time,
            # scopes=scopes,
            status=TokenStatus.ACTIVE,
            source="token_refresh"
        )
        
        logger.info(f"Successfully refreshed user_access_token")
        return token
    
    def get_user_access_token_interactive(
        self, 
        scope: str = "contact:contact offline_access", 
        port: int = 8080,
        timeout: int = 120,
        auto_open_browser: bool = True
    ) -> UserAccessToken:
        """Interactive OAuth flow to get user_access_token (Desktop/CLI apps)
        
        This method:
        1. Starts local HTTP server for OAuth callback
        2. Generates authorization URL
        3. Opens browser for user authorization (optional)
        4. Waits for callback and exchanges code for token
        5. Returns UserAccessToken instance
        
        Args:
            scope: Space-separated permissions (default: "contact:contact offline_access")
            port: Local server port for callback (default: 8080)
            timeout: Authorization timeout in seconds (default: 120)
            auto_open_browser: Automatically open browser (default: True)
            
        Returns:
            UserAccessToken: Token instance with access_token and refresh_token
            
        Raises:
            Exception: If authorization or token exchange fails
            TimeoutError: If user doesn't authorize within timeout
            
        Example:
            >>> from automation.client.lark.api.contact import LarkContact
            >>> 
            >>> contact = LarkContact(app_id="cli_xxx", app_secret="secret_xxx")
            >>> 
            >>> # Interactive OAuth - will open browser
            >>> token = contact.get_user_access_token_interactive(
            ...     scope="contact:contact bitable:app:readonly offline_access",
            ...     port=8080
            ... )
            >>> 
            >>> # Token is now available
            >>> contact.user_access_token = token
            >>> token.save()  # Save to database
            >>> 
            >>> # Use the token for API calls
            >>> result = contact.search_user("张三")
        
        Note:
            Make sure to add http://localhost:<port>/callback to your app's 
            redirect URLs in Feishu Developer Console before running.
        """
        from ..utils.oauth_local_server import OAuthCallbackServer
        import webbrowser
        
        redirect_uri = f"http://localhost:{port}/callback"
        
        # Generate authorization URL
        auth_url = self.get_authorization_url(
            redirect_uri=redirect_uri,
            # scope=scope,
            state=None  # Could add CSRF protection if needed
        )
        
        logger.info(f"Starting interactive OAuth flow on port {port}")
        print(f"\n{'='*60}")
        print(f"飞书用户授权")
        print(f"{'='*60}")
        
        # Start local callback server
        with OAuthCallbackServer(port=port) as server:
            print(f"\n回调服务器: {server.redirect_uri}")
            print(f"授权范围: {scope}")
            print(f"超时时间: {timeout}秒\n")
            
            if auto_open_browser:
                print("正在打开浏览器进行授权...")
                try:
                    webbrowser.open(auth_url)
                except Exception as e:
                    logger.warning(f"无法自动打开浏览器: {e}")
                    print(f"\n⚠️  无法自动打开浏览器，请手动访问以下URL:")
                    print(f"\n{auth_url}\n")
            else:
                print("请在浏览器中访问以下URL进行授权:")
                print(f"\n{auth_url}\n")
            
            print("等待授权中...")
            print(f"{'='*60}\n")
            
            # Wait for callback
            try:
                auth_code, state = server.wait_for_callback(timeout=timeout)
            except TimeoutError:
                print(f"\n✗ 授权超时 ({timeout}秒)")
                raise
            except Exception as e:
                print(f"\n✗ 授权失败: {e}")
                raise
        
        # Exchange code for token
        print("✓ 收到授权码")
        print("正在交换访问令牌...\n")
        
        token = self.exchange_code_for_token(
            code=auth_code,
            redirect_uri=redirect_uri
        )
        
        print(f"{'='*60}")
        print("✓ 成功获取用户访问令牌!")
        print(f"{'='*60}")
        print(f"权限范围: {', '.join(token.scopes)}")
        print(f"有效期至: {token.expire_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if token.refresh_token:
            print(f"刷新令牌: 已获取 (可用于延长有效期)")
        print(f"{'='*60}\n")
        
        return token
        
        
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
        
        response = request(
            method="GET",
            url=url,
            headers=headers,
            params=params
        )
        
        return response.json()