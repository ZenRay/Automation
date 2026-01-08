#coding:utf-8
"""Feishu aPaaS API module

Provides a minimal client for aPaaS related APIs. Currently implements
the workspace table listing endpoint.

"""

import logging
import time

from urllib.parse import urlencode
from typing import Optional, List

from ..utils import request
from ..exceptions import LarkException
from ..base import LarkClient, UserAccessToken
from .contact import LarkContact
from ..base.aPaaS import TableItem
from ..common import APaaSURL

logger = logging.getLogger("automation.lark.api.aPaaS")


class LarkAPaaS(LarkClient):
    """Client for Feishu aPaaS APIs.

    This is an initial, small implementation focused on listing tables
    under a workspace as described in the aPaaS docs.

    """
    _scopes=[
        "app_engine:workspace.table:read", 
        "app_engine:workspace.table:write", 
        "app_engine:workspace.table.record:write",
        "contact:contact", 
        "offline_access"
    ]
    def __init__(
        self, *, app_id, app_secret, lark_host="https://open.feishu.cn", 
        redirect_uri="http://localhost:9990/callback"
    ):
        super().__init__(app_id=app_id, app_secret=app_secret, lark_host=lark_host)

        # aPaas data platform tables
        self._tables = []  # type: List[TableItem]
        
        # aPaas data platform workspace id
        self._workspace_id = None  # type: Optional[str]
        # Initialize UserAccessToken database on first use
        # self._user_access_token = self.init_user_access_token()
        self._user_access_token = self.init_user_access_token(redirect_uri=redirect_uri)
    
    @property
    def tables(self) -> List[TableItem]:
        """Get the list of tables retrieved from aPaaS.

        Returns:
            List[TableItem]: list of TableItem instances

        """
        return self._tables
    
    @property
    def workspace_id(self) -> Optional[str]:
        """Get the workspace ID associated with this client.

        Returns:
            Optional[str]: workspace ID if set, else None

        """
        return self._workspace_id
    
    @property
    def user_access_token(self) -> str:
        """Get the User Access Token string.

        Returns:
            str: User Access Token string

        """
        if self._user_access_token is None:
            logger.warning("User access token is not set")
            return None
        
        if self._user_access_token.is_expired or self._user_access_token.needs_refresh():
            self._user_access_token.auto_refresh()
            
        return self._user_access_token.user_access_token
    
    
    @user_access_token.setter
    def user_access_token(self, token: UserAccessToken):
        """Set the User Access Token.

        Args:
            token (UserAccessToken): a UserAccessToken instance

        """
        if not isinstance(token, UserAccessToken):
            raise ValueError("token must be a UserAccessToken instance")
        
        self._user_access_token = token
            
        
    
    
    def init_user_access_token(self, redirect_uri: str = None):
        """Initialize UserAccessToken database on first use."""
        UserAccessToken.init_database()
        
    
        # TODO: right now get token by interactive method in webbrowser
        user_access_token = UserAccessToken.get_user_access_token_interactive(
            client=self,
            scope=" ".join(self._scopes),
            port=9990,
            auto_open_browser=True,
            redirect_uri=redirect_uri
        )

        return user_access_token
        
    def list_workspace_tables(self,
                              workspace_id: Optional[str] = None,
                              page_size: int = 20,
                              page_token: Optional[str] = None):
        """List tables under a workspace.

        Args:
            workspace_id: target workspace id (optional depending on API usage)
            user_access_token: a `UserAccessToken` instance
            page_size: number of items per page
            page_token: pagination token for next page

        Returns:
            dict: parsed response from Feishu aPaaS endpoint

        Raises:
            ValueError: when token is missing
            LarkException: when API returns an error code

        """
        # resolve token
        token = self._user_access_token
        if not token:
            raise ValueError("User access token is required to list workspace tables")
        
        # Correct endpoint (GET) per API: /open-apis/apaas/v1/workspaces/{workspace_id}/tables
        if not workspace_id:
            raise ValueError("workspace_id is required for listing workspace tables")

        url = APaaSURL.QUERY_WORKSPACE_TABLES.value.format(workspace_id=workspace_id)

        

        params = {
            "page_size": page_size
        }
    
        self._workspace_id = workspace_id
        
        while True:
            if page_token:
                params["page_token"] = page_token
                
            if token.is_expired:
                token.refresh()
            
            headers = {
                "Authorization": f"Bearer {token.user_access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            resp = request(
                method="GET",
                url=url,
                headers=headers,
                params=params
            )
            
            if resp.get("code", -1) == 0:
                logger.info(f"Workspace tables listed successfully for workspace: {workspace_id}")
            
            else:
                self._workspace_id = None
                self._tables = []
                logger.error(f"Failed to list workspace tables: {resp.get('msg', '')}. And Cleared workspace_id and tables.")
                raise LarkException(code=resp.get("code", -1), msg=resp.get("msg", "Unknown error"))
            
            for item in resp.get("data", {}).get("items", []):
                table_item = TableItem.from_dict(item)
                self._tables.append(table_item)
            time.sleep(3)

            # if there's not next page, break
            page_token = resp.get("data", {}).get("page_token")
            if not page_token:
                break
        
        
    def delete_table_records(self,
                             table_name: str,
                             filter_conditions: List[dict],
                             workspace_id: str=None):
        """Delete records from a table in a workspace.

        Docs:
        ------------
        delete reference: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/apaas-v1/workspace-table/records_delete
        filter reference: https://docs.postgrest.org/en/v13/references/api/tables_views.html#horizontal-filtering
        
        
        Args:
        ------------
            workspace_id: target workspace id
            table_name: target table name
            filter_conditions: list of filter conditions to delete records
        Returns:
            dict: parsed response from Feishu aPaaS endpoint
        Raises:
            ValueError: when token is missing
            LarkException: when API returns an error code
        """
        # check table exists
        if workspace_id is None:
            workspace_id = self.workspace_id
        else:
            self.list_workspace_tables(workspace_id=workspace_id)
            
        if not any(t.table_name == table_name for t in self._tables):
            logger.error(f"Table {table_name} not found in workspace {self.workspace_id}")
            raise ValueError(f"Table {table_name} not found in workspace {self.workspace_id}")
        
        # resolve token
        token = self.user_access_token 
        url = APaaSURL.DELETE_TABLE_RECORDS.value.format(
            workspace_id=workspace_id,
            table_name=table_name
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        payload = {
            "filter": filter_conditions
        }
        
        url = f"{url}?{urlencode(payload)}"
        logger.info(
            f"Deleting records from workspace({workspace_id}) table {table_name} with filter: {filter_conditions}."
            f"Request URL: {url}"
        )
        result = request(
            method="DELETE",
            url=url,
            headers=headers
        )
    
    
        if result.get("code") != 0:
            error_msg = result.get("msg") or "Unknown error"
            logger.error(f"Failed to delete records from table {table_name}: {error_msg}")
            raise LarkException(code=result.get("code"), msg=error_msg)
        else:
            logger.info(f"Records deleted successfully from workspace({workspace_id}) table ({table_name})")
        return result
            
    def query_user_info(self, user_access_token: str) -> dict:
        """Query user info using user_access_token.

        This implements the contract expected by `UserAccessToken.exchange_code_for_token`.
        """

        return LarkContact._query_user_info(user_access_token)