#coding:utf-8
"""Feishu aPaaS API module

Provides a minimal client for aPaaS related APIs. Currently implements
the workspace table listing endpoint.

"""

import logging
import time
import re

from urllib.parse import urlencode
from typing import Optional, List

# from ..utils import request
from ..exceptions import LarkException
from ..base import LarkClient, UserAccessToken
from .contact import LarkContact
from ..base.aPaaS import TableItem
from ..common import APaaSURL

from ..utils import lark_request
from ..utils import data_generator

logger = logging.getLogger("automation.lark.api.aPaaS")


class LarkAPaaSClient(LarkClient):
    """Client for Feishu aPaaS APIs.

    This is an aPaaS client application.

    Noticed ⚠️:  This client common requires User Access Token to access,
    which means it acts on behalf of a user. Please ensure you have the
    necessary permissions and tokens to perform operations.

    """
    
    # Default Token Scopes for aPaaS API Client
    _scopes=[
        "app_engine:workspace.table:read", 
        "app_engine:workspace.table:write", 
        "app_engine:workspace.table.record:write",
        "contact:contact", 
        "offline_access"
    ]
    # Extract workspace_id from aPaaS URL
    _regex_pattern = re.compile(r"http.*/suda/workspace/(?P<workspace_id>[A-Za-z0-9_]+)/", re.I)
    ADD_RECORD_LIMITATION = 500  # Max 500 records can be added at once.
    
    
    def __init__(
        self, *, app_id, app_secret, lark_host="https://open.feishu.cn", 
        user_name=None,
        redirect_uri="http://localhost:9990/callback"
    ):
        """Initialize Lark aPaaS Client.
        
        Args:
        ------------
            user_name: str, optional, the user name to identify the UserAccessToken
            redirect_uri: str, optional, the redirect URI for OAuth2 flow
        
        
        """
        super().__init__(app_id=app_id, app_secret=app_secret, lark_host=lark_host)
         
        
        # aPaas data platform tables
        self._tables = []  # type: List[TableItem]
        
        # aPaas data platform workspace id
        self._workspace_id = None  # type: Optional[str]
        
        # FIXME: Initialize UserAccessToken database on first use, right now
        # just the persistent token information, don't use interactive method.
        UserAccessToken.init_database()
        # self._user_access_token = self.init_user_access_token(redirect_uri=redirect_uri)
        self._user_access_token = UserAccessToken.extract_token_by_username(client=self,user_name=user_name)  # type: Optional[UserAccessToken]
        
        if not isinstance(self._user_access_token, UserAccessToken):
            logger.warning(
                f"UserAccessToken<user_name={user_name}> not found. You need setup "
                "'user_access_token' property manually when needed."
            )
            self._user_access_token = None
        else:
            logger.info(f"UserAccessToken<user_name={user_name}> loaded successfully.")
    
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
        
        self._user_access_token.check2refresh(client=self, force_refresh=False)
        
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
                token.refresh(client=self)
            
            headers = {
                "Authorization": f"Bearer {token.user_access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            resp = lark_request.request(
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
                             filter_conditions: str,
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
            filter_conditions: str, filter conditions to delete records
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
        result = lark_request.request(
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
    
    
    
    def add_table_records(self,
                          table_name: str,
                          records: List[dict],
                          workspace_id: str = None,
                          columns: List[str] = None,
                          on_conflict: List[str] = None,
                          prefer: str="missing=default") -> dict:
        """Add records to a table in a workspace.

        `UPSERT` method operations records. if 'columns' is provided, it will update the value specified in those columns.
        If 'on_conflict' is provided, it will determine the conflict resolution strategy, like UNIQUE constraint.
        `Prefer` is a header to specify update behavior.
        
        Docs:
        ------------
        Origin Doc link: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/apaas-v1/workspace-table/records_post
        UPSERT reference: https://docs.postgrest.org/en/v13/references/api/tables_views.html#upsert
        
        Args:
        ------------
            workspace_id: target workspace id (optional if client already has one)
            table_name: target table name
            records: list of record dicts following aPaaS records format, like [{"field_1": "value1", "field_2": "value2"}, ...]
            columns: list of column names to update if record exists. API needs `str`, but can
                pass list and will be converted to comma-separated string.
            on_conflict: list of column names to determine conflict resolution (for UPSERT). API needs `str`, but can
                pass list and will be converted to comma-separated string.
            prefer: str, Prefer header value to specify update behavior, besides can specified multiple options:
                * missing=default: use default value if column value is missing
                * resolution=merge-duplicates: when duplicated record update the 
                    record if primary key or unique constraint matches. Insert 
                    record if it's new
                * resolution=ignore-conflicts: when duplicated record, ignore
        Returns:
        -------------
            dict: parsed response from Feishu aPaaS endpoint

        Raises:
            ValueError: when token is missing or table not found
            LarkException: when API returns an error code
        """
        import json
        
        # import ipdb; ipdb.set_trace()
        if workspace_id is None:
            workspace_id = self.workspace_id
        else:
            # ensure tables list is loaded for the provided workspace
            self.list_workspace_tables(workspace_id=workspace_id)

        if not any(t.table_name == table_name for t in self._tables):
            logger.error(f"Table {table_name} not found in workspace {workspace_id}")
            raise ValueError(f"Table {table_name} not found in workspace {workspace_id}")


        


        url = APaaSURL.ADD_TABLE_RECORDS.value.format(
            workspace_id=workspace_id,
            table_name=table_name
        )

        # fix columns and on_conflict to str if they are list
        params = {}
        if columns and isinstance(columns, list):
            params["columns"] = ",".join(columns)
        if on_conflict and isinstance(on_conflict, list):
            params["on_conflict"] = ",".join(on_conflict)
        
        
        # update url
        url = f"{url}{ '?' + urlencode(params) if params else ''}" 
        
        record_num = 0
        for batch_records in data_generator(records, batch_size=self.ADD_RECORD_LIMITATION):
            # upate user access token
            self._user_access_token.check2refresh(client=self, force_refresh=False)
            token = self.user_access_token
            
            if not token:
                raise ValueError("User access token is required to add table records")
            
            record_num += len(batch_records)
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            # if there is prefer update headers
            if len(prefer) > 0:
                headers["Prefer"] = prefer
                
            payload = {
                "records": json.dumps(batch_records, ensure_ascii=False)
            }

            logger.info(f"Adding {record_num}/{len(records)} record(s) to workspace({workspace_id}) table {table_name}")

            
            result = lark_request.request(
                method="POST",
                url=url,
                headers=headers,
                payload=payload
            )

            if result.get("code") != 0:
                error_msg = result.get("msg") or "Unknown error"
                logger.error(f"Failed to add records to table {table_name}: {error_msg}")
                raise LarkException(code=result.get("code"), msg=error_msg)
            
            time.sleep(1)  # avoid hitting rate limits

        logger.info(f"Records added successfully to workspace({workspace_id}) table ({table_name})")
        
        
        
    
    def query_user_info(self, user_access_token: str) -> dict:
        """Query user info using user_access_token.

        This implements the contract expected by `UserAccessToken.exchange_code_for_token`.
        """

        return LarkContact._query_user_info(user_access_token)
    
    

    def _extract_workspace_from_url(self, url: str) -> Optional[str]:
        """Extract workspace ID from aPaaS URL.

        Args:
            url (str): The aPaaS URL containing the workspace information.
        Returns:
            Optional[str]: The extracted workspace ID, or None if not found.
        """
        
        match = self._regex_pattern.match(url)
        if match:
            workspace_id = match.group("workspace_id")
            logger.info(f"Extracted workspace_id<{workspace_id}> from origin url {url}.")
            return workspace_id
        else:
            logger.error(f"Failed to extract workspace_id from url: {url}.")
            return None