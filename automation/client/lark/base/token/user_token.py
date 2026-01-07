#coding:utf8
"""Lark Client Base Class"""
import logging
import sqlite3
import json
import os
import threading
import time
import requests
import webbrowser
import urllib.parse

from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Callable
from enum import Enum


from ...utils.oauth_local_server import OAuthCallbackServer
from ...exceptions import LarkException
from ...common import AuthURL

logger = logging.getLogger("automation.lark.base.token.user_token")


class TokenStatus(Enum):
    """Token status enumeration"""
    ACTIVE = "active"           # Active and usable
    EXPIRED = "expired"         # Expired
    INVALID = "invalid"         # Invalid
    REVOKED = "revoked"         # Revoked
    REFRESHING = "refreshing"   # Currently refreshing


class UserAccessToken:
    """User Access Token with ORM and Auto-refresh Support"""
    def __init__(self, 
                 user_id: Optional[str] = None,
                 open_id: Optional[str] = None,
                 union_id: Optional[str] = None,
                 user_name: Optional[str] = None,
                 en_name: Optional[str] = None,
                 user_email: Optional[str] = None,
                 mobile: Optional[str] = None,
                 tenant_key: Optional[str] = None,
                 access_token: Optional[str] = None,
                 refresh_token: Optional[str] = None,
                 expire_time: Optional[datetime] = None,
                 scopes: Optional[List[str]] = None,
                 status: TokenStatus = TokenStatus.ACTIVE,
                 source: str = "manual",
                 created_at: Optional[datetime] = None,
                 updated_at: Optional[datetime] = None,
                 id: Optional[int] = None):
        """UserAccessToken initialization

        Args:
            user_id: Lark user ID (depends on scope, might be null)
            open_id: User open ID (unique in app dimension)
            union_id: User union ID (unique across multiple apps)
            user_name: User name (Chinese name)
            en_name: User English name
            user_email: User email
            mobile: User mobile number
            tenant_key: Tenant key (organization ID)
            access_token: User access token
            refresh_token: Refresh token
            expire_time: Token expiration time
            scopes: List of permission scopes
            status: Token status
            source: Token source (manual, browser_auth, card_auth, etc.)
            created_at: Creation time
            updated_at: Last update time
            id: Database primary key ID
        """
        # store as private attributes; expose via properties
        self._id = id
        self._user_id = user_id
        self._open_id = open_id
        self._union_id = union_id
        self._user_name = user_name
        self._en_name = en_name
        self._user_email = user_email
        self._mobile = mobile
        self._tenant_key = tenant_key
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._expire_time = expire_time
        self._scopes = scopes or []
        self._status = status
        self._source = source
        self._created_at = created_at or datetime.now()
        self._updated_at = updated_at or datetime.now()
        # per-instance lock to protect concurrent modifications
        self._lock = threading.RLock()

    # -- Properties for private attributes (keep public API unchanged) --
    @property
    def id(self) -> Optional[int]:
        return self._id

    @id.setter
    def id(self, value: Optional[int]):
        self._id = value

    @property
    def user_id(self) -> Optional[str]:
        return self._user_id

    @user_id.setter
    def user_id(self, value: Optional[str]):
        self._user_id = value

    @property
    def open_id(self) -> Optional[str]:
        return self._open_id

    @open_id.setter
    def open_id(self, value: Optional[str]):
        self._open_id = value

    @property
    def union_id(self) -> Optional[str]:
        return self._union_id

    @union_id.setter
    def union_id(self, value: Optional[str]):
        self._union_id = value

    @property
    def user_name(self) -> Optional[str]:
        return self._user_name

    @user_name.setter
    def user_name(self, value: Optional[str]):
        self._user_name = value

    @property
    def en_name(self) -> Optional[str]:
        return self._en_name

    @en_name.setter
    def en_name(self, value: Optional[str]):
        self._en_name = value

    @property
    def user_email(self) -> Optional[str]:
        return self._user_email

    @user_email.setter
    def user_email(self, value: Optional[str]):
        self._user_email = value

    @property
    def mobile(self) -> Optional[str]:
        return self._mobile

    @mobile.setter
    def mobile(self, value: Optional[str]):
        self._mobile = value

    @property
    def tenant_key(self) -> Optional[str]:
        return self._tenant_key

    @tenant_key.setter
    def tenant_key(self, value: Optional[str]):
        self._tenant_key = value

    @property
    def access_token(self) -> Optional[str]:
        return self._access_token

    @access_token.setter
    def access_token(self, value: Optional[str]):
        self._access_token = value

    @property
    def refresh_token(self) -> Optional[str]:
        return self._refresh_token

    @refresh_token.setter
    def refresh_token(self, value: Optional[str]):
        self._refresh_token = value

    @property
    def expire_time(self) -> Optional[datetime]:
        return self._expire_time

    @expire_time.setter
    def expire_time(self, value: Optional[datetime]):
        self._expire_time = value

    @property
    def scopes(self) -> List[str]:
        return self._scopes

    @scopes.setter
    def scopes(self, value: List[str]):
        self._scopes = value or []

    @property
    def status(self) -> TokenStatus:
        return self._status

    @status.setter
    def status(self, value: TokenStatus):
        self._status = value

    @property
    def source(self) -> str:
        return self._source

    @source.setter
    def source(self, value: str):
        self._source = value

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @created_at.setter
    def created_at(self, value: datetime):
        self._created_at = value

    @property
    def updated_at(self) -> datetime:
        return self._updated_at

    @updated_at.setter
    def updated_at(self, value: datetime):
        self._updated_at = value

    @property
    def user_access_token(self) -> Optional[str]:
        """Get user access token (alias)"""
        return self._access_token
    
    
    
    @classmethod
    def init_database(cls, db_path:str = ".", db_name: str = "user_tokens.db"):
        """Initialize database connection and table structure"""
        # allow explicit path or try to read configuration lazily
        if not os.path.exists(db_path):
            os.makedirs(db_path, exist_ok=True)
            logger.debug(f"Created database directory at {os.path.abspath(db_path)}")
        cls._db_path = os.path.join(db_path, db_name)

        # ensure directory exists
        cls._ensure_table_exists()

    @classmethod
    def _get_connection(cls) -> sqlite3.Connection:
        """Get database connection"""
        if cls._db_path is None:
            cls.init_database()
        
        # Ensure _db_path is not None
        if cls._db_path is None:
            raise ValueError("Database path not initialized")
            
        # add a reasonable timeout to reduce write-lock failures
        conn = sqlite3.connect(cls._db_path, timeout=30)
        conn.row_factory = sqlite3.Row  # Make query results accessible like dictionaries
        try:
            # set busy timeout as well
            conn.execute('PRAGMA busy_timeout = 30000')
        except Exception:
            pass
        return conn

    @classmethod
    def _ensure_table_exists(cls):
        """Ensure database table exists"""
        with cls._get_connection() as conn:
            # improve concurrency characteristics
            try:
                conn.execute('PRAGMA journal_mode = WAL')
                conn.execute('PRAGMA synchronous = NORMAL')
            except Exception:
                pass
            
            sql="SELECT name FROM sqlite_master WHERE type='table' AND name='user_access_tokens'"
            if not conn.execute(sql).fetchone():
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS user_access_tokens (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT,
                        open_id TEXT,
                        union_id TEXT,
                        user_name TEXT,
                        en_name TEXT,
                        user_email TEXT,
                        mobile TEXT,
                        tenant_key TEXT,
                        access_token TEXT NOT NULL,
                        refresh_token TEXT,
                        expire_time TEXT,
                        scopes TEXT,
                        status TEXT NOT NULL DEFAULT 'active',
                        source TEXT NOT NULL DEFAULT 'manual',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE(open_id, tenant_key)
                    )
                ''')

                # Create indexes
                conn.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON user_access_tokens(user_id)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_open_id ON user_access_tokens(open_id)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_union_id ON user_access_tokens(union_id)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_user_email ON user_access_tokens(user_email)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON user_access_tokens(status)')
                conn.commit()
            logger.info("UserAccessToken database initialized successfully")

    def save(self) -> bool:
        """Save or update Token to database"""
        # protect instance state and retry on busy/locked errors
        with self._lock:
            attempts = 5
            backoff = 0.1
            for attempt in range(1, attempts + 1):
                try:
                    self.updated_at = datetime.now()
                    # use context manager for safe connection handling
                    with self._get_connection() as conn:
                        if self.id is None:
                            # Upsert based on unique (open_id, tenant_key)
                            cursor = conn.execute('''
                                INSERT INTO user_access_tokens 
                                (user_id, open_id, union_id, user_name, en_name, user_email, mobile, tenant_key,
                                 access_token, refresh_token, expire_time, scopes, status, source, created_at, updated_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(open_id, tenant_key) DO UPDATE SET
                                    user_id=excluded.user_id,
                                    union_id=excluded.union_id,
                                    user_name=excluded.user_name,
                                    en_name=excluded.en_name,
                                    user_email=excluded.user_email,
                                    mobile=excluded.mobile,
                                    access_token=excluded.access_token,
                                    refresh_token=excluded.refresh_token,
                                    expire_time=excluded.expire_time,
                                    scopes=excluded.scopes,
                                    status=excluded.status,
                                    source=excluded.source,
                                    updated_at=excluded.updated_at
                            ''', (
                                self.user_id, self.open_id, self.union_id,
                                self.user_name, self.en_name, self.user_email, self.mobile, self.tenant_key,
                                self.access_token, self.refresh_token,
                                self.expire_time.isoformat() if self.expire_time else None,
                                json.dumps(self.scopes),
                                self.status.value, self.source,
                                self.created_at.isoformat(), self.updated_at.isoformat()
                            ))

                            # ensure id is set (may be existing row)
                            if self.open_id and self.tenant_key:
                                row = conn.execute('SELECT id FROM user_access_tokens WHERE open_id = ? AND tenant_key = ?', 
                                                 (self.open_id, self.tenant_key)).fetchone()
                            elif self.user_id:
                                row = conn.execute('SELECT id FROM user_access_tokens WHERE user_id = ?', (self.user_id,)).fetchone()
                            else:
                                row = None
                            if row:
                                self.id = row['id']
                        else:
                            conn.execute('''
                                UPDATE user_access_tokens SET
                                user_id=?, open_id=?, union_id=?, user_name=?, en_name=?, user_email=?, mobile=?, tenant_key=?,
                                access_token=?, refresh_token=?, expire_time=?, scopes=?, status=?, source=?, updated_at=?
                                WHERE id=?
                            ''', (
                                self.user_id, self.open_id, self.union_id,
                                self.user_name, self.en_name, self.user_email, self.mobile, self.tenant_key,
                                self.access_token, self.refresh_token,
                                self.expire_time.isoformat() if self.expire_time else None,
                                json.dumps(self.scopes),
                                self.status.value, self.source, self.updated_at.isoformat(),
                                self.id
                            ))

                        conn.commit()
                        logger.info(f"Token saved successfully for user {self.user_id}")
                        return True

                except sqlite3.OperationalError as e:
                    msg = str(e).lower()
                    if 'locked' in msg or 'database is locked' in msg:
                        if attempt < attempts:
                            time.sleep(backoff)
                            backoff *= 2
                            continue
                        else:
                            logger.error(f"Database locked, max retries reached: {e}")
                            return False
                    else:
                        logger.error(f"Operational error saving token: {e}")
                        return False
                except sqlite3.IntegrityError as e:
                    logger.error(f"Database integrity error: {e}")
                    return False
                except Exception as e:
                    logger.error(f"Error saving token: {e}")
                    return False



    @classmethod
    def where(cls, **conditions) -> List['UserAccessToken']:
        """Query Token list by conditions"""
        try:
            with cls._get_connection() as conn:
                # Build WHERE clause
                where_parts = []
                params = []

                for key, value in conditions.items():
                    if key in ['user_id', 'open_id', 'union_id', 'user_name', 'en_name', 'user_email', 'mobile', 'tenant_key', 'status', 'source']:
                        where_parts.append(f"{key} = ?")
                        params.append(value.value if isinstance(value, TokenStatus) else value)

                where_clause = " AND ".join(where_parts) if where_parts else "1=1"
                query = f"SELECT * FROM user_access_tokens WHERE {where_clause} ORDER BY updated_at DESC"

                cursor = conn.execute(query, params)
                rows = cursor.fetchall()

                return [cls._from_db_row(row) for row in rows]

        except Exception as e:
            logger.error(f"Error in where query: {e}")
            return []

    @classmethod
    def _from_db_row(cls, row: sqlite3.Row) -> 'UserAccessToken':
        """Create UserAccessToken instance from database row"""
        def _parse_dt(s: Optional[str]) -> Optional[datetime]:
            if not s:
                return None
            try:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is not None:
                    dt = dt.astimezone().replace(tzinfo=None)
                return dt
            except Exception:
                return None

        return cls(
            id=row['id'],
            user_id=row.get('user_id'),
            open_id=row.get('open_id'),
            union_id=row.get('union_id'),
            user_name=row.get('user_name'),
            en_name=row.get('en_name'),
            user_email=row.get('user_email'),
            mobile=row.get('mobile'),
            tenant_key=row.get('tenant_key'),
            access_token=row['access_token'],
            refresh_token=row['refresh_token'],
            expire_time=_parse_dt(row['expire_time']),
            scopes=json.loads(row['scopes']) if row['scopes'] else [],
            status=TokenStatus(row['status']),
            source=row['source'],
            created_at=_parse_dt(row['created_at']) or datetime.now(),
            updated_at=_parse_dt(row['updated_at']) or datetime.now()
        )

    @property
    def is_valid(self) -> bool:
        """Check if Token is valid"""
        if self.status != TokenStatus.ACTIVE:
            return False

        if not self.access_token:
            return False

        if self.expire_time and datetime.now() >= self.expire_time:
            return False

        return True

    @property
    def is_expired(self) -> bool:
        """Check if Token is expired"""
        if not self.expire_time:
            return False
        return datetime.now() >= self.expire_time

    @property
    def expires_in_seconds(self) -> Optional[int]:
        """Get Token remaining valid time in seconds"""
        if not self.expire_time:
            return None
        
        delta = self.expire_time - datetime.now()
        return max(0, int(delta.total_seconds()))

    def needs_refresh(self, buffer_minutes: int = 10) -> bool:
        """Check if Token needs refresh
        
        Args:
            buffer_minutes: Buffer time for early refresh (minutes)
        """
        if not self.expire_time:
            return False
            
        buffer_time = datetime.now() + timedelta(minutes=buffer_minutes)
        return buffer_time >= self.expire_time

    def auto_refresh(self, refresh_func: Callable) -> bool:
        """Auto refresh Token
        
        Args:
            refresh_func: Function to refresh Token, should return new token info dict
        """
        if not self.refresh_token or self.status == TokenStatus.REFRESHING:
            logger.error(
                f"Cannot refresh token for user {self}: no refresh token or already refreshing"
            )
            return False

        with self._lock:
            try:
                self.status = TokenStatus.REFRESHING
                # save status change; save() has its own locking and retry
                self.save()

                # Call refresh function (may block) — do not hold DB locks across this call
                start_time = datetime.now()
                new_token_data = refresh_func(self.refresh_token)

                if new_token_data:
                    self.access_token = new_token_data.get('access_token', self.access_token)
                    self.refresh_token = new_token_data.get('refresh_token', self.refresh_token)

                    # Update expiration time
                    if 'expires_in' in new_token_data:
                        self.expire_time = start_time + timedelta(seconds=new_token_data['expires_in'])

                    self.status = TokenStatus.ACTIVE
                    success = self.save()

                    if success:
                        logger.info(f"Token refreshed successfully for user {self.user_id}")
                    return success
                else:
                    self.status = TokenStatus.INVALID
                    self.save()
                    return False

            except Exception as e:
                logger.error(f"Reefreshing token for user {self.user_id} failed: {e}")
                self.status = TokenStatus.INVALID
                self.save()
                return False


    def revoke(self) -> bool:
        """Revoke Token"""
        self.status = TokenStatus.REVOKED
        return self.save()


    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'open_id': self.open_id,
            'union_id': self.union_id,
            'user_name': self.user_name,
            'en_name': self.en_name,
            'user_email': self.user_email,
            'mobile': self.mobile,
            'tenant_key': self.tenant_key,
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'expire_time': self.expire_time.isoformat() if self.expire_time else None,
            'scopes': self.scopes,
            'status': self.status.value,
            'source': self.source,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'is_valid': self.is_valid,
            'expires_in_seconds': self.expires_in_seconds
        }

    def __str__(self) -> str:
        """String representation"""
        return f"UserAccessToken(open_id={self.open_id}, user_id={self.user_id}, email={self.user_email}, status={self.status.value})"

    def __repr__(self) -> str:
        """Detailed representation"""
        return (f"UserAccessToken(id={self.id}, open_id={self.open_id}, user_id={self.user_id}, "
            f"status={self.status.value})")

    # -- OAuth / lifecycle helpers to centralize token logic --
    @classmethod
    def from_oauth_response(cls, oauth_json: Dict[str, Any], user_info: Optional[Dict[str, Any]] = None, source: str = "oauth_authorization") -> 'UserAccessToken':
        """Create a UserAccessToken from an OAuth token response.

        Args:
        ---------
            oauth_json: Parsed JSON response from token endpoint.
            user_info: Optional dict with user fields (user_id, open_id, name, en_name, email, mobile, tenant_key)
            source: Source label for token
        Returns:
            UserAccessToken instance
        """
        access_token = oauth_json.get("access_token")
        refresh_token = oauth_json.get("refresh_token")
        expires_in = oauth_json.get("expires_in") or oauth_json.get("expires")
        scopes_raw = oauth_json.get("scope") or ""
        scopes = scopes_raw.split() if isinstance(scopes_raw, str) and scopes_raw else []

        expire_time = None
        try:
            if expires_in:
                expire_time = datetime.now() + timedelta(seconds=int(expires_in))
        except Exception:
            expire_time = None

        user_info = user_info or {}

        token = cls(
            user_id=user_info.get("user_id"),
            open_id=user_info.get("open_id"),
            union_id=user_info.get("union_id"),
            user_name=user_info.get("name") or user_info.get("user_name"),
            en_name=user_info.get("en_name"),
            user_email=user_info.get("email"),
            mobile=user_info.get("mobile"),
            tenant_key=user_info.get("tenant_key"),
            access_token=access_token,
            refresh_token=refresh_token,
            expire_time=expire_time,
            scopes=scopes,
            status=TokenStatus.ACTIVE,
            source=source
        )
        return token

    @classmethod
    def exchange_code_for_token(cls, code: str, redirect_uri: Optional[str] = None, client: Optional[object] = None, need_fetch_user_info: bool = True) -> 'UserAccessToken':
        """Exchange authorization code for user access token and construct UserAccessToken.

        Args:
        ----------
            code: Authorization code
            redirect_uri: Optional redirect uri
            client: Lark Client object or subclass object
            need_fetch_user_info: Whether to fetch user info after token exchange
                if `True`, the function will try to call `client.get_user_info_by_token()`
                and the client must can call this method.
        Returns:
            UserAccessToken instance
        """
        if client is None:
            raise LarkException("App client required: provide a Lark Client object")
        try:
            client_id = getattr(client, 'app_id', None)
            client_secret = getattr(client, 'app_secret', None)
        except Exception:
            raise LarkException("App credentials required: provide a `client` with `app_id` and `app_secret`")

        url = AuthURL.AUTH_USER_TOKEN.value
        headers = {"Content-Type": "application/json; charset=utf-8"}
        body = {
            "grant_type": "authorization_code",
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code
        }
        
        if redirect_uri:
            body["redirect_uri"] = redirect_uri

        resp = requests.post(url=url, headers=headers, json=body).json()

        # Normalize response: Feishu may wrap payload under 'data'
        data = resp.get('data') if isinstance(resp, dict) and resp.get('data') else resp


        # Extract tokens
        access_token = None
        refresh_token = None
        if isinstance(data, dict):
            access_token = data.get('access_token')
            refresh_token = data.get('refresh_token')

        # Fallback to top-level keys
        if not access_token:
            access_token = resp.get('access_token')
            refresh_token = resp.get('refresh_token')

        if not access_token:
            error_msg = resp.get("error_description") or resp.get("msg") or resp
            raise LarkException(f"Token exchange failed: {error_msg}")

        if need_fetch_user_info and access_token:
            if not (hasattr(client, 'query_user_info') and callable(getattr(client, 'query_user_info'))):
                raise LarkException("Client does not support fetching user info by token")
            user_info = client.query_user_info(access_token)
        else:
            user_info = None

        # Assemble token using normalized data (include refresh_token when present)
        oauth_payload = data if isinstance(data, dict) else resp
        if refresh_token is not None:
            oauth_payload = dict(oauth_payload)
            oauth_payload['refresh_token'] = refresh_token

        token = cls.from_oauth_response(oauth_payload, user_info=user_info, source="oauth_authorization")
        return token


    def refresh(self, http_client: Optional[Callable] = None, user_info_fetcher: Optional[Callable] = None, save: bool = True, client: Optional[object] = None) -> 'UserAccessToken':
        """Refresh this user's access token using the refresh_token.

        Args:
            client_id: App client id
            client_secret: App client secret
            http_client: Optional HTTP client callable
            user_info_fetcher: Optional callable to update user info given new access token
            save: Whether to persist changes via `save()`
        Returns:
            Updated self
        """
        if not self.refresh_token:
            raise LarkException("No refresh_token available")

        with self._lock:
            if self.status == TokenStatus.REFRESHING:
                # already refreshing in another thread; return current
                return self
            self.status = TokenStatus.REFRESHING
            try:
                # Resolve credentials from provided `client` (preferred)
                client_id = None
                client_secret = None
                if client is not None:
                    try:
                        client_id = getattr(client, 'app_id', None)
                        client_secret = getattr(client, 'app_secret', None)
                    except Exception:
                        client_id = client_secret = None

                if not client_id or not client_secret:
                    raise LarkException("App credentials required: provide a `client` with `app_id` and `app_secret`")

                if http_client is None:
                    try:
                        from ...utils import lark_request
                        http_client = lark_request.request
                    except Exception:
                        import requests
                        def http_client(method: str, url: str, headers=None, payload=None, params=None, json=None):
                            resp = requests.post(url, headers=headers, json=payload)
                            return resp.json()

                url = AuthURL.AUTH_USER_TOKEN.value
                headers = {"Content-Type": "application/json; charset=utf-8"}
                body = {
                    "grant_type": "refresh_token",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": self.refresh_token
                }

                result = http_client(method="POST", url=url, headers=headers, payload=body)
                if not isinstance(result, dict):
                    try:
                        result = result.json()
                    except Exception:
                        raise LarkException("Invalid refresh response")

                if result.get("code") != 0 and not result.get("access_token"):
                    error_msg = result.get("error_description") or result.get("msg") or result
                    self.status = TokenStatus.INVALID
                    if save:
                        try:
                            self.save()
                        except Exception:
                            pass
                    raise LarkException(f"Token refresh failed: {error_msg}")

                # update fields
                self.access_token = result.get("access_token")
                self.refresh_token = result.get("refresh_token") or self.refresh_token
                expires_in = result.get("expires_in")
                if expires_in:
                    try:
                        self.expire_time = datetime.now() + timedelta(seconds=int(expires_in))
                    except Exception:
                        self.expire_time = None

                scopes_raw = result.get("scope") or ""
                self.scopes = scopes_raw.split() if isinstance(scopes_raw, str) and scopes_raw else self.scopes

                # try to update user info
                if user_info_fetcher and self.access_token:
                    try:
                        user_info = user_info_fetcher(self.access_token)
                        if user_info:
                            self.user_id = user_info.get("user_id") or self.user_id
                            self.open_id = user_info.get("open_id") or self.open_id
                            self.union_id = user_info.get("union_id") or self.union_id
                            self.user_name = user_info.get("name") or self.user_name
                            self.en_name = user_info.get("en_name") or self.en_name
                            self.user_email = user_info.get("email") or self.user_email
                            self.mobile = user_info.get("mobile") or self.mobile
                            self.tenant_key = user_info.get("tenant_key") or self.tenant_key
                    except Exception:
                        pass

                self.status = TokenStatus.ACTIVE
                if save:
                    self.save()
                return self

            except Exception as e:
                logger.error(f"Error refreshing token for user {self.user_id}: {e}")
                self.status = TokenStatus.INVALID
                try:
                    if save:
                        self.save()
                except Exception:
                    pass
                raise

    @classmethod
    def get_user_access_token_interactive(
            cls, client: Optional[object] = None, scope: Optional[str] = None, 
            port: int = 8080, timeout: int = 120, auto_open_browser: bool = True
        ) -> 'UserAccessToken':
        """Interactive OAuth flow implemented at model level: starts local callback server, exchanges code and returns token instance.

        Args:
            scope: Optional scope string
            port: Local callback port
            timeout: Timeout seconds
            client: Lark client instance (must provide `app_id` and `app_secret`)
            auto_open_browser: Whether to open browser automatically
        Returns:
            UserAccessToken
        """

        if client is None:
            raise LarkException("App client required: provide a Lark Client object")

        client_id = getattr(client, 'app_id', None)
        client_secret = getattr(client, 'app_secret', None)
        if not client_id or not client_secret:
            raise LarkException("App credentials required: provide a `client` with `app_id` and `app_secret`")

        redirect = f"http://localhost:{port}/callback"

        # Build authorization url
        auth_base_url = AuthURL.AUTH_CODE.value
        params = {"client_id": client_id, "response_type": "code", "redirect_uri": redirect}
        if scope:
            params["scope"] = scope
        query_string = urllib.parse.urlencode(params)
        auth_url = f"{auth_base_url}?{query_string}"

        logger.info('%s', '\n' + '=' * 60)
        logger.info('飞书用户授权')
        logger.info('%s', '=' * 60)

        with OAuthCallbackServer(port=port) as server:
            logger.info('回调服务器: %s', server.redirect_uri)
            logger.info('授权范围: %s', scope or '（无/使用应用默认）')
            logger.info('超时时间: %s秒', timeout)

            if auto_open_browser:
                try:
                    webbrowser.open(auth_url)
                except Exception:
                    logger.info('请手动访问以下URL进行授权:\n%s', auth_url)
            else:
                logger.info('请手动访问以下URL进行授权:\n%s', auth_url)

            try:
                auth_code, state = server.wait_for_callback(timeout=timeout)
            except TimeoutError:
                raise

        # Exchange code
        token = cls.exchange_code_for_token(
            code=auth_code,
            redirect_uri=redirect,
            client=client,
            need_fetch_user_info=True
        )
        return token



