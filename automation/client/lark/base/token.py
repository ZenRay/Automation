#coding:utf8
"""Lark Client Base Class"""
import logging
import sqlite3
import json
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Callable
from enum import Enum

from ..exceptions import LarkException

logger = logging.getLogger("automation.lark.base.token")


class TokenStatus(Enum):
    """Token status enumeration"""
    ACTIVE = "active"           # Active and usable
    EXPIRED = "expired"         # Expired
    INVALID = "invalid"         # Invalid
    REVOKED = "revoked"         # Revoked
    REFRESHING = "refreshing"   # Currently refreshing


class AccessToken:
    """Lark Access Token"""
    def __init__(self, 
                 tenant_access_token: Optional[str] = None, 
                 app_access_token: Optional[str] = None, 
                 expire_time: Optional[datetime] = None):
        """Access Token Information

        Args:
            tenant_access_token: Lark tenant access token
            app_access_token: Lark application access token
            expire_time: token expire datetime
        """
        self.tenant_access_token = tenant_access_token
        self.app_access_token = app_access_token
        self.expire_time = expire_time or datetime.min

    @property
    def is_valid(self) -> bool:
        """Check Token Validate"""
        has_tokens = (
            self.app_access_token is not None and 
            self.tenant_access_token is not None
        )

        is_not_expired = (
            self.expire_time is not None and 
            datetime.now() < self.expire_time
        )

        return has_tokens and is_not_expired


class UserAccessToken:
    """User Access Token with ORM and Auto-refresh Support"""
    # Class variables: database connection and table structure
    
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
        self.id = id
        self.user_id = user_id
        self.open_id = open_id
        self.union_id = union_id
        self.user_name = user_name
        self.en_name = en_name
        self.user_email = user_email
        self.mobile = mobile
        self.tenant_key = tenant_key
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expire_time = expire_time
        self.scopes = scopes or []
        self.status = status
        self.source = source
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
        # per-instance lock to protect concurrent modifications
        self._lock = threading.RLock()

    @classmethod
    def init_database(cls, db_path: str = "user_tokens.db"):
        """Initialize database connection and table structure"""
        # allow explicit path or try to read configuration lazily
        if db_path:
            cls._db_path = db_path
        else:
            # try to load config if available, but don't raise on import errors
            try:
                from ....conf import lark as _lark_conf
                if 'sqlite_db' in _lark_conf and 'DB_Name' in _lark_conf['sqlite_db']:
                    cls._db_path = _lark_conf['sqlite_db']['DB_Name']
            except Exception:
                cls._db_path = cls._db_path or None

        # fallback default
        if cls._db_path is None:
            cls._db_path = "user_tokens.db"

        # ensure directory exists
        db_dir = os.path.dirname(cls._db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

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
    def get_by_user_id(cls, user_id: str) -> Optional['UserAccessToken']:
        """Get Token by user ID"""
        try:
            with cls._get_connection() as conn:
                cursor = conn.execute(
                    'SELECT * FROM user_access_tokens WHERE user_id = ? ORDER BY updated_at DESC LIMIT 1',
                    (user_id,)
                )
                row = cursor.fetchone()

                if row:
                    return cls._from_db_row(row)
                return None

        except Exception as e:
            logger.error(f"Error getting token by user_id: {e}")
            return None

    @classmethod
    def get_by_email(cls, email: str) -> Optional['UserAccessToken']:
        """Get Token by email"""
        try:
            with cls._get_connection() as conn:
                cursor = conn.execute(
                    'SELECT * FROM user_access_tokens WHERE user_email = ? ORDER BY updated_at DESC LIMIT 1',
                    (email,)
                )
                row = cursor.fetchone()

                if row:
                    return cls._from_db_row(row)
                return None

        except Exception as e:
            logger.error(f"Error getting token by email: {e}")
            return None

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
            return False

        with self._lock:
            try:
                self.status = TokenStatus.REFRESHING
                # save status change; save() has its own locking and retry
                self.save()

                # Call refresh function (may block) — do not hold DB locks across this call
                new_token_data = refresh_func(self.refresh_token)

                if new_token_data:
                    self.access_token = new_token_data.get('access_token', self.access_token)
                    self.refresh_token = new_token_data.get('refresh_token', self.refresh_token)

                    # Update expiration time
                    if 'expires_in' in new_token_data:
                        self.expire_time = datetime.now() + timedelta(seconds=new_token_data['expires_in'])

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
                logger.error(f"Error refreshing token for user {self.user_id}: {e}")
                self.status = TokenStatus.INVALID
                try:
                    self.save()
                except Exception:
                    pass
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



