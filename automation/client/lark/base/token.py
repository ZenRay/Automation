#coding:utf8
"""Lark Client Base Class"""
import logging
import sqlite3
import json
import os
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
    _db_path = None
    _db_connection = None
    
    def __init__(self, 
                 user_id: Optional[str] = None,
                 user_name: Optional[str] = None, 
                 user_email: Optional[str] = None,
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
            user_id: Lark user ID
            user_name: User name
            user_email: User email
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
        self.user_name = user_name
        self.user_email = user_email
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expire_time = expire_time
        self.scopes = scopes or []
        self.status = status
        self.source = source
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()

    @classmethod
    def init_database(cls, db_path: str = "user_tokens.db"):
        """Initialize database connection and table structure"""
        cls._db_path = db_path
        cls._ensure_table_exists()

    @classmethod
    def _get_connection(cls) -> sqlite3.Connection:
        """Get database connection"""
        if cls._db_path is None:
            cls.init_database()
        
        # Ensure _db_path is not None
        if cls._db_path is None:
            raise ValueError("Database path not initialized")
            
        conn = sqlite3.connect(cls._db_path)
        conn.row_factory = sqlite3.Row  # Make query results accessible like dictionaries
        return conn

    @classmethod
    def _ensure_table_exists(cls):
        """Ensure database table exists"""
        conn = cls._get_connection()
        try:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS user_access_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT UNIQUE NOT NULL,
                    user_name TEXT,
                    user_email TEXT,
                    access_token TEXT NOT NULL,
                    refresh_token TEXT,
                    expire_time TEXT NOT NULL,
                                         scopes TEXT,  -- JSON format for storing list
                     status TEXT NOT NULL DEFAULT 'active',
                     source TEXT NOT NULL DEFAULT 'manual',
                     created_at TEXT NOT NULL,
                     updated_at TEXT NOT NULL
                 )
             ''')
             
             # Create indexes
            conn.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON user_access_tokens(user_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_user_email ON user_access_tokens(user_email)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON user_access_tokens(status)')
            
            conn.commit()
        finally:
            conn.close()

    def save(self) -> bool:
        """Save or update Token to database"""
        try:
            conn = self._get_connection()
            self.updated_at = datetime.now()
            
            if self.id is None:
                # Insert new record
                cursor = conn.execute('''
                    INSERT INTO user_access_tokens 
                    (user_id, user_name, user_email, access_token, refresh_token, 
                     expire_time, scopes, status, source, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    self.user_id, self.user_name, self.user_email,
                    self.access_token, self.refresh_token,
                    self.expire_time.isoformat() if self.expire_time else None,
                    json.dumps(self.scopes) if self.scopes else None,
                    self.status.value, self.source,
                    self.created_at.isoformat(), self.updated_at.isoformat()
                ))
                self.id = cursor.lastrowid
            else:
                # Update existing record
                conn.execute('''
                    UPDATE user_access_tokens SET
                    user_name=?, user_email=?, access_token=?, refresh_token=?,
                    expire_time=?, scopes=?, status=?, source=?, updated_at=?
                    WHERE id=?
                ''', (
                    self.user_name, self.user_email, self.access_token, self.refresh_token,
                    self.expire_time.isoformat() if self.expire_time else None,
                    json.dumps(self.scopes) if self.scopes else None,
                    self.status.value, self.source, self.updated_at.isoformat(),
                    self.id
                ))
            
            conn.commit()
            logger.info(f"Token saved successfully for user {self.user_id}")
            return True
            
        except sqlite3.IntegrityError as e:
            logger.error(f"Database integrity error: {e}")
            return False
        except Exception as e:
            logger.error(f"Error saving token: {e}")
            return False
        finally:
            conn.close()

    @classmethod
    def get_by_user_id(cls, user_id: str) -> Optional['UserAccessToken']:
        """Get Token by user ID"""
        try:
            conn = cls._get_connection()
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
        finally:
            conn.close()

    @classmethod
    def get_by_email(cls, email: str) -> Optional['UserAccessToken']:
        """Get Token by email"""
        try:
            conn = cls._get_connection()
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
        finally:
            conn.close()

    @classmethod
    def where(cls, **conditions) -> List['UserAccessToken']:
        """Query Token list by conditions"""
        try:
            conn = cls._get_connection()
            
            # Build WHERE clause
            where_parts = []
            params = []
            
            for key, value in conditions.items():
                if key in ['user_id', 'user_name', 'user_email', 'status', 'source']:
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
        finally:
            conn.close()

    @classmethod
    def _from_db_row(cls, row: sqlite3.Row) -> 'UserAccessToken':
        """Create UserAccessToken instance from database row"""
        return cls(
            id=row['id'],
            user_id=row['user_id'],
            user_name=row['user_name'],
            user_email=row['user_email'],
            access_token=row['access_token'],
            refresh_token=row['refresh_token'],
            expire_time=datetime.fromisoformat(row['expire_time']) if row['expire_time'] else None,
            scopes=json.loads(row['scopes']) if row['scopes'] else [],
            status=TokenStatus(row['status']),
            source=row['source'],
            created_at=datetime.fromisoformat(row['created_at']),
            updated_at=datetime.fromisoformat(row['updated_at'])
        )

    @property
    def is_valid(self) -> bool:
        """Check if Token is valid"""
        if self.status != TokenStatus.ACTIVE:
            return False
        
        if not self.access_token:
            return False
            
        if self.expire_time and datetime.now() >= self.expire_time:
            self.status = TokenStatus.EXPIRED
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
            
        try:
            self.status = TokenStatus.REFRESHING
            self.save()
            
            # Call refresh function
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
            'user_name': self.user_name,
            'user_email': self.user_email,
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
        return f"UserAccessToken(user_id={self.user_id}, email={self.user_email}, status={self.status.value})"

    def __repr__(self) -> str:
        """Detailed representation"""
        return (f"UserAccessToken(id={self.id}, user_id={self.user_id}, "
                f"status={self.status.value}, valid={self.is_valid})")



