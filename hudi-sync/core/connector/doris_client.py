"""
Module: doris_client.py
Function: Provide database connection and SQL execution for Doris.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created

Why need a connector layer?
----------------------------------------------------------------
1. Encapsulate pymysql details - the rest of the code doesn't need to know how to connect
2. All SQL execution goes through this class - easy to add logging, metrics, or tracing
3. Single place to handle connection management, timeouts, and error handling
4. Easy to switch to a different database driver in the future without changing business code
"""

import pymysql
import logging

from config.settings import (
    DORIS_HOST,
    DORIS_PORT,
    DORIS_USER,
    DORIS_PASSWORD,
    DORIS_DATABASE
)
from common.exceptions import ConnectionException

logger = logging.getLogger(__name__)


class DorisClient:
    """
    Doris database client for executing SQL queries and commands.

    Provides three main methods:
    - query(): Execute SELECT and return all rows
    - query_one(): Execute SELECT and return first row
    - execute(): Execute INSERT/UPDATE/DELETE and return affected row count

    The class manages its own connection and reconnects automatically if needed.
    """

    def __init__(self):
        """Initialize the client with connection parameters from settings."""
        self.host = DORIS_HOST
        self.port = DORIS_PORT
        self.user = DORIS_USER
        self.password = DORIS_PASSWORD
        self.database = DORIS_DATABASE
        self.connection = None

    def _connect(self) -> None:
        """
        Establish a connection to Doris.

        Raises:
            ConnectionException: If connection fails (network, auth, etc.)
        """
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                autocommit=True,
                connect_timeout=10,
                read_timeout=60,
                write_timeout=60
            )
            logger.info(f'Connected to Doris: {self.host}:{self.port}/{self.database}')
        except Exception as e:
            raise ConnectionException(f'Failed to connect to Doris: {e}')

    def _ensure_connection(self) -> None:
        """
        Ensure a connection exists. Reconnect if the connection is closed or broken.
        """
        if self.connection is None:
            self._connect()
            return

        try:
            # Test if connection is still alive
            self.connection.ping(reconnect=True)
        except Exception:
            # Connection is dead, reconnect
            self._connect()

    def _get_cursor(self):
        """
        Get a cursor for executing SQL.

        Returns:
            pymysql.cursors.DictCursor: Cursor that returns rows as dictionaries
        """
        self._ensure_connection()
        return self.connection.cursor(pymysql.cursors.DictCursor)

    def query(self, sql: str) -> list:
        """
        Execute a SELECT query and return all rows.

        Args:
            sql: SQL SELECT statement

        Returns:
            List of dictionaries, each representing a row.
            Returns empty list if no rows found.

        Raises:
            Exception: If SQL execution fails
        """
        logger.debug(f'Executing query: {sql[:200]}...')

        cursor = self._get_cursor()
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            logger.debug(f'Query returned {len(result)} rows')
            return result
        except Exception as e:
            logger.error(f'Query failed: {sql[:200]}... Error: {e}')
            raise
        finally:
            cursor.close()

    def query_one(self, sql: str) -> dict:
        """
        Execute a SELECT query and return the first row.

        Args:
            sql: SQL SELECT statement

        Returns:
            Dictionary representing the first row, or None if no rows found.

        Raises:
            Exception: If SQL execution fails
        """
        result = self.query(sql)
        return result[0] if result else None

    def execute(self, sql: str) -> int:
        """
        Execute an INSERT, UPDATE, DELETE, or DDL statement.

        Args:
            sql: SQL statement to execute

        Returns:
            Number of rows affected (for INSERT/UPDATE/DELETE)

        Raises:
            Exception: If SQL execution fails
        """
        logger.debug(f'Executing SQL: {sql[:200]}...')

        cursor = self._get_cursor()
        try:
            affected = cursor.execute(sql)
            logger.debug(f'SQL executed, affected rows: {affected}')
            return affected
        except Exception as e:
            logger.error(f'SQL execution failed: {sql[:200]}... Error: {e}')
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        """
        Close the database connection.
        Should be called when the client is no longer needed.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info('Doris connection closed')