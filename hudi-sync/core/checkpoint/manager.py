"""
Module: manager.py
Function: Manage sync checkpoints (last_commit_time) for each table.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created

What is a checkpoint?
----------------------------------------------------------------
A checkpoint stores the last Hudi commit_time that was successfully synced
for each table. This allows incremental sync to only pull data that has
been added since the last sync.

Checkpoint is stored in sync_hudi_checkpoint table with primary key:
(project_code, schema_name, table_name)
"""

from core.connector.doris_client import DorisClient
from common.exceptions import CheckpointException


class CheckpointManager:
    """
    Manage sync checkpoints for incremental sync.

    Provides methods to:
    - Get the last commit time for a table
    - Update the checkpoint after a successful sync
    - Query the max commit time from Hudi source table
    """

    def __init__(self):
        """Initialize the checkpoint manager with a Doris client."""
        self.client = DorisClient()

    def get_last_commit_time(self, project_code: str, schema_name: str, table_name: str) -> str:
        """
        Get the last synced Hudi commit time for a specific table.

        Args:
            project_code: Project code
            schema_name: Doris schema name
            table_name: Doris table name

        Returns:
            str: Last commit time in format YYYYMMDDHHMMSS, or None if no checkpoint exists

        Raises:
            CheckpointException: If query fails
        """
        sql = f"""
            SELECT last_commit_time
            FROM sync_hudi_checkpoint
            WHERE project_code = '{project_code}'
              AND schema_name = '{schema_name}'
              AND table_name = '{table_name}'
        """

        try:
            result = self.client.query_one(sql)
            return result.get('last_commit_time') if result else None
        except Exception as e:
            raise CheckpointException(
                f'Failed to get checkpoint for {project_code}.{schema_name}.{table_name}: {e}'
            )

    def update_checkpoint(
        self,
        project_code: str,
        schema_name: str,
        table_name: str,
        last_commit_time: str,
        last_sync_rows: int = 0,
        created_by: str = None
    ) -> None:
        """
        Update the checkpoint after a successful sync.

        Uses DELETE + INSERT pattern because Doris UNIQUE KEY table does not support
        INSERT ON DUPLICATE KEY UPDATE or REPLACE INTO natively in all versions.

        Args:
            project_code: Project code
            schema_name: Doris schema name
            table_name: Doris table name
            last_commit_time: New commit time to save (format: YYYYMMDDHHMMSS)
            last_sync_rows: Number of rows synced in this job
            created_by: User who executed the sync

        Raises:
            CheckpointException: If update fails
        """
        if created_by is None:
            created_by = 'system'

        sql = f"""
            DELETE FROM sync_hudi_checkpoint
            WHERE project_code = '{project_code}'
              AND schema_name = '{schema_name}'
              AND table_name = '{table_name}'
        """

        try:
            # Step 1: Delete existing checkpoint
            self.client.execute(sql)

            # Step 2: Insert new checkpoint
            insert_sql = f"""
                INSERT INTO sync_hudi_checkpoint (
                    project_code,
                    schema_name,
                    table_name,
                    last_commit_time,
                    last_sync_time,
                    last_sync_rows,
                    created_by,
                    created_at
                ) VALUES (
                    '{project_code}',
                    '{schema_name}',
                    '{table_name}',
                    '{last_commit_time}',
                    NOW(),
                    {last_sync_rows},
                    '{created_by}',
                    NOW()
                )
            """
            self.client.execute(insert_sql)

        except Exception as e:
            raise CheckpointException(
                f'Failed to update checkpoint for {project_code}.{schema_name}.{table_name}: {e}'
            )

    def get_hudi_max_commit_time(
        self,
        hudi_catalog: str,
        hudi_schema: str,
        hudi_table: str
    ) -> str:
        """
        Get the maximum _hoodie_commit_time from the Hudi source table.

        This represents the latest data available in the source table,
        used to determine if new data exists since last sync.

        Args:
            hudi_catalog: Hudi catalog name
            hudi_schema: Hudi schema name (may be empty)
            hudi_table: Hudi table name

        Returns:
            str: Max commit time in format YYYYMMDDHHMMSS, or None if table is empty

        Raises:
            CheckpointException: If query fails

        Note:
            If hudi_schema is empty, the table name is catalog.table
            Otherwise it is catalog.schema.table
        """
        if hudi_schema:
            full_table = f'{hudi_catalog}.{hudi_schema}.{hudi_table}'
        else:
            full_table = f'{hudi_catalog}.{hudi_table}'

        sql = f"""
            SELECT MAX(_hoodie_commit_time) as max_commit
            FROM {full_table}
        """

        try:
            result = self.client.query_one(sql)
            return result.get('max_commit') if result else None
        except Exception as e:
            raise CheckpointException(
                f'Failed to get max commit time from {full_table}: {e}'
            )

    def get_hudi_table_row_count(
        self,
        hudi_catalog: str,
        hudi_schema: str,
        hudi_table: str,
        last_commit_time: str = None
    ) -> int:
        """
        Get the number of rows in Hudi table, optionally filtered by commit time.

        Used for data volume estimation before sync.

        Args:
            hudi_catalog: Hudi catalog name
            hudi_schema: Hudi schema name (may be empty)
            hudi_table: Hudi table name
            last_commit_time: If provided, count rows with commit_time > this value

        Returns:
            int: Number of rows, or 0 if query fails

        Raises:
            CheckpointException: If query fails
        """
        if hudi_schema:
            full_table = f'{hudi_catalog}.{hudi_schema}.{hudi_table}'
        else:
            full_table = f'{hudi_catalog}.{hudi_table}'

        if last_commit_time:
            sql = f"""
                SELECT COUNT(*) as cnt
                FROM {full_table}
                WHERE _hoodie_commit_time > '{last_commit_time}'
            """
        else:
            sql = f"""
                SELECT COUNT(*) as cnt
                FROM {full_table}
            """

        try:
            result = self.client.query_one(sql)
            return result.get('cnt', 0) if result else 0
        except Exception as e:
            raise CheckpointException(
                f'Failed to get row count from {full_table}: {e}'
            )