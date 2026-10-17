"""
Module: logger.py
Function: Write job and detail logs to Doris tables.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created
    - 2026-07-21: Updated column names to match actual table schema
                  sync_job_log: sync_mode → sync_type
                  sync_detail_log: hudi_schema → hudi_schema_name
"""

import logging

from core.connector.doris_client import DorisClient
from common.exceptions import CheckpointException

logger = logging.getLogger(__name__)


class LoggerManager:
    """
    Write job and detail logs to Doris tables.

    Provides methods for:
        - Job log: start and end (sync_job_log table)
        - Detail log: start and end (sync_detail_log table)

    Usage pattern:
        1. log_job_start() when job begins
        2. For each table: log_detail_start() → sync → log_detail_end()
        3. log_job_end() when job finishes
    """

    def __init__(self):
        """Initialize the logger manager with a Doris client."""
        self.client = DorisClient()

    # ========================================================================
    # Job Log Methods
    # ========================================================================

    def log_job_start(
        self,
        job_id: str,
        project_code: str,
        sync_type: str,
        created_by: str = 'system'
    ) -> None:
        """
        Insert a job start record into sync_job_log.

        Args:
            job_id: Unique job ID
            project_code: Project being synced
            sync_type: full / incremental (maps to sync_type column)
            created_by: User who triggered the sync (default: system)

        Raises:
            CheckpointException: If insert fails
        """
        sql = f"""
            INSERT INTO sync_job_log (
                job_id,
                project_code,
                sync_type,
                trigger_type,
                start_time,
                status,
                created_by,
                created_at
            ) VALUES (
                '{job_id}',
                '{project_code}',
                '{sync_type}',
                'auto',
                NOW(),
                'running',
                '{created_by}',
                NOW()
            )
        """

        try:
            self.client.execute(sql)
            logger.info(f'Job log started: job_id={job_id}, project={project_code}')
        except Exception as e:
            raise CheckpointException(f'Failed to log job start: {e}')

    def log_job_end(
        self,
        job_id: str,
        total_tables: int,
        success_count: int,
        fail_count: int,
        skip_count: int,
        status: str,
        error_msg: str = None
    ) -> None:
        """
        Update job log with end status and statistics.

        Args:
            job_id: Unique job ID
            total_tables: Total number of tables processed
            success_count: Number of tables synced successfully
            fail_count: Number of tables that failed
            skip_count: Number of tables skipped
            status: success / failed / partial
            error_msg: Error message if job failed

        Raises:
            CheckpointException: If update fails
        """
        error_msg_sql = f"'{error_msg}'" if error_msg else 'NULL'

        sql = f"""
            UPDATE sync_job_log
            SET
                end_time = NOW(),
                total_tables = {total_tables},
                success_count = {success_count},
                fail_count = {fail_count},
                skip_count = {skip_count},
                status = '{status}',
                error_msg = {error_msg_sql}
            WHERE job_id = '{job_id}'
        """

        try:
            self.client.execute(sql)
            logger.info(f'Job log ended: job_id={job_id}, status={status}')
        except Exception as e:
            raise CheckpointException(f'Failed to log job end: {e}')

    # ========================================================================
    # Detail Log Methods
    # ========================================================================

    def log_detail_start(
        self,
        detail_id: str,
        job_id: str,
        project_code: str,
        schema_name: str,
        table_name: str,
        hudi_catalog: str,
        hudi_schema: str,
        hudi_table: str,
        sync_mode: str,
        created_by: str = 'system'
    ) -> None:
        """
        Insert a detail log record into sync_detail_log.

        Note: The actual table column is hudi_schema_name, but we accept hudi_schema
              as parameter name for consistency with other modules.

        Args:
            detail_id: Unique detail ID (format: job_id_seq)
            job_id: Parent job ID
            project_code: Project code
            schema_name: Doris schema name
            table_name: Doris table name
            hudi_catalog: Hudi catalog name
            hudi_schema: Hudi schema name (maps to hudi_schema_name column)
            hudi_table: Hudi table name
            sync_mode: full / incremental
            created_by: User who triggered the sync

        Raises:
            CheckpointException: If insert fails
        """
        sql = f"""
            INSERT INTO sync_detail_log (
                detail_id,
                job_id,
                project_code,
                schema_name,
                table_name,
                hudi_catalog,
                hudi_schema_name,
                hudi_table,
                sync_mode,
                start_time,
                status,
                created_by,
                created_at
            ) VALUES (
                '{detail_id}',
                '{job_id}',
                '{project_code}',
                '{schema_name}',
                '{table_name}',
                '{hudi_catalog}',
                '{hudi_schema}',
                '{hudi_table}',
                '{sync_mode}',
                NOW(),
                'running',
                '{created_by}',
                NOW()
            )
        """

        try:
            self.client.execute(sql)
            logger.debug(f'Detail log started: detail_id={detail_id}, table={table_name}')
        except Exception as e:
            raise CheckpointException(f'Failed to log detail start: {e}')

    def log_detail_end(
        self,
        detail_id: str,
        rows_synced: int,
        duration_ms: int,
        status: str,
        error_msg: str = None,
        source_filter: str = None
    ) -> None:
        """
        Update detail log with end status and statistics.

        Args:
            detail_id: Unique detail ID
            rows_synced: Number of rows synced (0 for failed/skipped)
            duration_ms: Duration in milliseconds
            status: success / failed / skip
            error_msg: Error message if failed
            source_filter: Filter condition used for incremental sync

        Raises:
            CheckpointException: If update fails
        """
        error_msg_sql = f"'{error_msg}'" if error_msg else 'NULL'
        source_filter_sql = f"'{source_filter}'" if source_filter else 'NULL'

        sql = f"""
            UPDATE sync_detail_log
            SET
                end_time = NOW(),
                duration_ms = {duration_ms},
                rows_synced = {rows_synced},
                status = '{status}',
                error_msg = {error_msg_sql},
                source_filter = {source_filter_sql}
            WHERE detail_id = '{detail_id}'
        """

        try:
            self.client.execute(sql)
            logger.debug(f'Detail log ended: detail_id={detail_id}, status={status}')
        except Exception as e:
            raise CheckpointException(f'Failed to log detail end: {e}')