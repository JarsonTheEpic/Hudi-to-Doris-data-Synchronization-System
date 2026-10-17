"""
Module: project_worker.py
Function: Orchestrate the sync process for all tables in a project.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created
    - 2026-07-21: Updated to match actual table schema
                  sync_job_log: sync_mode → sync_type (handled in logger.py)
                  sync_detail_log: hudi_schema → hudi_schema_name (handled in logger.py)

What does ProjectWorker do?
----------------------------------------------------------------
ProjectWorker is the core orchestrator. It coordinates the entire sync process:

1. Read all table configurations from project_table_mapping
2. For each table, orchestrate the sync flow:
   a. Validate schema (field count and order)
   b. Validate primary key (warning only, non-blocking)
   c. Read checkpoint (last_commit_time)
   d. Determine sync mode (full or incremental)
   e. Execute the sync (call FullSync or IncrementalSync)
   f. Update checkpoint
   g. Log detail start/end
3. Collect and return statistics

This is the main entry point called by sync_project.py.
"""

import logging
import time

from core.config_reader.table_config import TableConfigReader
from core.checkpoint.manager import CheckpointManager
from core.validator.schema_validator import SchemaValidator
from core.validator.primary_key_validator import PrimaryKeyValidator
from core.engine.full_sync import FullSync
from core.engine.incremental_sync import IncrementalSync
from core.worker.logger import LoggerManager
from common.exceptions import (
    ConfigNotFoundException,
    TableNotFoundException,
    SchemaNotMatchException,
    SyncFailedException,
    CheckpointException,
    DataVolumeExceedException
)
from common.utils import generate_detail_id, format_duration, get_current_time
from config.constants import (
    SYNC_MODE_FULL,
    SYNC_MODE_INCREMENTAL,
    SYNC_MODE_DEFAULT,
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_SKIP,
    DATA_VOLUME_WARN_THRESHOLD,
    DATA_VOLUME_FAIL_THRESHOLD
)
from config.settings import SYNC_RETRY_TIMES, SYNC_RETRY_INTERVAL

logger = logging.getLogger(__name__)


class ProjectWorker:
    """
    Orchestrate the sync process for all tables in a project.

    Responsibilities:
        - Read table configurations
        - Manage the sync flow for each table
        - Handle failures gracefully (skip failed tables, continue with others)
        - Collect and return statistics
    """

    def __init__(self):
        """Initialize the project worker with all required dependencies."""
        self.config_reader = TableConfigReader()
        self.checkpoint_manager = CheckpointManager()
        self.schema_validator = SchemaValidator()
        self.pk_validator = PrimaryKeyValidator()
        self.full_sync = FullSync()
        self.incremental_sync = IncrementalSync()
        self.logger_manager = LoggerManager()

    def _sync_single_table(
        self,
        config: dict,
        job_id: str,
        sync_mode_override: str,
        created_by: str
    ) -> dict:
        """
        Sync a single table (internal method).

        This method orchestrates the complete sync flow for one table:
        1. Generate detail_id
        2. Log detail start
        3. Validate schema (blocking)
        4. Validate primary key (warning only)
        5. Read checkpoint
        6. Determine actual sync mode
        7. Estimate data volume
        8. Execute sync
        9. Update checkpoint
        10. Log detail end

        Args:
            config: Table configuration from project_table_mapping
            job_id: Parent job ID
            sync_mode_override: Override from command line (full/incremental/None)
            created_by: User who triggered the sync

        Returns:
            dict: Result with keys: table_name, status, rows_synced, error_msg, detail_id
        """
        # Extract configuration values
        project_code = config.get('project_code')
        schema_name = config.get('schema_name')
        table_name = config.get('table_name')
        hudi_catalog = config.get('hudi_catalog')
        hudi_schema = config.get('hudi_schema')
        hudi_table = config.get('hudi_table')
        config_sync_mode = config.get('sync_mode', SYNC_MODE_DEFAULT)

        # Generate detail_id
        detail_id = generate_detail_id(job_id)

        # Determine actual sync mode
        if sync_mode_override:
            actual_sync_mode = sync_mode_override
        else:
            actual_sync_mode = config_sync_mode

        start_time = get_current_time()
        rows_synced = 0
        status = STATUS_SUCCESS
        error_msg = None
        source_filter = None

        logger.info(f'Starting sync for table: {schema_name}.{table_name}, mode={actual_sync_mode}')

        # Log detail start
        self.logger_manager.log_detail_start(
            detail_id=detail_id,
            job_id=job_id,
            project_code=project_code,
            schema_name=schema_name,
            table_name=table_name,
            hudi_catalog=hudi_catalog,
            hudi_schema=hudi_schema,
            hudi_table=hudi_table,
            sync_mode=actual_sync_mode,
            created_by=created_by
        )

        try:
            # Step 1: Schema validation
            logger.info(f'Validating schema for {schema_name}.{table_name}')
            self.schema_validator.validate(
                hudi_catalog=hudi_catalog,
                hudi_schema=hudi_schema,
                hudi_table=hudi_table,
                doris_schema=schema_name,
                doris_table=table_name
            )

            # Step 2: Primary key validation (warning only)
            logger.info(f'Validating primary key for {schema_name}.{table_name}')
            pk_match = self.pk_validator.validate(
                hudi_catalog=hudi_catalog,
                hudi_schema=hudi_schema,
                hudi_table=hudi_table,
                doris_schema=schema_name,
                doris_table=table_name
            )
            if not pk_match:
                logger.warning(f'Primary key mismatch for {schema_name}.{table_name}, continuing anyway')

            # Step 3: Read checkpoint (for incremental mode)
            last_commit_time = None
            if actual_sync_mode == SYNC_MODE_INCREMENTAL:
                logger.info(f'Reading checkpoint for {schema_name}.{table_name}')
                last_commit_time = self.checkpoint_manager.get_last_commit_time(
                    project_code=project_code,
                    schema_name=schema_name,
                    table_name=table_name
                )
                logger.info(f'Last commit time: {last_commit_time}')

                # If no checkpoint exists, fallback to full sync for first run
                if not last_commit_time:
                    logger.info(f'No checkpoint found for {schema_name}.{table_name}, falling back to full sync')
                    actual_sync_mode = SYNC_MODE_FULL

            # Step 4: Estimate data volume
            logger.info(f'Estimating data volume for {schema_name}.{table_name}')
            estimated_rows = self.checkpoint_manager.get_hudi_table_row_count(
                hudi_catalog=hudi_catalog,
                hudi_schema=hudi_schema,
                hudi_table=hudi_table,
                last_commit_time=last_commit_time if actual_sync_mode == SYNC_MODE_INCREMENTAL else None
            )

            logger.info(f'Estimated rows: {estimated_rows}')

            # Check data volume thresholds
            if estimated_rows > DATA_VOLUME_FAIL_THRESHOLD:
                raise DataVolumeExceedException(
                    f'Estimated rows {estimated_rows} exceeds fail threshold {DATA_VOLUME_FAIL_THRESHOLD}'
                )
            elif estimated_rows > DATA_VOLUME_WARN_THRESHOLD:
                logger.warning(
                    f'Large data volume: {estimated_rows} rows exceeds warning threshold {DATA_VOLUME_WARN_THRESHOLD}'
                )

            # Step 5: Execute sync (with retry)
            logger.info(f'Executing sync for {schema_name}.{table_name}, mode={actual_sync_mode}')
            rows_synced = self._execute_sync_with_retry(
                actual_sync_mode=actual_sync_mode,
                hudi_catalog=hudi_catalog,
                hudi_schema=hudi_schema,
                hudi_table=hudi_table,
                doris_schema=schema_name,
                doris_table=table_name,
                last_commit_time=last_commit_time
            )

            # Step 6: Update checkpoint (for incremental mode)
            if actual_sync_mode == SYNC_MODE_INCREMENTAL and rows_synced > 0:
                logger.info(f'Updating checkpoint for {schema_name}.{table_name}')
                max_commit = self.checkpoint_manager.get_hudi_max_commit_time(
                    hudi_catalog=hudi_catalog,
                    hudi_schema=hudi_schema,
                    hudi_table=hudi_table
                )

                if max_commit:
                    self.checkpoint_manager.update_checkpoint(
                        project_code=project_code,
                        schema_name=schema_name,
                        table_name=table_name,
                        last_commit_time=max_commit,
                        last_sync_rows=rows_synced,
                        created_by=created_by
                    )
                    source_filter = f'_hoodie_commit_time > {last_commit_time}'

            status = STATUS_SUCCESS
            logger.info(f'Sync completed successfully for {schema_name}.{table_name}, rows={rows_synced}')

        except SchemaNotMatchException as e:
            status = STATUS_SKIP
            error_msg = f'Schema validation failed: {e}'
            logger.warning(f'Skipping {schema_name}.{table_name}: {error_msg}')

        except DataVolumeExceedException as e:
            status = STATUS_SKIP
            error_msg = f'Data volume exceeds threshold: {e}'
            logger.warning(f'Skipping {schema_name}.{table_name}: {error_msg}')

        except (SyncFailedException, TableNotFoundException, CheckpointException) as e:
            status = STATUS_FAILED
            error_msg = str(e)
            logger.error(f'Sync failed for {schema_name}.{table_name}: {error_msg}')

        except Exception as e:
            status = STATUS_FAILED
            error_msg = f'Unexpected error: {e}'
            logger.error(f'Unexpected error for {schema_name}.{table_name}: {error_msg}', exc_info=True)

        finally:
            # Calculate duration
            end_time = get_current_time()
            duration_ms = format_duration(start_time, end_time)

            # Log detail end
            self.logger_manager.log_detail_end(
                detail_id=detail_id,
                rows_synced=rows_synced if status == STATUS_SUCCESS else 0,
                duration_ms=duration_ms,
                status=status,
                error_msg=error_msg,
                source_filter=source_filter
            )

        return {
            'table_name': f'{schema_name}.{table_name}',
            'status': status,
            'rows_synced': rows_synced,
            'error_msg': error_msg,
            'detail_id': detail_id,
            'duration_ms': duration_ms
        }

    def _execute_sync_with_retry(
        self,
        actual_sync_mode: str,
        hudi_catalog: str,
        hudi_schema: str,
        hudi_table: str,
        doris_schema: str,
        doris_table: str,
        last_commit_time: str = None
    ) -> int:
        """
        Execute sync with retry logic.

        Args:
            actual_sync_mode: full or incremental
            hudi_catalog: Hudi catalog name
            hudi_schema: Hudi schema name
            hudi_table: Hudi table name
            doris_schema: Doris schema name
            doris_table: Doris table name
            last_commit_time: Last commit time for incremental sync

        Returns:
            int: Number of rows synced

        Raises:
            SyncFailedException: If all retry attempts fail
        """
        last_error = None

        for attempt in range(SYNC_RETRY_TIMES):
            try:
                if actual_sync_mode == SYNC_MODE_FULL:
                    rows_synced = self.full_sync.sync(
                        hudi_catalog=hudi_catalog,
                        hudi_schema=hudi_schema,
                        hudi_table=hudi_table,
                        doris_schema=doris_schema,
                        doris_table=doris_table
                    )
                else:  # incremental
                    rows_synced = self.incremental_sync.sync(
                        hudi_catalog=hudi_catalog,
                        hudi_schema=hudi_schema,
                        hudi_table=hudi_table,
                        doris_schema=doris_schema,
                        doris_table=doris_table,
                        last_commit_time=last_commit_time
                    )

                return rows_synced

            except Exception as e:
                last_error = e
                if attempt < SYNC_RETRY_TIMES - 1:
                    wait_time = SYNC_RETRY_INTERVAL * (attempt + 1)
                    logger.warning(
                        f'Sync attempt {attempt + 1} failed: {e}, retrying in {wait_time}s...'
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f'All {SYNC_RETRY_TIMES} sync attempts failed')

        raise SyncFailedException(f'Sync failed after {SYNC_RETRY_TIMES} attempts: {last_error}')

    def run(
        self,
        project_code: str,
        job_id: str,
        sync_mode_override: str = None,
        created_by: str = 'system'
    ) -> tuple:
        """
        Run the sync process for all tables in a project.

        Args:
            project_code: Project code to sync
            job_id: Parent job ID
            sync_mode_override: Override from command line (full/incremental/None)
            created_by: User who triggered the sync

        Returns:
            tuple: (success_count, fail_count, skip_count)
        """
        logger.info(f'ProjectWorker started: project={project_code}, job={job_id}')

        # Step 1: Read table configurations
        try:
            tables = self.config_reader.get_tables_by_project(project_code)
            logger.info(f'Found {len(tables)} active tables for project {project_code}')
        except ConfigNotFoundException as e:
            logger.error(f'No tables found for project {project_code}: {e}')
            return (0, 0, 0)

        # Step 2: Process each table
        success_count = 0
        fail_count = 0
        skip_count = 0

        for config in tables:
            result = self._sync_single_table(
                config=config,
                job_id=job_id,
                sync_mode_override=sync_mode_override,
                created_by=created_by
            )

            if result['status'] == STATUS_SUCCESS:
                success_count += 1
            elif result['status'] == STATUS_SKIP:
                skip_count += 1
            else:  # STATUS_FAILED
                fail_count += 1

            logger.info(
                f'Table result: {result["table_name"]} '
                f'status={result["status"]}, rows={result["rows_synced"]}'
            )

        logger.info(
            f'ProjectWorker completed: success={success_count}, '
            f'fail={fail_count}, skip={skip_count}'
        )

        return (success_count, fail_count, skip_count)