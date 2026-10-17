#!/usr/bin/env python3
"""
Module: sync_project.py
Function: Main entry point for Hudi to Doris project sync (called by DolphinScheduler).
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created
    - 2026-07-21: Updated log_job_start parameter: sync_mode → sync_type (matches actual table schema)

What does this script do?
----------------------------------------------------------------
1. Parse command line arguments (project_code, sync_mode)
2. Generate a unique job_id
3. Run environment pre-checks (Doris connection, project exists, configs exist)
4. Log job start to sync_job_log table
5. Call ProjectWorker to sync all tables in the project
6. Log job end with statistics
7. Return exit code to DolphinScheduler

Exit Codes (for DolphinScheduler):
    0 - All tables synced successfully
    1 - All tables failed
    2 - Partial success (some tables failed or skipped)
    3 - Abnormal termination (exception or pre-check failed)

Usage Examples:
    # Incremental sync (default)
    python bin/sync_project.py --project_code demo_project

    # Force full sync
    python bin/sync_project.py --project_code demo_project --mode full

    # Force incremental sync
    python bin/sync_project.py --project_code demo_project --mode incremental
"""

import sys
import os
import argparse
import logging
import logging.config

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load logging configuration
LOG_CONF_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'config',
    'logging.conf'
)

# Create log directory if it doesn't exist
log_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'logs',
    'sync'
)
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

if os.path.exists(LOG_CONF_PATH):
    logging.config.fileConfig(LOG_CONF_PATH)
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

logger = logging.getLogger(__name__)

# Import business modules (after logging is configured)
from common.utils import generate_job_id, reset_detail_counter, get_current_time_str
from common.exceptions import (
    SyncException,
    ConnectionException,
    ConfigNotFoundException,
    PreCheckFailedException
)
from config.constants import (
    SYNC_MODE_INCREMENTAL,
    SYNC_MODE_FULL,
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_PARTIAL,
    EXIT_SUCCESS,
    EXIT_ALL_FAILED,
    EXIT_PARTIAL,
    EXIT_ABNORMAL
)
from core.pre_checker import PreChecker
from core.worker.project_worker import ProjectWorker
from core.worker.logger import LoggerManager


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments with project_code and mode

    Examples:
        python bin/sync_project.py --project_code demo_project
        python bin/sync_project.py --project_code demo_project --mode full
    """
    parser = argparse.ArgumentParser(
        description='Hudi to Doris project sync tool (for DolphinScheduler)'
    )

    parser.add_argument(
        '--project_code',
        '-p',
        required=True,
        help='Project code to sync (required)'
    )

    parser.add_argument(
        '--mode',
        '-m',
        default=None,
        choices=[SYNC_MODE_INCREMENTAL, SYNC_MODE_FULL],
        help=f'Sync mode: {SYNC_MODE_INCREMENTAL} or {SYNC_MODE_FULL}. '
             'If not specified, uses each table\'s configured mode.'
    )

    return parser.parse_args()


def main() -> None:
    """
    Main entry point.

    Orchestrates the entire sync process from start to finish.
    """
    # Parse arguments
    args = parse_args()
    project_code = args.project_code
    sync_mode = args.mode

    logger.info('=' * 70)
    logger.info(f'Starting sync for project: {project_code}')
    if sync_mode:
        logger.info(f'Sync mode override: {sync_mode}')
    else:
        logger.info('Sync mode: using table configuration')
    logger.info('=' * 70)

    # Step 1: Run environment pre-checks
    logger.info('Running environment pre-checks...')
    try:
        pre_checker = PreChecker()
        pre_checker.check_all_environment(project_code)
    except (ConnectionException, ConfigNotFoundException, PreCheckFailedException) as e:
        logger.error(f'Pre-check failed: {e}')
        sys.exit(EXIT_ABNORMAL)
    except Exception as e:
        logger.error(f'Unexpected error during pre-check: {e}', exc_info=True)
        sys.exit(EXIT_ABNORMAL)

    # Step 2: Generate job ID and reset detail counter
    job_id = generate_job_id()
    reset_detail_counter()
    logger.info(f'Job ID generated: {job_id}')

    # Step 3: Initialize logger manager
    logger_manager = LoggerManager()

    # Step 4: Log job start
    try:
        logger_manager.log_job_start(
            job_id=job_id,
            project_code=project_code,
            sync_type=sync_mode or SYNC_MODE_INCREMENTAL,  # Changed from sync_mode to sync_type
            created_by='system'
        )
        logger.info(f'Job start logged: {job_id}')
    except Exception as e:
        logger.error(f'Failed to log job start: {e}', exc_info=True)
        sys.exit(EXIT_ABNORMAL)

    # Step 5: Execute sync
    try:
        worker = ProjectWorker()
        success_count, fail_count, skip_count = worker.run(
            project_code=project_code,
            job_id=job_id,
            sync_mode_override=sync_mode,
            created_by='system'
        )

        total_tables = success_count + fail_count + skip_count

        # Determine final status
        if fail_count == total_tables and total_tables > 0:
            status = STATUS_FAILED
            error_msg = f'All tables failed: {fail_count} tables'
        elif fail_count > 0 or skip_count > 0:
            status = STATUS_PARTIAL
            error_msg = f'Partial: success={success_count}, fail={fail_count}, skip={skip_count}'
        else:
            status = STATUS_SUCCESS
            error_msg = None

        # Step 6: Log job end
        logger_manager.log_job_end(
            job_id=job_id,
            total_tables=total_tables,
            success_count=success_count,
            fail_count=fail_count,
            skip_count=skip_count,
            status=status,
            error_msg=error_msg
        )

        logger.info('=' * 70)
        logger.info(f'Project sync completed: {project_code}')
        logger.info(f'Status: {status}')
        logger.info(f'Success: {success_count}, Failed: {fail_count}, Skipped: {skip_count}')
        logger.info('=' * 70)

        # Step 7: Return exit code
        if status == STATUS_SUCCESS:
            sys.exit(EXIT_SUCCESS)
        elif status == STATUS_FAILED:
            sys.exit(EXIT_ALL_FAILED)
        else:  # STATUS_PARTIAL
            sys.exit(EXIT_PARTIAL)

    except Exception as e:
        logger.error(f'Project sync failed: {e}', exc_info=True)

        # Try to log job failure
        try:
            logger_manager.log_job_end(
                job_id=job_id,
                total_tables=0,
                success_count=0,
                fail_count=1,
                skip_count=0,
                status=STATUS_FAILED,
                error_msg=str(e)
            )
        except Exception:
            logger.error('Failed to log job failure', exc_info=True)

        sys.exit(EXIT_ABNORMAL)


if __name__ == '__main__':
    main()