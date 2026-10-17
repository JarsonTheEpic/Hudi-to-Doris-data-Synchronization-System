"""
Module: pre_checker.py
Function: Perform environment checks before starting a sync job.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created

What does PreChecker do?
----------------------------------------------------------------
PreChecker validates the environment before any sync work begins.
If any check fails, the sync job is aborted immediately.

Why need pre-check?
    - Fail fast: detect problems early before wasting time on partial work
    - Clear error messages: tell the operator exactly what's wrong
    - Prevent zombie jobs: clean up stuck jobs from previous runs

Checks performed:
    1. Database connection - Can we connect to Doris?
    2. Project existence - Does project_code exist in project_base_info?
    3. Table configurations - Does the project have at least one active table mapping?
    4. Stuck jobs - Is there a previous job still running for this project?
"""

import logging

from core.connector.doris_client import DorisClient
from common.exceptions import (
    ConnectionException,
    ConfigNotFoundException,
    PreCheckFailedException
)
from config.settings import STUCK_JOB_CLEANUP_MINUTES

logger = logging.getLogger(__name__)


class PreChecker:
    """
    Perform environment checks before starting a sync job.

    All checks must pass for the sync to proceed.
    """

    def __init__(self):
        """Initialize the pre-checker with a Doris client."""
        self.client = DorisClient()

    def check_connection(self) -> bool:
        """
        Check if Doris connection is working.

        Executes a simple "SELECT 1" query to verify connectivity.

        Returns:
            bool: True if connection is working

        Raises:
            ConnectionException: If connection fails
        """
        logger.info('Checking Doris connection...')

        try:
            result = self.client.query_one('SELECT 1 as test')
            if result and result.get('test') == 1:
                logger.info('Doris connection OK')
                return True
            else:
                raise ConnectionException('Unexpected response from Doris')
        except Exception as e:
            raise ConnectionException(f'Doris connection failed: {e}')

    def check_project_exists(self, project_code: str) -> bool:
        """
        Check if the project exists in project_base_info.

        Args:
            project_code: Project code to check

        Returns:
            bool: True if project exists

        Raises:
            ConfigNotFoundException: If project does not exist
        """
        logger.info(f'Checking project existence: {project_code}')

        sql = f"""
            SELECT project_code, project_name, project_status
            FROM project_base_info
            WHERE project_code = '{project_code}'
        """

        result = self.client.query_one(sql)

        if not result:
            raise ConfigNotFoundException(f'Project not found: {project_code}')

        project_status = result.get('project_status')
        logger.info(f'Project found: {project_code}, status={project_status}')
        return True

    def check_table_configs_exist(self, project_code: str) -> bool:
        """
        Check if the project has at least one active table mapping.

        Args:
            project_code: Project code to check

        Returns:
            bool: True if at least one active config exists

        Raises:
            ConfigNotFoundException: If no active configurations found
        """
        logger.info(f'Checking table configurations for project: {project_code}')

        sql = f"""
            SELECT COUNT(*) as cnt
            FROM project_table_mapping
            WHERE project_code = '{project_code}'
              AND is_active = 1
        """

        result = self.client.query_one(sql)
        count = result.get('cnt', 0) if result else 0

        if count == 0:
            raise ConfigNotFoundException(
                f'No active table configurations found for project: {project_code}'
            )

        logger.info(f'Found {count} active table configurations')
        return True

    def check_stuck_jobs(self, project_code: str) -> bool:
        """
        Check if there is a stuck job for this project.

        A job is considered stuck if it has been running for more than
        STUCK_JOB_CLEANUP_MINUTES minutes.

        This prevents duplicate syncs when the previous job crashed or hung.

        Args:
            project_code: Project code to check

        Returns:
            bool: True if no stuck job exists (or stuck job cleaned up)

        Raises:
            PreCheckFailedException: If duplicate running job found and not expired
        """
        logger.info(f'Checking for stuck jobs for project: {project_code}')

        # Look for running jobs that started more than N minutes ago
        sql = f"""
            SELECT job_id, start_time,
                   TIMESTAMPDIFF(MINUTE, start_time, NOW()) as running_minutes
            FROM sync_job_log
            WHERE project_code = '{project_code}'
              AND status = 'running'
            ORDER BY start_time DESC
            LIMIT 1
        """

        result = self.client.query_one(sql)

        if not result:
            logger.info('No running jobs found for this project')
            return True

        job_id = result.get('job_id')
        running_minutes = result.get('running_minutes', 0)

        logger.info(f'Found running job: {job_id}, running for {running_minutes} minutes')

        if running_minutes > STUCK_JOB_CLEANUP_MINUTES:
            # Stuck job detected, clean it up
            logger.warning(
                f'Job {job_id} has been running for {running_minutes} minutes, '
                f'exceeds threshold of {STUCK_JOB_CLEANUP_MINUTES} minutes. '
                'Marking as failed and continuing.'
            )

            # Mark the stuck job as failed
            cleanup_sql = f"""
                UPDATE sync_job_log
                SET status = 'failed',
                    end_time = NOW(),
                    error_msg = 'Job was stuck and cleaned up by PreChecker'
                WHERE job_id = '{job_id}'
            """
            self.client.execute(cleanup_sql)

            logger.info(f'Stuck job {job_id} marked as failed')
            return True
        else:
            # Job is still running within normal time
            raise PreCheckFailedException(
                f'Another sync job is still running for project {project_code}. '
                f'Job ID: {job_id}, running for {running_minutes} minutes.'
            )

    def check_all_environment(self, project_code: str) -> bool:
        """
        Run all pre-checks in sequence.

        If any check fails, an exception is raised.
        All checks must pass for the sync to proceed.

        Args:
            project_code: Project code to check

        Returns:
            bool: True if all checks pass

        Raises:
            ConnectionException: If database connection fails
            ConfigNotFoundException: If project or configs not found
            PreCheckFailedException: If stuck job detected
        """
        logger.info('=' * 60)
        logger.info('Starting environment pre-checks')
        logger.info('=' * 60)

        # Check 1: Database connection
        self.check_connection()

        # Check 2: Project existence
        self.check_project_exists(project_code)

        # Check 3: Table configurations exist
        self.check_table_configs_exist(project_code)

        # Check 4: Stuck jobs
        self.check_stuck_jobs(project_code)

        logger.info('=' * 60)
        logger.info('All pre-checks passed!')
        logger.info('=' * 60)

        return True