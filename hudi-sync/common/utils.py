"""
Module: utils.py
Function: Provide common utility functions for the sync service.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created
"""

import time
from datetime import datetime


# ============================================================================
# ID Generation
# ============================================================================

# Counter for generating detail_id sequence within a single job
# Reset to 0 for each new job
_detail_counter = 0


def generate_job_id() -> str:
    """
    Generate a unique job ID based on timestamp.

    Format: YYYYMMDDHHmmSS + 3-digit millisecond
    Example: 20260720143015123

    Why no sequence number?
    - Job ID is generated once per sync job (project level)
    - Timestamp is sufficient for uniqueness since jobs run serially
    - Simpler and more readable than adding sequence

    Returns:
        str: 17-character job ID
    """
    now = datetime.now()
    timestamp = now.strftime('%Y%m%d%H%M%S')
    millisecond = f'{now.microsecond // 1000:03d}'
    return timestamp + millisecond


def reset_detail_counter() -> None:
    """
    Reset the detail sequence counter to 0.
    Must be called at the beginning of each new job.
    """
    global _detail_counter
    _detail_counter = 0


def generate_detail_id(job_id: str) -> str:
    """
    Generate a unique detail ID for each table within a job.

    Format: job_id_seq
    Example: 20260720143015123_1, 20260720143015123_2, ...

    The sequence number starts from 1 and increments for each table.
    This ensures detail_id is unique and traceable back to the parent job.

    Args:
        job_id: The parent job ID

    Returns:
        str: detail ID in format job_id_seq
    """
    global _detail_counter
    _detail_counter += 1
    return f'{job_id}_{_detail_counter}'


# ============================================================================
# Time Utilities
# ============================================================================

def get_current_time() -> datetime:
    """
    Get the current datetime object.
    Use this instead of datetime.now() directly for consistency.

    Returns:
        datetime: Current datetime
    """
    return datetime.now()


def get_current_time_str() -> str:
    """
    Get the current time as formatted string.

    Returns:
        str: Current time in format 'YYYY-MM-DD HH:MM:SS'
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def format_duration(start_time: datetime, end_time: datetime) -> int:
    """
    Calculate duration between two timestamps in milliseconds.

    Args:
        start_time: Start datetime
        end_time: End datetime

    Returns:
        int: Duration in milliseconds
    """
    delta = end_time - start_time
    return int(delta.total_seconds() * 1000)


# ============================================================================
# String Utilities
# ============================================================================

def build_full_table_name(catalog: str, schema: str, table: str) -> str:
    """
    Build a fully qualified table name.

    If schema is empty or None, returns catalog.table.
    Otherwise returns catalog.schema.table.

    Args:
        catalog: Catalog name (e.g., 'internal', 'hudi')
        schema: Schema name (may be empty)
        table: Table name

    Returns:
        str: Fully qualified table name
    """
    if schema:
        return f'{catalog}.{schema}.{table}'
    return f'{catalog}.{table}'