"""
Module: constants.py
Function: Define system-wide constants for the Hudi to Doris sync service.
Author: jason
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created
"""


# ============================================================================
# Sync Mode Constants
# ============================================================================

SYNC_MODE_FULL = 'full'
"""
Full sync mode: truncate target table and reload all data from source.
"""

SYNC_MODE_INCREMENTAL = 'incremental'
"""
Incremental sync mode: only insert new data since last sync based on _hoodie_commit_time.
"""

SYNC_MODE_DEFAULT = SYNC_MODE_INCREMENTAL
"""
Default sync mode when not specified.
"""


# ============================================================================
# Status Constants
# ============================================================================

STATUS_RUNNING = 'running'
"""
Job or detail is currently running.
"""

STATUS_SUCCESS = 'success'
"""
Job or detail completed successfully.
"""

STATUS_FAILED = 'failed'
"""
Job or detail failed.
"""

STATUS_SKIP = 'skip'
"""
Detail was skipped (e.g., schema validation failed).
"""

STATUS_PARTIAL = 'partial'
"""
Job completed with partial success (some tables failed or skipped).
"""


# ============================================================================
# Exit Code Constants (for DolphinScheduler)
# ============================================================================

EXIT_SUCCESS = 0
"""
All tables synced successfully.
"""

EXIT_ALL_FAILED = 1
"""
All tables failed.
"""

EXIT_PARTIAL = 2
"""
Partial success (some tables failed or skipped).
"""

EXIT_ABNORMAL = 3
"""
Abnormal termination (exception or environment check failed).
"""


# ============================================================================
# Hudi System Fields (excluded from data sync)
# ============================================================================

HUDI_SYSTEM_FIELDS = {
    '_hoodie_commit_time',
    '_hoodie_commit_seqno',
    '_hoodie_record_key',
    '_hoodie_partition_path',
    '_hoodie_file_name',
}
"""
Hudi system fields that are automatically added by Hudi.
These fields are excluded from SELECT and INSERT statements during sync.
"""


# ============================================================================
# Doris Internal Catalog
# ============================================================================

DORIS_INTERNAL_CATALOG = 'internal'
"""
Default Doris internal catalog name.
"""


# ============================================================================
# Default Values
# ============================================================================

DEFAULT_SYNC_ORDER = 0
"""
Default sync order value.
"""

DEFAULT_IS_ACTIVE = 1
"""
Default is_active value: 1 = enabled.
"""

DEFAULT_RETRY_TIMES = 3
"""
Default number of retry attempts on failure.
"""

DEFAULT_RETRY_INTERVAL = 5
"""
Default retry interval in seconds.
"""


# ============================================================================
# Stuck Job Cleanup
# ============================================================================

STUCK_JOB_CLEANUP_MINUTES = 60
"""
If a job has been running for more than this many minutes, it is considered stuck.
"""


# ============================================================================
# Data Volume Thresholds (for safety)
# ============================================================================

DATA_VOLUME_WARN_THRESHOLD = 1000000
"""
If estimated rows exceed this, log a warning but continue sync.
"""

DATA_VOLUME_FAIL_THRESHOLD = 3000000
"""
If estimated rows exceed this, skip the table and require manual intervention.
"""
