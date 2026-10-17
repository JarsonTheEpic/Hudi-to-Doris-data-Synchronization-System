"""
Module: settings.py
Function: Load system configuration from environment variables.
Author: jason
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
# This allows configuration to be overridden per environment (dev/prod)
load_dotenv()


# ============================================================================
# Doris Connection
# ============================================================================

DORIS_HOST = os.getenv('DORIS_HOST', '127.0.0.1')
"""Doris FE host address. Default: 127.0.0.1"""

DORIS_PORT = int(os.getenv('DORIS_PORT', '9030'))
"""Doris FE MySQL protocol port. Default: 9030"""

DORIS_USER = os.getenv('DORIS_USER', 'root')
"""Doris username. Default: root"""

DORIS_PASSWORD = os.getenv('DORIS_PASSWORD', '')
"""Doris password. Default: empty string"""

DORIS_DATABASE = os.getenv('DORIS_DATABASE', 'com')
"""Default Doris database name where tables are located. Default: com"""


# ============================================================================
# Sync Execution
# ============================================================================

SYNC_RETRY_TIMES = int(os.getenv('SYNC_RETRY_TIMES', '3'))
"""Number of retry attempts when sync fails. Default: 3"""

SYNC_RETRY_INTERVAL = int(os.getenv('SYNC_RETRY_INTERVAL', '5'))
"""Retry interval in seconds. Default: 5"""

SYNC_SQL_TIMEOUT = int(os.getenv('SYNC_SQL_TIMEOUT', '3600'))
"""SQL execution timeout in seconds. Default: 3600 (1 hour)"""


# ============================================================================
# Data Volume Thresholds
# ============================================================================

DATA_VOLUME_WARN_THRESHOLD = int(os.getenv('DATA_VOLUME_WARN_THRESHOLD', '1000000'))
"""
If estimated rows exceed this, log a warning but continue sync.
Default: 1,000,000 rows.
"""

DATA_VOLUME_FAIL_THRESHOLD = int(os.getenv('DATA_VOLUME_FAIL_THRESHOLD', '3000000'))
"""
If estimated rows exceed this, skip the table and require manual intervention.
This prevents accidental massive data loads that could impact system performance.
Default: 3,000,000 rows.
"""


# ============================================================================
# Stuck Job Detection
# ============================================================================

STUCK_JOB_CLEANUP_MINUTES = int(os.getenv('STUCK_JOB_CLEANUP_MINUTES', '60'))
"""
If a job has been running for more than this many minutes, it is considered stuck.
A stuck job indicates the previous sync may have crashed or hung.
The next sync will clean it up before starting.
Default: 60 minutes.
"""


# ============================================================================
# Logging
# ============================================================================

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
"""Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default: INFO"""

LOG_FILE_PATH = os.getenv('LOG_FILE_PATH', 'logs/sync/sync.log')
"""Log file path relative to project root. Default: logs/sync/sync.log"""

LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', '10485760'))
"""Maximum size of a single log file in bytes. Default: 10MB"""

LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))
"""Number of backup log files to keep before rotation. Default: 5"""
