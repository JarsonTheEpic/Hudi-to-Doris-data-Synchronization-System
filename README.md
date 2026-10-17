================================================================================
                    Hudi to Doris Sync Service - README
================================================================================

1. Overview
================================================================================

A lightweight Python service for synchronizing data from Hudi tables to Doris 
tables. Designed for production use with DolphinScheduler integration.

Features:
- Full sync: Replace all data in target table with source data
- Incremental sync: Only insert new data since last sync based on _hoodie_commit_time
- Schema validation: Verify column count and order before sync
- Checkpoint management: Track last sync position for incremental sync
- Job logging: Record sync history at both job and detail level
- Retry mechanism: Auto-retry failed syncs with exponential backoff
- Data volume safety: Skip large data loads that exceed configured thresholds
- Stuck job detection: Clean up hanging jobs automatically


2. Directory Structure
================================================================================

hudi-sync/
├── bin/
│   └── sync_project.py          # Main entry point
├── config/
│   ├── constants.py             # System constants
│   ├── settings.py              # Configuration from .env
│   └── logging.conf             # Logging configuration
├── core/
│   ├── connector/
│   │   └── doris_client.py      # Database connection
│   ├── config_reader/
│   │   └── table_config.py      # Read project_table_mapping
│   ├── checkpoint/
│   │   └── manager.py           # Checkpoint read/write
│   ├── validator/
│   │   ├── schema_validator.py  # Schema validation
│   │   └── primary_key_validator.py  # Primary key validation
│   ├── engine/
│   │   ├── full_sync.py         # Full sync execution
│   │   └── incremental_sync.py  # Incremental sync execution
│   ├── worker/
│   │   ├── project_worker.py    # Sync orchestration
│   │   └── logger.py            # Log writer
│   └── pre_checker.py           # Environment pre-checks
├── common/
│   ├── utils.py                 # Utility functions
│   ├── exceptions.py            # Custom exceptions
│   └── retry.py                 # Retry decorator
├── scripts/
│   └── init_db.sql              # Database initialization
├── .env                         # Environment variables (local only)
├── requirements.txt             # Python dependencies
└── README.txt                   # This file


3. Quick Start
================================================================================

Step 1: Install Dependencies
--------------------------------------------------------------------------------
pip install -r requirements.txt


Step 2: Configure Connection
--------------------------------------------------------------------------------
Create .env file with your Doris connection:

DORIS_HOST=127.0.0.1
DORIS_PORT=9030
DORIS_USER=root
DORIS_PASSWORD=your_password
DORIS_DATABASE=com


Step 3: Initialize Database Tables
--------------------------------------------------------------------------------
Run scripts/init_db.sql in Doris to create all required tables:
- project_base_info      # Project information
- project_table_mapping  # Table mapping configuration
- sync_hudi_checkpoint   # Sync checkpoint
- sync_job_log           # Job log
- sync_detail_log        # Detail log


Step 4: Insert Project and Table Configuration
--------------------------------------------------------------------------------
-- Add a project
INSERT INTO project_base_info (project_code, project_name, project_status)
VALUES ('demo_project', 'Demo Project', 'running');

-- Add table mapping
INSERT INTO project_table_mapping (
    project_code, schema_name, table_name,
    hudi_catalog, hudi_schema, hudi_table, sync_mode
) VALUES (
    'demo_project', 'ods', 'dim_gg_cgsjmx_sjbz',
    'hudi', 'default', 'dim_gg_cgsjmx_sjbz', 'incremental'
);


Step 5: Run Sync
--------------------------------------------------------------------------------
# Incremental sync (uses table configuration)
python bin/sync_project.py --project_code demo_project

# Force full sync
python bin/sync_project.py --project_code demo_project --mode full

# Force incremental sync
python bin/sync_project.py --project_code demo_project --mode incremental


4. Command Reference
================================================================================

Command                                          Description
--------------------------------------------------------------------------------
python bin/sync_project.py -p <project_code>     Incremental sync (default)
python bin/sync_project.py -p <project_code> -m full   Force full sync
python bin/sync_project.py -p <project_code> -m incremental  Force incremental sync


Parameter      Short    Description
--------------------------------------------------------------------------------
--project_code -p       Project code (required)
--mode         -m       Sync mode: full / incremental (optional)


5. Exit Codes (for DolphinScheduler)
================================================================================

Exit Code    Meaning
--------------------------------------------------------------------------------
0            All tables synced successfully
1            All tables failed
2            Partial success (some tables failed or skipped)
3            Abnormal termination (exception or pre-check failed)


6. Table Schemas
================================================================================

6.1 project_base_info - Project information
--------------------------------------------------------------------------------
Column              Type          Description
--------------------------------------------------------------------------------
project_code        VARCHAR(10)   Project code (primary key)
project_name        VARCHAR(200)  Project name
app_category        VARCHAR(50)   Application category
dept_sys_code       VARCHAR(10)   Department/System code
dept_sys_name       VARCHAR(200)  Department/System name
importance_level    VARCHAR(10)   high/medium/low
consumer            VARCHAR(100)  Consumer
involve_fill_calc   VARCHAR(10)   Involve fill calculation: 1=yes, 0=no
key_user            VARCHAR(100)  Key user
project_status      VARCHAR(20)   designing/developing/running/stopped
primary_owner       VARCHAR(100)  Primary owner
secondary_owner     VARCHAR(100)  Secondary owner
remark              VARCHAR(2000) Remark
created_by          VARCHAR(10)   Created by
updated_by          VARCHAR(10)   Updated by
created_at          DATETIME      Created time
updated_at          DATETIME      Updated time


6.2 project_table_mapping - Table mapping configuration
--------------------------------------------------------------------------------
Column          Type          Description
--------------------------------------------------------------------------------
project_code    VARCHAR(10)   Project code
schema_name     VARCHAR(20)   Doris target schema
table_name      VARCHAR(200)  Doris target table name
hudi_catalog    VARCHAR(20)   Hudi catalog name
hudi_schema     VARCHAR(20)   Hudi schema name
hudi_table      VARCHAR(200)  Hudi source table name
sync_mode       VARCHAR(20)   full / incremental
sync_order      INT           Sync order (reserved)
is_active       TINYINT       1=enabled, 0=disabled
remark          VARCHAR(500)  Remark
created_by      VARCHAR(100)  Created by
updated_by      VARCHAR(100)  Updated by
created_at      DATETIME      Created time
updated_at      DATETIME      Updated time


6.3 sync_hudi_checkpoint - Sync checkpoint
--------------------------------------------------------------------------------
Column              Type          Description
--------------------------------------------------------------------------------
project_code        VARCHAR(10)   Project code
schema_name         VARCHAR(20)   Doris schema
table_name          VARCHAR(200)  Doris table name
last_commit_time    VARCHAR(20)   Last Hudi commit time (YYYYMMDDHHMMSS)
last_sync_time      DATETIME      Last sync completion time
last_sync_rows      BIGINT        Last sync row count
created_by          VARCHAR(100)  User who executed sync
created_at          DATETIME      Created time


6.4 sync_job_log - Job log
--------------------------------------------------------------------------------
Column          Type          Description
--------------------------------------------------------------------------------
job_id          VARCHAR(30)   Job ID (timestamp + sequence)
project_code    VARCHAR(10)   Project code
sync_mode       VARCHAR(20)   full / incremental
trigger_type    VARCHAR(20)   auto / manual
start_time      DATETIME      Job start time
end_time        DATETIME      Job end time
total_tables    INT           Total tables processed
success_count   INT           Successful tables
fail_count      INT           Failed tables
skip_count      INT           Skipped tables
status          VARCHAR(20)   success / failed / partial
error_msg       VARCHAR(2000) Error message
created_by      VARCHAR(100)  User who executed sync
created_at      DATETIME      Created time


6.5 sync_detail_log - Detail log
--------------------------------------------------------------------------------
Column          Type          Description
--------------------------------------------------------------------------------
detail_id       VARCHAR(50)   Detail ID (job_id_seq)
job_id          VARCHAR(30)   Parent job ID
project_code    VARCHAR(10)   Project code
schema_name     VARCHAR(20)   Doris schema
table_name      VARCHAR(200)  Doris table name
hudi_catalog    VARCHAR(20)   Hudi catalog
hudi_schema     VARCHAR(20)   Hudi schema
hudi_table      VARCHAR(200)  Hudi source table
sync_mode       VARCHAR(20)   full / incremental
start_time      DATETIME      Start time
end_time        DATETIME      End time
duration_ms     INT           Duration in milliseconds
rows_synced     BIGINT        Rows synced
status          VARCHAR(20)   success / failed / skip
error_msg       VARCHAR(2000) Error message
source_filter   VARCHAR(500)  Source filter condition
created_by      VARCHAR(100)  User who executed sync
created_at      DATETIME      Created time


7. Database Queries
================================================================================

-- View recent jobs
SELECT * FROM sync_job_log ORDER BY created_at DESC LIMIT 10;

-- View job details
SELECT * FROM sync_detail_log WHERE job_id = '20260720143015123';

-- View failed tables
SELECT * FROM sync_detail_log WHERE status = 'failed' ORDER BY created_at DESC;

-- View checkpoints
SELECT * FROM sync_hudi_checkpoint WHERE project_code = 'demo_project';

-- View skipped tables
SELECT * FROM sync_detail_log WHERE status = 'skip' ORDER BY created_at DESC;


8. Logging
================================================================================

Log files are stored in: logs/sync/sync.log

# Real-time log tailing
tail -f logs/sync/sync.log

# Search for specific job
grep "20260720143015123" logs/sync/sync.log

# View errors only
grep "ERROR" logs/sync/sync.log

# View warnings only
grep "WARNING" logs/sync/sync.log


9. DolphinScheduler Configuration
================================================================================

# Incremental sync (daily schedule)
python3 /home/appadm/hudi-sync/bin/sync_project.py -p demo_project

# Force full sync
python3 /home/appadm/hudi-sync/bin/sync_project.py -p demo_project -m full


10. Troubleshooting
================================================================================

Issue: "Doris connection failed"
Solution: Check .env file for correct HOST, PORT, USER, PASSWORD

Issue: "Project not found"
Solution: Ensure project_code exists in project_base_info table

Issue: "No active table configurations found"
Solution: Insert mapping records into project_table_mapping with is_active=1

Issue: "Schema validation failed"
Solution: Ensure Hudi table and Doris table have same column count and order

Issue: "Data volume exceeds fail threshold"
Solution: Increase DATA_VOLUME_FAIL_THRESHOLD in .env or reduce the data


11. Version History
================================================================================

Version    Date        Description
--------------------------------------------------------------------------------
1.0.0      2026-07-20  Initial release
                       - Full and incremental sync support
                       - Schema validation
                       - Checkpoint management
                       - Job and detail logging
                       - Retry mechanism
                       - Data volume safety
                       - Stuck job detection


================================================================================
                              End of README
================================================================================
