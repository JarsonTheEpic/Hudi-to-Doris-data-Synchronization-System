-- ============================================================================
-- Script: Hudi to Doris Data Sync System - Database Initialization Script
-- Author: jason
-- Date: 2026-07-20
-- Description:
--   1. This script creates 5 tables in Doris for project info, table mapping,
--      sync checkpoint, job log and detail log.
--   2. All tables use UNIQUE KEY model to support data updates.
--   3. Execute this script under the 'com' database.
-- Modification History:
--   - 2026-07-20: Initial version created
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. project_base_info
--    Stores basic project information including name, owner, priority, status.
--    Primary Key: project_code
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS project_base_info (
    project_code      VARCHAR(10)   NOT NULL   COMMENT 'Project code (primary key)',
    project_name      VARCHAR(200)  NOT NULL   COMMENT 'Project name',
    app_category      VARCHAR(50)   NULL       COMMENT 'Application category',
    dept_sys_code     VARCHAR(10)   NULL       COMMENT 'Department/System code',
    dept_sys_name     VARCHAR(200)  NULL       COMMENT 'Department/System name',
    importance_level  VARCHAR(10)   NULL       DEFAULT 'medium' COMMENT 'Importance level: high/medium/low',
    consumer          VARCHAR(100)  NULL       COMMENT 'Consumer',
    involve_fill_calc VARCHAR(10)   NULL       COMMENT 'Involve fill calculation: 1=yes, 0=no',
    key_user          VARCHAR(100)  NULL       COMMENT 'Key user',
    project_status    VARCHAR(20)   NULL       COMMENT 'Project status: designing/developing/running/stopped',
    primary_owner     VARCHAR(100)  NULL       COMMENT 'Primary owner',
    secondary_owner   VARCHAR(100)  NULL       COMMENT 'Secondary owner',
    remark            VARCHAR(2000) NULL       COMMENT 'Remark',
    created_by        VARCHAR(10)   NULL       COMMENT 'Created by',
    updated_by        VARCHAR(10)   NULL       COMMENT 'Updated by',
    created_at        DATETIME      NULL       DEFAULT CURRENT_TIMESTAMP COMMENT 'Created time',
    updated_at        DATETIME      NULL       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Updated time'
)
ENGINE=OLAP
UNIQUE KEY(project_code)
COMMENT 'Project base information table'
DISTRIBUTED BY HASH(project_code) BUCKETS 1
PROPERTIES (
    'replication_allocation' = 'tag.location.default: 1'
);


-- ----------------------------------------------------------------------------
-- 2. project_table_mapping
--    Stores the mapping between Doris target tables and Hudi source tables.
--    Primary Key: (project_code, schema_name, table_name)
--    Doris target table: schema_name.table_name (internal catalog)
--    Hudi source table:  hudi_catalog.hudi_schema.hudi_table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS project_table_mapping (
    project_code  VARCHAR(10)   NOT NULL   COMMENT 'Project code',
    schema_name   VARCHAR(20)   NOT NULL   COMMENT 'Doris target table schema',
    table_name    VARCHAR(200)  NOT NULL   COMMENT 'Doris target table name',
    hudi_catalog  VARCHAR(20)   NOT NULL   COMMENT 'Hudi catalog name',
    hudi_schema   VARCHAR(20)   NOT NULL   COMMENT 'Hudi schema name',
    hudi_table    VARCHAR(200)  NOT NULL   COMMENT 'Hudi source table name',
    sync_mode     VARCHAR(20)   NULL       DEFAULT 'incremental' COMMENT 'Sync mode: full/incremental',
    sync_order    INT           NULL       DEFAULT 0 COMMENT 'Sync order (reserved for parent-child ordering)',
    is_active     TINYINT       NULL       DEFAULT 1 COMMENT 'Is active: 1=enabled, 0=disabled',
    remark        VARCHAR(500)  NULL       COMMENT 'Remark',
    created_by    VARCHAR(100)  NULL       COMMENT 'Created by',
    updated_by    VARCHAR(100)  NULL       COMMENT 'Updated by',
    created_at    DATETIME      NULL       DEFAULT CURRENT_TIMESTAMP COMMENT 'Created time',
    updated_at    DATETIME      NULL       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Updated time'
)
ENGINE=OLAP
UNIQUE KEY(project_code, schema_name, table_name)
COMMENT 'Project-table mapping configuration table'
DISTRIBUTED BY HASH(project_code) BUCKETS 1
PROPERTIES (
    'replication_allocation' = 'tag.location.default: 1'
);


-- ----------------------------------------------------------------------------
-- 3. sync_hudi_checkpoint
--    Records the last Hudi commit_time for each table, used for incremental sync.
--    Primary Key: (project_code, schema_name, table_name)
--    last_commit_time: Format YYYYMMDDHHMMSS
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sync_hudi_checkpoint (
    project_code     VARCHAR(10)   NOT NULL   COMMENT 'Project code',
    schema_name      VARCHAR(20)   NOT NULL   COMMENT 'Doris target table schema',
    table_name       VARCHAR(200)  NOT NULL   COMMENT 'Doris target table name',
    last_commit_time VARCHAR(20)   NULL       COMMENT 'Last Hudi commit time (for incremental sync)',
    last_sync_time   DATETIME      NULL       COMMENT 'Last sync completion time (for logging)',
    last_sync_rows   BIGINT        NULL       DEFAULT 0 COMMENT 'Last sync row count',
    created_by       VARCHAR(100)  NULL       COMMENT 'User who executed sync',
    created_at       DATETIME      NULL       DEFAULT CURRENT_TIMESTAMP COMMENT 'Created time'
)
ENGINE=OLAP
UNIQUE KEY(project_code, schema_name, table_name)
COMMENT 'Sync checkpoint table'
DISTRIBUTED BY HASH(project_code) BUCKETS 1
PROPERTIES (
    'replication_allocation' = 'tag.location.default: 1'
);


-- ----------------------------------------------------------------------------
-- 4. sync_job_log
--    Stores the overall execution summary for each sync job (project-level).
--    Primary Key: job_id (format: timestamp + sequence)
--    One job corresponds to one project sync task, aggregating all tables.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sync_job_log (
    job_id          VARCHAR(30)   NOT NULL   COMMENT 'Job unique ID (timestamp + sequence)',
    project_code    VARCHAR(10)   NOT NULL   COMMENT 'Project code',
    sync_mode       VARCHAR(20)   NULL       DEFAULT 'incremental' COMMENT 'Sync mode: full/incremental',
    trigger_type    VARCHAR(20)   NULL       DEFAULT 'auto' COMMENT 'Trigger type: auto/manual',
    start_time      DATETIME      NOT NULL   COMMENT 'Start time',
    end_time        DATETIME      NULL       COMMENT 'End time',
    total_tables    INT           NULL       DEFAULT 0 COMMENT 'Total number of tables',
    success_count   INT           NULL       DEFAULT 0 COMMENT 'Number of successful tables',
    fail_count      INT           NULL       DEFAULT 0 COMMENT 'Number of failed tables',
    skip_count      INT           NULL       DEFAULT 0 COMMENT 'Number of skipped tables',
    status          VARCHAR(20)   NULL       DEFAULT 'success' COMMENT 'Status: success/failed/partial',
    error_msg       VARCHAR(2000) NULL       COMMENT 'Error message',
    created_by      VARCHAR(100)  NULL       COMMENT 'User who executed sync',
    created_at      DATETIME      NULL       DEFAULT CURRENT_TIMESTAMP COMMENT 'Created time'
)
ENGINE=OLAP
UNIQUE KEY(job_id)
COMMENT 'Sync job log table'
DISTRIBUTED BY HASH(job_id) BUCKETS 1
PROPERTIES (
    'replication_allocation' = 'tag.location.default: 1'
);


-- ----------------------------------------------------------------------------
-- 5. sync_detail_log
--    Stores detailed execution information for each table in a sync job.
--    Primary Key: detail_id (format: job_id_seq)
--    One job contains multiple details, one detail per table.
--    source_filter: Records the filter condition for incremental sync debugging.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sync_detail_log (
    detail_id      VARCHAR(50)   NOT NULL   COMMENT 'Detail unique ID (job_id_seq)',
    job_id         VARCHAR(30)   NOT NULL   COMMENT 'Associated job ID',
    project_code   VARCHAR(10)   NOT NULL   COMMENT 'Project code',
    schema_name    VARCHAR(20)   NOT NULL   COMMENT 'Doris target table schema',
    table_name     VARCHAR(200)  NOT NULL   COMMENT 'Doris target table name',
    hudi_catalog   VARCHAR(20)   NOT NULL   COMMENT 'Hudi catalog name',
    hudi_schema    VARCHAR(20)   NOT NULL   COMMENT 'Hudi schema name',
    hudi_table     VARCHAR(200)  NOT NULL   COMMENT 'Hudi source table name',
    sync_mode      VARCHAR(20)   NULL       COMMENT 'Actual sync mode: full/incremental',
    start_time     DATETIME      NOT NULL   COMMENT 'Start time',
    end_time       DATETIME      NULL       COMMENT 'End time',
    duration_ms    INT           NULL       COMMENT 'Duration in milliseconds',
    rows_synced    BIGINT        NULL       DEFAULT 0 COMMENT 'Rows synced',
    status         VARCHAR(20)   NULL       COMMENT 'Status: success/failed/skip',
    error_msg      VARCHAR(2000) NULL       COMMENT 'Error message',
    source_filter  VARCHAR(500)  NULL       COMMENT 'Source filter condition (commit condition for incremental)',
    created_by     VARCHAR(100)  NULL       COMMENT 'User who executed sync',
    created_at     DATETIME      NULL       DEFAULT CURRENT_TIMESTAMP COMMENT 'Created time'
)
ENGINE=OLAP
UNIQUE KEY(detail_id)
COMMENT 'Sync detail log table'
DISTRIBUTED BY HASH(detail_id) BUCKETS 1
PROPERTIES (
    'replication_allocation' = 'tag.location.default: 1'
);
