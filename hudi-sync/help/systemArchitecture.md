================================================================================
                    Hudi to Doris Data Sync Service - Complete Flowchart
================================================================================


【1. Program Entry】bin/sync_project.py
│
├── 1.1 Parse Command Line Arguments
│   ├── --project_code  Project code (required)
│   └── --mode          Sync mode override (optional: full / incremental)
│
├── 1.2 Generate job_id
│   └── Format: YYYYMMDDHHmmSS + 3-digit millisecond (e.g., 20260721011723179)
│
├── 1.3 Environment Pre-check (PreChecker)
│   ├── 1.3.1 Check Doris Connection
│   │   └── Execute SELECT 1 → fail → exit(3)
│   ├── 1.3.2 Check Project Existence
│   │   └── Query project_base_info → not found → exit(3)
│   ├── 1.3.3 Check Table Configurations
│   │   └── Query project_table_mapping (is_active=1) → no records → exit(3)
│   └── 1.3.4 Check Stuck Jobs
│       └── Query sync_job_log (status='running') → >60 minutes → cleanup, otherwise exit(3)
│
├── 1.4 Log Job Start (LoggerManager)
│   └── INSERT INTO sync_job_log (job_id, project_code, sync_type, start_time, status='running')
│
└── 1.5 Invoke ProjectWorker.run()
    │
    └── 【2. Project Sync Orchestration】core/worker/project_worker.py
        │
        ├── 2.1 Read Table Configurations (TableConfigReader)
        │   └── SELECT * FROM project_table_mapping WHERE project_code = ? AND is_active = 1
        │
        ├── 2.2 Iterate Tables (process each table in a loop)
        │   │
        │   └── 【3. Single Table Sync Flow】_sync_single_table()
        │       │
        │       ├── 3.1 Generate detail_id
        │       │   └── Format: job_id_seq (e.g., 20260721011723179_1)
        │       │
        │       ├── 3.2 Log Detail Start (LoggerManager)
        │       │   └── INSERT INTO sync_detail_log (detail_id, job_id, status='running')
        │       │
        │       ├── 3.3 Schema Validation (SchemaValidator)
        │       │   ├── DESC hudi_catalog.hudi_schema.hudi_table  → get source columns
        │       │   ├── DESC internal.schema_name.table_name      → get target columns
        │       │   ├── Filter out _hoodie_* system fields
        │       │   ├── Compare column count → mismatch → skip table (status='skip')
        │       │   └── Compare column order → mismatch → skip table (status='skip')
        │       │
        │       ├── 3.4 Primary Key Validation (PrimaryKeyValidator) 【warning only, non-blocking】
        │       │   ├── Parse Hudi _hoodie_record_key to extract primary key fields
        │       │   ├── Parse Doris UNIQUE KEY from SHOW CREATE TABLE
        │       │   └── Compare sets → mismatch → log warning, continue sync
        │       │
        │       ├── 3.5 Read Checkpoint (CheckpointManager) 【incremental mode only】
        │       │   └── SELECT last_commit_time FROM sync_hudi_checkpoint
        │       │
        │       ├── 3.6 Estimate Data Volume (CheckpointManager)
        │       │   ├── Full: SELECT COUNT(*) FROM hudi_catalog.hudi_schema.hudi_table
        │       │   ├── Incremental: SELECT COUNT(*) FROM ... WHERE _hoodie_commit_time > last_commit_time
        │       │   └── Exceeds threshold → skip table (status='skip')
        │       │
        │       ├── 3.7 Determine Sync Mode
        │       │   ├── --mode override present → force use that mode
        │       │   ├── sync_mode='incremental' with no checkpoint → fallback to full
        │       │   └── Otherwise use sync_mode from configuration table
        │       │
        │       ├── 3.8 Execute Sync (with retry mechanism)
        │       │   │
        │       │   ├── 【4.1 Full Sync】FullSync.sync()
        │       │   │   ├── DELETE FROM internal.schema_name.table_name
        │       │   │   └── INSERT INTO internal.schema_name.table_name (business_columns)
        │       │   │       SELECT business_columns FROM hudi_catalog.hudi_schema.hudi_table
        │       │   │
        │       │   └── 【4.2 Incremental Sync】IncrementalSync.sync()
        │       │       ├── Check if _hoodie_commit_time column exists → missing → raise error
        │       │       └── INSERT INTO internal.schema_name.table_name (business_columns)
        │       │           SELECT business_columns FROM hudi_catalog.hudi_schema.hudi_table
        │       │           WHERE _hoodie_commit_time > last_commit_time
        │       │
        │       ├── 3.9 Update Checkpoint (CheckpointManager) 【incremental mode only】
        │       │   ├── SELECT MAX(_hoodie_commit_time) FROM hudi_catalog.hudi_schema.hudi_table
        │       │   ├── DELETE FROM sync_hudi_checkpoint WHERE ...
        │       │   └── INSERT INTO sync_hudi_checkpoint (last_commit_time, last_sync_rows)
        │       │
        │       └── 3.10 Log Detail End (LoggerManager)
        │           └── UPDATE sync_detail_log SET end_time, duration_ms, rows_synced, status
        │
        └── 2.3 Return Statistics
            └── (success_count, fail_count, skip_count)


【5. Finalization Phase】bin/sync_project.py (back to main entry)
│
├── 5.1 Determine Final Status
│   ├── fail_count == total_tables → status = 'failed'
│   ├── fail_count > 0 or skip_count > 0 → status = 'partial'
│   └── All succeeded → status = 'success'
│
├── 5.2 Log Job End (LoggerManager)
│   └── UPDATE sync_job_log SET end_time, total_tables, success_count, fail_count, skip_count, status
│
└── 5.3 Return Exit Code
    ├── 0  → All tables synced successfully
    ├── 1  → All tables failed
    ├── 2  → Partial success
    └── 3  → Abnormal termination


================================================================================
                              Exception Handling & Retry
================================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│                           Retry Mechanism                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  Sync fails → wait (5s → 10s → 20s) → retry → max 3 attempts              │
│  All attempts fail → log status='failed', continue to next table           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           Stuck Job Cleanup                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│  Detect status='running' with duration > 60 minutes                        │
│  → Mark as status='failed', error_msg='Job was stuck and cleaned up'       │
│  → Continue with new job execution                                         │
└─────────────────────────────────────────────────────────────────────────────┘


================================================================================
                              Data Flow Diagram
================================================================================

                    ┌─────────────────────────────────────────────────┐
                    │                                                 │
                    ▼                                                 │
    ┌───────────────────────────┐                                    │
    │   Hudi Source Table       │                                    │
    │   (hudi_catalog.          │                                    │
    │    hudi_schema.           │                                    │
    │    hudi_table)            │                                    │
    └───────────┬───────────────┘                                    │
                │                                                    │
                │  SELECT (excluding _hoodie_* system fields)       │
                │                                                    │
                ▼                                                    │
    ┌───────────────────────────┐                                    │
    │   Doris Target Table      │                                    │
    │   (internal.schema_name.  │                                    │
    │    table_name)            │                                    │
    └───────────────────────────┘                                    │


================================================================================
                              Table Dependency Diagram
================================================================================

    project_base_info
    └── project_code ──────┐
                           │
                           ▼
    project_table_mapping ─┘
    └── project_code ──────────────────────────────────────┐
    └── schema_name ──────────────────────────────────────┐│
    └── table_name ──────────────────────────────────────┐││
                                                         │││
                                                         ▼▼▼
    sync_hudi_checkpoint (Checkpoint)                sync_detail_log (Detail Log)
    └── project_code ──────────────────────────────────┐
    └── schema_name ──────────────────────────────────┐│
    └── table_name ──────────────────────────────────┐││
                                                     │││
                                                     ▼▼▼
                                              sync_job_log (Job Log)
                                              └── job_id
                                              └── project_code


================================================================================
                              Sync Mode Decision Tree
================================================================================

                              ┌─────────────────────┐
                              │  Start Table Sync   │
                              └──────────┬──────────┘
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │  --mode override?   │
                              └──────────┬──────────┘
                                     │            │
                                    Yes           No
                                     │            │
                                     ▼            ▼
                              ┌─────────────┐  ┌─────────────────────┐
                              │ Use forced  │  │ Read sync_mode from │
                              │    mode     │  │ project_table_mapping│
                              └─────────────┘  └──────────┬──────────┘
                                                          │
                                                          ▼
                                         ┌─────────────────────────────┐
                                         │ sync_mode = 'incremental'?  │
                                         └──────────────┬──────────────┘
                                                      │            │
                                                     Yes           No
                                                      │            │
                                                      ▼            ▼
                                         ┌─────────────────────┐  ┌─────────────┐
                                         │  Checkpoint exists? │  │  FULL SYNC  │
                                         └──────────┬──────────┘  └─────────────┘
                                              │            │
                                             Yes           No
                                              │            │
                                              ▼            ▼
                                         ┌─────────────┐  ┌─────────────────────┐
                                         │ INCREMENTAL │  │ Fallback to FULL    │
                                         │    SYNC     │  │ (first time sync)   │
                                         └─────────────┘  └─────────────────────┘


================================================================================
                              Core Module Sequence
================================================================================

    sync_project.py          PreChecker          ProjectWorker          SyncEngine
         │                       │                    │                    │
         │  ──── pre-check ──────►                    │                    │
         │                       │                    │                    │
         │  ◄─── pass/fail ──────│                    │                    │
         │                       │                    │                    │
         │  ── log job start ────────────────────────►│                    │
         │                       │                    │                    │
         │  ── ProjectWorker.run() ──────────────────►│                    │
         │                       │                    │                    │
         │                       │                    │  ── read config ───►│
         │                       │                    │                    │
         │                       │                    │  ◄── config rows ───│
         │                       │                    │                    │
         │                       │                    │  ── for each table ─│
         │                       │                    │                    │
         │                       │                    │  ── validate ──────►│
         │                       │                    │                    │
         │                       │                    │  ◄── pass/fail ─────│
         │                       │                    │                    │
         │                       │                    │  ── read checkpoint─►│
         │                       │                    │                    │
         │                       │                    │  ◄── checkpoint ────│
         │                       │                    │                    │
         │                       │                    │  ── sync ──────────►│
         │                       │                    │                    │
         │                       │                    │  ◄── rows synced ───│
         │                       │                    │                    │
         │                       │                    │  ── update ckpt ───►│
         │                       │                    │                    │
         │  ◄── statistics ──────────────────────────│                    │
         │                       │                    │                    │
         │  ── log job end ──────────────────────────►│                    │
         │                       │                    │                    │
         │  ── exit code ───────►                    │                    │
         
================================================================================
                    Hudi to Doris 数据同步系统 - 完整流程图
================================================================================


【1. 程序入口】bin/sync_project.py
│
├── 1.1 解析命令行参数
│   ├── --project_code  项目代码（必填）
│   └── --mode          同步模式（可选：full / incremental）
│
├── 1.2 生成 job_id
│   └── 格式：YYYYMMDDHHmmSS + 3位毫秒（如：20260721011723179）
│
├── 1.3 环境预检 (PreChecker)
│   ├── 1.3.1 检查 Doris 连接
│   │   └── 执行 SELECT 1 → 失败则退出(3)
│   ├── 1.3.2 检查项目是否存在
│   │   └── 查询 project_base_info → 不存在则退出(3)
│   ├── 1.3.3 检查表配置
│   │   └── 查询 project_table_mapping (is_active=1) → 无记录则退出(3)
│   └── 1.3.4 检查卡住的作业
│       └── 查询 sync_job_log (status='running') → 超过60分钟则清理，否则退出(3)
│
├── 1.4 记录作业开始 (LoggerManager)
│   └── INSERT INTO sync_job_log (job_id, project_code, sync_type, start_time, status='running')
│
└── 1.5 调用 ProjectWorker.run()
    │
    └── 【2. 项目同步编排】core/worker/project_worker.py
        │
        ├── 2.1 读取表配置 (TableConfigReader)
        │   └── SELECT * FROM project_table_mapping WHERE project_code = ? AND is_active = 1
        │
        ├── 2.2 逐表同步（循环处理每张表）
        │   │
        │   └── 【3. 单表同步流程】_sync_single_table()
        │       │
        │       ├── 3.1 生成 detail_id
        │       │   └── 格式：job_id_序号（如：20260721011723179_1）
        │       │
        │       ├── 3.2 记录明细开始 (LoggerManager)
        │       │   └── INSERT INTO sync_detail_log (detail_id, job_id, status='running')
        │       │
        │       ├── 3.3 表结构校验 (SchemaValidator)
        │       │   ├── DESC hudi_catalog.hudi_schema.hudi_table  → 获取源表字段
        │       │   ├── DESC internal.schema_name.table_name      → 获取目标表字段
        │       │   ├── 过滤 _hoodie_* 系统字段
        │       │   ├── 比较字段数量 → 不等则跳过该表 (status='skip')
        │       │   └── 比较字段顺序 → 不等则跳过该表 (status='skip')
        │       │
        │       ├── 3.4 主键校验 (PrimaryKeyValidator) 【警告不阻断】
        │       │   ├── 读取 Hudi _hoodie_record_key 解析主键字段
        │       │   ├── 解析 Doris UNIQUE KEY 字段
        │       │   └── 比较是否一致 → 不一致则打印警告，继续同步
        │       │
        │       ├── 3.5 读取水位线 (CheckpointManager) 【仅增量模式】
        │       │   └── SELECT last_commit_time FROM sync_hudi_checkpoint
        │       │
        │       ├── 3.6 预估数据量 (CheckpointManager)
        │       │   ├── 全量：SELECT COUNT(*) FROM hudi_catalog.hudi_schema.hudi_table
        │       │   ├── 增量：SELECT COUNT(*) FROM ... WHERE _hoodie_commit_time > last_commit_time
        │       │   └── 超过阈值则跳过该表 (status='skip')
        │       │
        │       ├── 3.7 确定同步模式
        │       │   ├── 命令行有 --mode 参数 → 强制使用该模式
        │       │   ├── 配置表 sync_mode='incremental' 且无水印 → 降级为全量
        │       │   └── 否则使用配置表 sync_mode
        │       │
        │       ├── 3.8 执行同步 (带重试机制)
        │       │   │
        │       │   ├── 【4.1 全量同步】FullSync.sync()
        │       │   │   ├── DELETE FROM internal.schema_name.table_name
        │       │   │   └── INSERT INTO internal.schema_name.table_name (业务字段)
        │       │   │       SELECT 业务字段 FROM hudi_catalog.hudi_schema.hudi_table
        │       │   │
        │       │   └── 【4.2 增量同步】IncrementalSync.sync()
        │       │       ├── 检查 _hoodie_commit_time 字段是否存在 → 不存在则报错
        │       │       └── INSERT INTO internal.schema_name.table_name (业务字段)
        │       │           SELECT 业务字段 FROM hudi_catalog.hudi_schema.hudi_table
        │       │           WHERE _hoodie_commit_time > last_commit_time
        │       │
        │       ├── 3.9 更新水位线 (CheckpointManager) 【仅增量模式】
        │       │   ├── SELECT MAX(_hoodie_commit_time) FROM hudi_catalog.hudi_schema.hudi_table
        │       │   ├── DELETE FROM sync_hudi_checkpoint WHERE ...
        │       │   └── INSERT INTO sync_hudi_checkpoint (last_commit_time, last_sync_rows)
        │       │
        │       └── 3.10 记录明细结束 (LoggerManager)
        │           └── UPDATE sync_detail_log SET end_time, duration_ms, rows_synced, status
        │
        └── 2.3 返回统计结果
            └── (success_count, fail_count, skip_count)


【5. 收尾阶段】bin/sync_project.py（回到主入口）
│
├── 5.1 判断最终状态
│   ├── fail_count == total_tables → status = 'failed'
│   ├── fail_count > 0 or skip_count > 0 → status = 'partial'
│   └── 全部成功 → status = 'success'
│
├── 5.2 记录作业结束 (LoggerManager)
│   └── UPDATE sync_job_log SET end_time, total_tables, success_count, fail_count, skip_count, status
│
└── 5.3 返回退出码
    ├── 0  → 全部成功
    ├── 1  → 全部失败
    ├── 2  → 部分成功
    └── 3  → 执行异常


================================================================================
                              异常处理与重试
================================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│                           重试机制 (Retry)                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  同步失败 → 等待 (5s → 10s → 20s) → 重试 → 最多 3 次                       │
│  全部失败 → 记录 status='failed'，继续下一张表                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           卡住作业清理                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│  检测到 status='running' 且运行时间 > 60分钟                               │
│  → 标记为 status='failed'，error_msg='作业卡住已清理'                      │
│  → 继续执行新作业                                                          │
└─────────────────────────────────────────────────────────────────────────────┘


================================================================================
                              数据流向图
================================================================================

                    ┌─────────────────────────────────────────────────┐
                    │                                                 │
                    ▼                                                 │
    ┌───────────────────────────┐                                    │
    │   Hudi 源表               │                                    │
    │   (hudi_catalog.          │                                    │
    │    hudi_schema.           │                                    │
    │    hudi_table)            │                                    │
    └───────────┬───────────────┘                                    │
                │                                                    │
                │  SELECT (排除 _hoodie_* 系统字段)                  │
                │                                                    │
                ▼                                                    │
    ┌───────────────────────────┐                                    │
    │   Doris 目标表            │                                    │
    │   (internal.schema_name.  │                                    │
    │    table_name)            │                                    │
    └───────────────────────────┘                                    │


================================================================================
                              表依赖关系图
================================================================================

    project_base_info
    └── project_code ──────┐
                           │
                           ▼
    project_table_mapping ─┘
    └── project_code ──────────────────────────────────────┐
    └── schema_name ──────────────────────────────────────┐│
    └── table_name ──────────────────────────────────────┐││
                                                         │││
                                                         ▼▼▼
    sync_hudi_checkpoint (水位线)                    sync_detail_log (明细日志)
    └── project_code ──────────────────────────────────┐
    └── schema_name ──────────────────────────────────┐│
    └── table_name ──────────────────────────────────┐││
                                                     │││
                                                     ▼▼▼
                                              sync_job_log (作业日志)
                                              └── job_id
                                              └── project_code

