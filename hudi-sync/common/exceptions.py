"""
Module: exceptions.py
Function: Define custom exceptions for the Hudi to Doris sync service.
Author: jason — requirements, testing, debugging (AI-assisted code generation)
Date: 2026-07-20
Modification History:
    - 2026-07-20: Initial version created

Why custom exceptions instead of standard Python exceptions?
----------------------------------------------------------------
1. 标准异常（如 ValueError, RuntimeError）太通用，无法区分是"配置错误"还是"数据库连接错误"
2. 自定义异常让代码在 except 时可以精确捕获特定类型的错误，做不同处理
3. 上层调用者（如 DolphinScheduler）可以根据异常类型返回不同的退出码
4. 日志中能快速定位问题类型，提高排查效率
"""


class SyncException(Exception):
    """
    Base exception for all sync service errors.
    所有同步异常的基类，方便统一捕获所有同步相关的异常。
    """
    pass


class ConnectionException(SyncException):
    """
    Raised when database connection fails.
    Example: Doris is unreachable, authentication failed, or network timeout.
    触发场景: Doris 连接失败、认证失败、网络超时。
    处理方式: 同步任务直接退出，返回退出码 3（异常）。
    """
    pass


class ConfigNotFoundException(SyncException):
    """
    Raised when required configuration is not found.
    Example: project_code not found in project_base_info, or no active table mappings.
    触发场景: 项目代码不存在、或项目下没有配置任何表映射。
    处理方式: 同步任务直接退出，返回退出码 3（异常）。
    """
    pass


class TableNotFoundException(SyncException):
    """
    Raised when source or target table does not exist in the database.
    触发场景: Hudi 源表不存在，或 Doris 目标表未创建。
    处理方式: 跳过该表，继续同步其他表，记录日志状态为 skip。
    """
    pass


class SchemaNotMatchException(SyncException):
    """
    Raised when source and target tables have different column structures.
    This includes mismatched column count or column order.
    触发场景: 源表和目标表的字段数量不等，或字段顺序不一致。
    处理方式: 跳过该表，继续同步其他表，记录日志状态为 skip。
    说明: Doris 表对字段顺序敏感，顺序不一致会导致数据错位。
    """
    pass


class SyncFailedException(SyncException):
    """
    Raised when the actual data sync operation fails.
    Example: INSERT INTO SELECT fails due to data type mismatch.
    触发场景: 执行 INSERT INTO SELECT 时失败，如数据类型不兼容。
    处理方式: 重试配置次数，全部失败后记录日志状态为 failed。
    """
    pass


class CheckpointException(SyncException):
    """
    Raised when checkpoint read or write operations fail.
    Example: Unable to read last_commit_time, or unable to update checkpoint table.
    触发场景: 读取或写入 sync_hudi_checkpoint 表时失败。
    处理方式: 增量同步无法继续，降级为全量同步或跳过该表。
    """
    pass


class DataVolumeExceedException(SyncException):
    """
    Raised when estimated data volume exceeds the configured fail threshold.
    This prevents accidental massive data loads that could impact system performance.
    触发场景: 预估同步行数超过 DATA_VOLUME_FAIL_THRESHOLD 配置值。
    处理方式: 跳过该表，需人工介入确认后处理。
    目的: 防止误操作导致大批量数据写入，影响 Doris 集群性能。
    """
    pass


class PreCheckFailedException(SyncException):
    """
    Raised when environment pre-check fails.
    Example: Doris connection test failed, or duplicate running job detected.
    触发场景: 环境检测不通过，如 Doris 连不上、或有相同项目正在运行。
    处理方式: 同步任务直接退出，返回退出码 3（异常）。
    """
    pass