namespace DbArchiveTool.Web.Models;

/// <summary>
/// 定时归档任务DTO（用于列表和详情展示，仅支持普通表+BulkCopy）
/// </summary>
public record ScheduledArchiveJobDto
{
    /// <summary>任务ID</summary>
    public Guid Id { get; init; }

    /// <summary>任务名称，长度2-100字符</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>任务描述</summary>
    public string? Description { get; init; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; init; }

    /// <summary>数据源名称（用于列表显示，避免二次查询）</summary>
    public string DataSourceName { get; init; } = string.Empty;

    /// <summary>源表架构名</summary>
    public string SourceSchemaName { get; init; } = string.Empty;

    /// <summary>源表名</summary>
    public string SourceTableName { get; init; } = string.Empty;

    /// <summary>完整源表名（格式化为 "dbo.Orders"，便于显示）</summary>
    public string FullSourceTable { get; init; } = string.Empty;

    /// <summary>目标表架构名</summary>
    public string TargetSchemaName { get; init; } = string.Empty;

    /// <summary>目标表名</summary>
    public string TargetTableName { get; init; } = string.Empty;

    /// <summary>完整目标表名（格式化为 "dbo.Orders_Archive"）</summary>
    public string FullTargetTable { get; init; } = string.Empty;

    /// <summary>归档筛选列名，推荐时间字段</summary>
    public string ArchiveFilterColumn { get; init; } = string.Empty;

    /// <summary>归档筛选条件（WHERE 子句），必填</summary>
    public string ArchiveFilterCondition { get; init; } = string.Empty;

    /// <summary>批次大小，推荐 1000-10000</summary>
    public int BatchSize { get; init; }

    /// <summary>单次执行最大行数，推荐 50000</summary>
    public int MaxRowsPerExecution { get; init; }

    /// <summary>间隔分钟（简单模式），与 CronExpression 二选一</summary>
    public int? IntervalMinutes { get; init; }

    /// <summary>Cron表达式（高级模式），与 IntervalMinutes 二选一</summary>
    public string? CronExpression { get; init; }

    /// <summary>Cron 中文描述: "每5分钟执行一次" / "每天凌晨2点执行"</summary>
    public string? CronDescription { get; init; }

    /// <summary>是否已启用</summary>
    public bool IsEnabled { get; init; }

    /// <summary>下次执行时间（UTC）</summary>
    public DateTime? NextExecutionAtUtc { get; init; }

    /// <summary>下次执行时间本地化显示: "2小时后" / "明天 02:00"</summary>
    public string? NextExecutionDisplay { get; init; }

    /// <summary>最后执行时间（UTC）</summary>
    public DateTime? LastExecutionAtUtc { get; init; }

    /// <summary>最后执行状态</summary>
    public JobExecutionStatus LastExecutionStatus { get; init; }

    /// <summary>最后执行状态显示文本: "运行中" / "成功" / "失败"</summary>
    public string LastExecutionStatusDisplay { get; init; } = string.Empty;

    /// <summary>最后一次归档行数</summary>
    public long? LastArchivedRowCount { get; init; }

    /// <summary>总执行次数</summary>
    public long TotalExecutionCount { get; init; }

    /// <summary>总归档行数</summary>
    public long TotalArchivedRowCount { get; init; }

    /// <summary>连续失败次数</summary>
    public int ConsecutiveFailureCount { get; init; }

    /// <summary>最大连续失败次数（达到后自动禁用）</summary>
    public int MaxConsecutiveFailures { get; init; }

    /// <summary>创建时间（UTC）</summary>
    public DateTime CreatedAtUtc { get; init; }

    /// <summary>更新时间（UTC）</summary>
    public DateTime UpdatedAtUtc { get; init; }
}

/// <summary>
/// 任务执行状态枚举（与后端 Domain.Entities.JobExecutionStatus 保持一致）
/// </summary>
[System.Text.Json.Serialization.JsonConverter(typeof(JobExecutionStatusConverter))]
public enum JobExecutionStatus
{
    /// <summary>未开始</summary>
    NotStarted = 0,

    /// <summary>运行中</summary>
    Running = 1,

    /// <summary>成功</summary>
    Success = 2,

    /// <summary>失败</summary>
    Failed = 3,

    /// <summary>跳过（本周期无数据）</summary>
    Skipped = 4
}

/// <summary>
/// 创建定时归档任务请求（仅支持普通表 + BulkCopy）
/// </summary>
public record CreateScheduledArchiveJobRequest
{
    /// <summary>任务名称，长度2-100字符，不允许仅数字</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>任务描述</summary>
    public string? Description { get; init; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; init; }

    /// <summary>源表架构名</summary>
    public string SourceSchemaName { get; init; } = string.Empty;

    /// <summary>源表名（必须是普通表）</summary>
    public string SourceTableName { get; init; } = string.Empty;

    /// <summary>目标表架构名</summary>
    public string TargetSchemaName { get; init; } = string.Empty;

    /// <summary>目标表名</summary>
    public string TargetTableName { get; init; } = string.Empty;

    /// <summary>归档筛选列名，推荐时间字段</summary>
    public string ArchiveFilterColumn { get; init; } = string.Empty;

    /// <summary>归档筛选条件（WHERE 子句），必填</summary>
    public string ArchiveFilterCondition { get; init; } = string.Empty;

    /// <summary>批次大小，范围 1-100000，推荐 1000-10000</summary>
    public int BatchSize { get; init; } = 5000;

    /// <summary>单次执行最大行数，推荐 50000</summary>
    public int MaxRowsPerExecution { get; init; } = 50000;

    /// <summary>间隔分钟（简单模式），与 CronExpression 二选一</summary>
    public int? IntervalMinutes { get; init; }

    /// <summary>Cron表达式（高级模式），与 IntervalMinutes 二选一</summary>
    public string? CronExpression { get; init; }

    /// <summary>是否立即启用</summary>
    public bool IsEnabled { get; init; } = true;

    /// <summary>最大连续失败次数，达到后自动禁用任务，默认 5</summary>
    public int MaxConsecutiveFailures { get; init; } = 5;
}

/// <summary>
/// 更新定时归档任务请求
/// </summary>
public record UpdateScheduledArchiveJobRequest
{
    /// <summary>任务名称</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>任务描述</summary>
    public string? Description { get; init; }

    /// <summary>源表架构名</summary>
    public string SourceSchemaName { get; init; } = string.Empty;

    /// <summary>源表名</summary>
    public string SourceTableName { get; init; } = string.Empty;

    /// <summary>目标表架构名</summary>
    public string TargetSchemaName { get; init; } = string.Empty;

    /// <summary>目标表名</summary>
    public string TargetTableName { get; init; } = string.Empty;

    /// <summary>归档筛选列名</summary>
    public string ArchiveFilterColumn { get; init; } = string.Empty;

    /// <summary>归档筛选条件（WHERE 子句）</summary>
    public string ArchiveFilterCondition { get; init; } = string.Empty;

    /// <summary>批次大小</summary>
    public int BatchSize { get; init; }

    /// <summary>单次执行最大行数</summary>
    public int MaxRowsPerExecution { get; init; }

    /// <summary>间隔分钟</summary>
    public int? IntervalMinutes { get; init; }

    /// <summary>Cron表达式</summary>
    public string? CronExpression { get; init; }

    /// <summary>最大连续失败次数</summary>
    public int MaxConsecutiveFailures { get; init; }
}

/// <summary>
/// 定时归档任务统计信息
/// </summary>
public record ScheduledArchiveJobStatisticsDto
{
    /// <summary>任务总数</summary>
    public int TotalJobCount { get; init; }

    /// <summary>启用任务数</summary>
    public int EnabledJobCount { get; init; }

    /// <summary>运行中任务数</summary>
    public int RunningJobCount { get; init; }

    /// <summary>今日执行次数</summary>
    public int TodayExecutionCount { get; init; }

    /// <summary>今日成功次数</summary>
    public int TodaySuccessCount { get; init; }

    /// <summary>今日失败次数</summary>
    public int TodayFailureCount { get; init; }

    /// <summary>今日成功率（百分比，0-100）</summary>
    public double TodaySuccessRate { get; init; }

    /// <summary>总归档行数</summary>
    public long TotalArchivedRowCount { get; init; }

    /// <summary>平均每次归档行数</summary>
    public long AverageArchivedRowCount { get; init; }
}

/// <summary>
/// 任务执行历史记录
/// </summary>
public record JobExecutionHistoryDto
{
    /// <summary>执行记录ID</summary>
    public Guid Id { get; init; }

    /// <summary>任务ID</summary>
    public Guid JobId { get; init; }

    /// <summary>开始时间（UTC）</summary>
    public DateTime StartedAtUtc { get; init; }

    /// <summary>结束时间（UTC）</summary>
    public DateTime? CompletedAtUtc { get; init; }

    /// <summary>耗时（秒）</summary>
    public double DurationSeconds { get; init; }

    /// <summary>归档行数</summary>
    public long ArchivedRowCount { get; init; }

    /// <summary>批次循环次数</summary>
    public int BatchCount { get; init; }

    /// <summary>执行状态</summary>
    public JobExecutionStatus Status { get; init; }

    /// <summary>执行状态显示文本</summary>
    public string StatusDisplay { get; init; } = string.Empty;

    /// <summary>错误消息</summary>
    public string? ErrorMessage { get; init; }

    /// <summary>详细错误信息（堆栈跟踪等）</summary>
    public string? ErrorDetails { get; init; }

    /// <summary>单批次平均耗时（毫秒）</summary>
    public double AverageBatchDurationMs { get; init; }

    /// <summary>数据传输速率（行/秒）</summary>
    public double TransferRateRowsPerSecond { get; init; }

    /// <summary>是否触发最大行数限制</summary>
    public bool MaxRowsLimitReached { get; init; }
}

/// <summary>
/// 表验证响应（用于验证是否为普通表）
/// </summary>
public record TableValidationResponse
{
    /// <summary>是否为有效的普通表</summary>
    public bool IsValid { get; init; }

    /// <summary>是否为分区表</summary>
    public bool IsPartitionedTable { get; init; }

    /// <summary>错误提示（若 IsValid=false）</summary>
    public string? ErrorMessage { get; init; }

    /// <summary>表列信息（含类型、是否索引）</summary>
    public List<ColumnInfo>? Columns { get; init; }
}

/// <summary>
/// 列信息
/// </summary>
public record ColumnInfo
{
    /// <summary>列名</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>数据类型</summary>
    public string DataType { get; init; } = string.Empty;

    /// <summary>是否有索引</summary>
    public bool IsIndexed { get; init; }

    /// <summary>是否推荐用于筛选（时间字段、主键等）</summary>
    public bool IsRecommendedForFilter { get; init; }
}
