using DbArchiveTool.Domain.Entities;
using DbArchiveTool.Shared.Archive;

namespace DbArchiveTool.Api.DTOs.Archives;

/// <summary>
/// 定时归档任务列表项 DTO
/// </summary>
public sealed class ScheduledArchiveJobListItemDto
{
    /// <summary>任务ID</summary>
    public Guid Id { get; set; }

    /// <summary>任务名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>描述</summary>
    public string? Description { get; set; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; set; }

    /// <summary>源架构名</summary>
    public string SourceSchemaName { get; set; } = string.Empty;

    /// <summary>源表名</summary>
    public string SourceTableName { get; set; } = string.Empty;

    /// <summary>目标架构名</summary>
    public string TargetSchemaName { get; set; } = string.Empty;

    /// <summary>目标表名</summary>
    public string TargetTableName { get; set; } = string.Empty;

    /// <summary>归档方法</summary>
    public ArchiveMethod ArchiveMethod { get; set; }

    /// <summary>批次大小(每次数据库操作的行数)</summary>
    public int BatchSize { get; set; }

    /// <summary>每次执行最大行数</summary>
    public int MaxRowsPerExecution { get; set; }

    /// <summary>执行间隔(分钟)</summary>
    public int IntervalMinutes { get; set; }

    /// <summary>是否启用</summary>
    public bool IsEnabled { get; set; }

    /// <summary>下次执行时间</summary>
    public DateTime? NextExecutionAtUtc { get; set; }

    /// <summary>最后执行时间</summary>
    public DateTime? LastExecutionAtUtc { get; set; }

    /// <summary>最后执行状态</summary>
    public JobExecutionStatus LastExecutionStatus { get; set; }

    /// <summary>最后归档行数</summary>
    public long? LastArchivedRowCount { get; set; }

    /// <summary>总执行次数</summary>
    public long TotalExecutionCount { get; set; }

    /// <summary>总归档行数</summary>
    public long TotalArchivedRowCount { get; set; }

    /// <summary>连续失败次数</summary>
    public int ConsecutiveFailureCount { get; set; }

    /// <summary>创建时间</summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>更新时间</summary>
    public DateTime UpdatedAtUtc { get; set; }
}

/// <summary>
/// 定时归档任务详情 DTO
/// </summary>
public sealed class ScheduledArchiveJobDetailDto
{
    /// <summary>任务ID</summary>
    public Guid Id { get; set; }

    /// <summary>任务名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>描述</summary>
    public string? Description { get; set; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; set; }

    /// <summary>源架构名</summary>
    public string SourceSchemaName { get; set; } = string.Empty;

    /// <summary>源表名</summary>
    public string SourceTableName { get; set; } = string.Empty;

    /// <summary>目标架构名</summary>
    public string TargetSchemaName { get; set; } = string.Empty;

    /// <summary>目标表名</summary>
    public string TargetTableName { get; set; } = string.Empty;

    /// <summary>归档过滤列名</summary>
    public string ArchiveFilterColumn { get; set; } = string.Empty;

    /// <summary>归档过滤条件</summary>
    public string ArchiveFilterCondition { get; set; } = string.Empty;

    /// <summary>归档过滤条件定义(JSON格式,用于编辑时还原表单)</summary>
    public string? ArchiveFilterDefinition { get; set; }

    /// <summary>归档方法</summary>
    public ArchiveMethod ArchiveMethod { get; set; }

    /// <summary>是否在归档后删除源数据</summary>
    public bool DeleteSourceDataAfterArchive { get; set; }

    /// <summary>批次大小(每次数据库操作的行数)</summary>
    public int BatchSize { get; set; }

    /// <summary>每次执行最大行数</summary>
    public int MaxRowsPerExecution { get; set; }

    /// <summary>执行间隔(分钟)</summary>
    public int IntervalMinutes { get; set; }

    /// <summary>Cron 表达式</summary>
    public string? CronExpression { get; set; }

    /// <summary>是否启用</summary>
    public bool IsEnabled { get; set; }

    /// <summary>下次执行时间</summary>
    public DateTime? NextExecutionAtUtc { get; set; }

    /// <summary>最后执行时间</summary>
    public DateTime? LastExecutionAtUtc { get; set; }

    /// <summary>最后执行状态</summary>
    public JobExecutionStatus LastExecutionStatus { get; set; }

    /// <summary>最后执行错误信息</summary>
    public string? LastExecutionError { get; set; }

    /// <summary>最后归档行数</summary>
    public long? LastArchivedRowCount { get; set; }

    /// <summary>总执行次数</summary>
    public long TotalExecutionCount { get; set; }

    /// <summary>总归档行数</summary>
    public long TotalArchivedRowCount { get; set; }

    /// <summary>连续失败次数</summary>
    public int ConsecutiveFailureCount { get; set; }

    /// <summary>最大连续失败次数</summary>
    public int MaxConsecutiveFailures { get; set; }

    /// <summary>创建时间</summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>创建人</summary>
    public string CreatedBy { get; set; } = string.Empty;

    /// <summary>更新时间</summary>
    public DateTime UpdatedAtUtc { get; set; }

    /// <summary>更新人</summary>
    public string UpdatedBy { get; set; } = string.Empty;
}

/// <summary>
/// 创建定时归档任务请求
/// </summary>
public sealed class CreateScheduledArchiveJobRequest
{
    /// <summary>任务名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>描述</summary>
    public string? Description { get; set; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; set; }

    /// <summary>源架构名</summary>
    public string SourceSchemaName { get; set; } = "dbo";

    /// <summary>源表名</summary>
    public string SourceTableName { get; set; } = string.Empty;

    /// <summary>目标架构名</summary>
    public string TargetSchemaName { get; set; } = "dbo";

    /// <summary>目标表名</summary>
    public string TargetTableName { get; set; } = string.Empty;

    /// <summary>归档过滤列名</summary>
    public string ArchiveFilterColumn { get; set; } = string.Empty;

    /// <summary>归档过滤条件</summary>
    public string ArchiveFilterCondition { get; set; } = string.Empty;

    /// <summary>归档过滤条件定义(JSON格式,用于编辑时还原表单)</summary>
    public string? ArchiveFilterDefinition { get; set; }

    /// <summary>归档方法</summary>
    public ArchiveMethod ArchiveMethod { get; set; }

    /// <summary>是否在归档后删除源数据</summary>
    public bool DeleteSourceDataAfterArchive { get; set; } = true;

    /// <summary>批次大小</summary>
    public int BatchSize { get; set; } = 5000;

    /// <summary>每次执行最大行数</summary>
    public int MaxRowsPerExecution { get; set; } = 50000;

    /// <summary>执行间隔(分钟)</summary>
    public int IntervalMinutes { get; set; } = 5;

    /// <summary>Cron 表达式(可选,与 IntervalMinutes 二选一)</summary>
    public string? CronExpression { get; set; }

    /// <summary>最大连续失败次数</summary>
    public int MaxConsecutiveFailures { get; set; } = 5;
}

/// <summary>
/// 更新定时归档任务请求
/// </summary>
public sealed class UpdateScheduledArchiveJobRequest
{
    /// <summary>任务名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>描述</summary>
    public string? Description { get; set; }

    /// <summary>源架构名</summary>
    public string SourceSchemaName { get; set; } = "dbo";

    /// <summary>源表名</summary>
    public string SourceTableName { get; set; } = string.Empty;

    /// <summary>目标架构名</summary>
    public string TargetSchemaName { get; set; } = "dbo";

    /// <summary>目标表名</summary>
    public string TargetTableName { get; set; } = string.Empty;

    /// <summary>归档过滤列名</summary>
    public string ArchiveFilterColumn { get; set; } = string.Empty;

    /// <summary>归档过滤条件</summary>
    public string ArchiveFilterCondition { get; set; } = string.Empty;

    /// <summary>归档过滤条件定义(JSON格式,用于编辑时还原表单)</summary>
    public string? ArchiveFilterDefinition { get; set; }

    /// <summary>归档方法</summary>
    public ArchiveMethod ArchiveMethod { get; set; }

    /// <summary>是否在归档后删除源数据</summary>
    public bool DeleteSourceDataAfterArchive { get; set; } = true;

    /// <summary>批次大小</summary>
    public int BatchSize { get; set; } = 5000;

    /// <summary>每次执行最大行数</summary>
    public int MaxRowsPerExecution { get; set; } = 50000;

    /// <summary>执行间隔(分钟)</summary>
    public int IntervalMinutes { get; set; } = 5;

    /// <summary>Cron 表达式(可选,与 IntervalMinutes 二选一)</summary>
    public string? CronExpression { get; set; }

    /// <summary>最大连续失败次数</summary>
    public int MaxConsecutiveFailures { get; set; } = 5;
}

/// <summary>
/// 任务统计信息 DTO
/// </summary>
public sealed class ScheduledArchiveJobStatisticsDto
{
    /// <summary>任务ID</summary>
    public Guid Id { get; set; }

    /// <summary>任务名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>总执行次数</summary>
    public long TotalExecutionCount { get; set; }

    /// <summary>总归档行数</summary>
    public long TotalArchivedRowCount { get; set; }

    /// <summary>成功次数</summary>
    public long SuccessCount { get; set; }

    /// <summary>失败次数</summary>
    public long FailureCount { get; set; }

    /// <summary>跳过次数</summary>
    public long SkippedCount { get; set; }

    /// <summary>成功率(%)</summary>
    public double SuccessRate { get; set; }

    /// <summary>平均每次归档行数</summary>
    public long AverageArchivedRowCount { get; set; }

    /// <summary>最后执行时间</summary>
    public DateTime? LastExecutionAtUtc { get; set; }

    /// <summary>最后执行状态</summary>
    public JobExecutionStatus LastExecutionStatus { get; set; }

    /// <summary>连续失败次数</summary>
    public int ConsecutiveFailureCount { get; set; }
}
