using DbArchiveTool.Domain.Abstractions;
using DbArchiveTool.Domain.Entities;
using DbArchiveTool.Shared.Archive;

namespace DbArchiveTool.Domain.ScheduledArchiveJobs;

/// <summary>
/// 定时归档任务实体
/// 用于配置和管理小批量持续归档任务,支持按固定间隔或 Cron 表达式执行
/// </summary>
public sealed class ScheduledArchiveJob : AggregateRoot
{
    /// <summary>任务名称</summary>
    public string Name { get; private set; } = string.Empty;

    /// <summary>任务描述</summary>
    public string? Description { get; private set; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; private set; }

    /// <summary>源表架构名</summary>
    public string SourceSchemaName { get; private set; } = "dbo";

    /// <summary>源表名称</summary>
    public string SourceTableName { get; private set; } = string.Empty;

    /// <summary>目标表架构名</summary>
    public string TargetSchemaName { get; private set; } = "dbo";

    /// <summary>目标表名称</summary>
    public string TargetTableName { get; private set; } = string.Empty;

    /// <summary>归档过滤列名(如 CreateDate)</summary>
    public string ArchiveFilterColumn { get; private set; } = string.Empty;

    /// <summary>归档过滤条件(如 &lt; DATEADD(minute, -10, GETDATE()))</summary>
    public string ArchiveFilterCondition { get; private set; } = string.Empty;

    /// <summary>归档过滤条件定义(JSON格式,用于编辑时还原表单)</summary>
    public string? ArchiveFilterDefinition { get; private set; }

    /// <summary>归档方法</summary>
    public ArchiveMethod ArchiveMethod { get; private set; }

    /// <summary>是否在归档后删除源数据</summary>
    public bool DeleteSourceDataAfterArchive { get; private set; } = true;

    /// <summary>每批次数据库操作的行数(如 5000)</summary>
    public int BatchSize { get; private set; } = 5000;

    /// <summary>每次任务执行最多归档的总行数(如 50000,超过则下次继续)</summary>
    public int MaxRowsPerExecution { get; private set; } = 50000;

    /// <summary>执行间隔(分钟)- 如 5 表示每5分钟执行一次</summary>
    public int IntervalMinutes { get; private set; } = 5;

    /// <summary>Cron 表达式(备用方式,与 IntervalMinutes 二选一)</summary>
    public string? CronExpression { get; private set; }

    /// <summary>是否启用</summary>
    public bool IsEnabled { get; private set; } = true;

    /// <summary>下次执行时间(UTC)</summary>
    public DateTime? NextExecutionAtUtc { get; private set; }

    /// <summary>最后执行时间(UTC)</summary>
    public DateTime? LastExecutionAtUtc { get; private set; }

    /// <summary>最后执行状态</summary>
    public JobExecutionStatus LastExecutionStatus { get; private set; } = JobExecutionStatus.NotStarted;

    /// <summary>最后执行错误信息</summary>
    public string? LastExecutionError { get; private set; }

    /// <summary>最后归档行数</summary>
    public long? LastArchivedRowCount { get; private set; }

    /// <summary>总执行次数</summary>
    public long TotalExecutionCount { get; private set; }

    /// <summary>总归档行数</summary>
    public long TotalArchivedRowCount { get; private set; }

    /// <summary>连续失败次数</summary>
    public int ConsecutiveFailureCount { get; private set; }

    /// <summary>最大连续失败次数(达到后自动禁用任务)</summary>
    public int MaxConsecutiveFailures { get; private set; } = 5;

    /// <summary>仅供 ORM 使用的无参构造函数</summary>
    private ScheduledArchiveJob() { }

    /// <summary>创建定时归档任务</summary>
    public ScheduledArchiveJob(
        string name,
        string? description,
        Guid dataSourceId,
        string sourceSchemaName,
        string sourceTableName,
        string targetSchemaName,
        string targetTableName,
        string archiveFilterColumn,
        string archiveFilterCondition,
        string? archiveFilterDefinition,
        ArchiveMethod archiveMethod,
        bool deleteSourceDataAfterArchive,
        int batchSize,
        int maxRowsPerExecution,
        int intervalMinutes,
        string? cronExpression,
        int maxConsecutiveFailures,
        string createdBy)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("任务名称不能为空", nameof(name));
        if (dataSourceId == Guid.Empty)
            throw new ArgumentException("数据源ID无效", nameof(dataSourceId));
        if (string.IsNullOrWhiteSpace(sourceSchemaName))
            throw new ArgumentException("源表架构名不能为空", nameof(sourceSchemaName));
        if (string.IsNullOrWhiteSpace(sourceTableName))
            throw new ArgumentException("源表名称不能为空", nameof(sourceTableName));
        if (string.IsNullOrWhiteSpace(targetSchemaName))
            throw new ArgumentException("目标表架构名不能为空", nameof(targetSchemaName));
        if (string.IsNullOrWhiteSpace(targetTableName))
            throw new ArgumentException("目标表名称不能为空", nameof(targetTableName));
        if (string.IsNullOrWhiteSpace(archiveFilterColumn))
            throw new ArgumentException("归档过滤列名不能为空", nameof(archiveFilterColumn));
        if (string.IsNullOrWhiteSpace(archiveFilterCondition))
            throw new ArgumentException("归档过滤条件不能为空", nameof(archiveFilterCondition));
        if (batchSize <= 0)
            throw new ArgumentException("批次大小必须大于0", nameof(batchSize));
        if (maxRowsPerExecution < batchSize)
            throw new ArgumentException("每次执行最大行数不能小于批次大小", nameof(maxRowsPerExecution));
        if (intervalMinutes <= 0 && string.IsNullOrWhiteSpace(cronExpression))
            throw new ArgumentException("执行间隔或 Cron 表达式必须提供其中之一", nameof(intervalMinutes));
        if (maxConsecutiveFailures <= 0)
            throw new ArgumentException("最大连续失败次数必须大于0", nameof(maxConsecutiveFailures));

        Name = name;
        Description = description;
        DataSourceId = dataSourceId;
        SourceSchemaName = sourceSchemaName;
        SourceTableName = sourceTableName;
        TargetSchemaName = targetSchemaName;
        TargetTableName = targetTableName;
        ArchiveFilterColumn = archiveFilterColumn;
        ArchiveFilterCondition = archiveFilterCondition;
        ArchiveFilterDefinition = archiveFilterDefinition;
        ArchiveMethod = archiveMethod;
        DeleteSourceDataAfterArchive = deleteSourceDataAfterArchive;
        BatchSize = batchSize;
        MaxRowsPerExecution = maxRowsPerExecution;
        IntervalMinutes = intervalMinutes;
        CronExpression = cronExpression;
        MaxConsecutiveFailures = maxConsecutiveFailures;

        IsEnabled = true;
        LastExecutionStatus = JobExecutionStatus.NotStarted;
        TotalExecutionCount = 0;
        TotalArchivedRowCount = 0;
        ConsecutiveFailureCount = 0;

        CreatedAtUtc = UpdatedAtUtc = DateTime.UtcNow;
        CreatedBy = UpdatedBy = createdBy ?? "System";
    }

    /// <summary>更新任务配置</summary>
    public void Update(
        string name,
        string? description,
        string sourceSchemaName,
        string sourceTableName,
        string targetSchemaName,
        string targetTableName,
        string archiveFilterColumn,
        string archiveFilterCondition,
        string? archiveFilterDefinition,
        ArchiveMethod archiveMethod,
        bool deleteSourceDataAfterArchive,
        int batchSize,
        int maxRowsPerExecution,
        int intervalMinutes,
        string? cronExpression,
        int maxConsecutiveFailures,
        string updatedBy)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("任务名称不能为空", nameof(name));
        if (string.IsNullOrWhiteSpace(sourceSchemaName))
            throw new ArgumentException("源表架构名不能为空", nameof(sourceSchemaName));
        if (string.IsNullOrWhiteSpace(sourceTableName))
            throw new ArgumentException("源表名称不能为空", nameof(sourceTableName));
        if (string.IsNullOrWhiteSpace(targetSchemaName))
            throw new ArgumentException("目标表架构名不能为空", nameof(targetSchemaName));
        if (string.IsNullOrWhiteSpace(targetTableName))
            throw new ArgumentException("目标表名称不能为空", nameof(targetTableName));
        if (string.IsNullOrWhiteSpace(archiveFilterColumn))
            throw new ArgumentException("归档过滤列名不能为空", nameof(archiveFilterColumn));
        if (string.IsNullOrWhiteSpace(archiveFilterCondition))
            throw new ArgumentException("归档过滤条件不能为空", nameof(archiveFilterCondition));
        if (batchSize <= 0)
            throw new ArgumentException("批次大小必须大于0", nameof(batchSize));
        if (maxRowsPerExecution < batchSize)
            throw new ArgumentException("每次执行最大行数不能小于批次大小", nameof(maxRowsPerExecution));
        if (intervalMinutes <= 0 && string.IsNullOrWhiteSpace(cronExpression))
            throw new ArgumentException("执行间隔或 Cron 表达式必须提供其中之一", nameof(intervalMinutes));
        if (maxConsecutiveFailures <= 0)
            throw new ArgumentException("最大连续失败次数必须大于0", nameof(maxConsecutiveFailures));

        Name = name;
        Description = description;
        SourceSchemaName = sourceSchemaName;
        SourceTableName = sourceTableName;
        TargetSchemaName = targetSchemaName;
        TargetTableName = targetTableName;
        ArchiveFilterColumn = archiveFilterColumn;
        ArchiveFilterCondition = archiveFilterCondition;
        ArchiveFilterDefinition = archiveFilterDefinition;
        ArchiveMethod = archiveMethod;
        DeleteSourceDataAfterArchive = deleteSourceDataAfterArchive;
        BatchSize = batchSize;
        MaxRowsPerExecution = maxRowsPerExecution;
        IntervalMinutes = intervalMinutes;
        CronExpression = cronExpression;
        MaxConsecutiveFailures = maxConsecutiveFailures;

        UpdatedAtUtc = DateTime.UtcNow;
        UpdatedBy = updatedBy;
    }

    /// <summary>启用任务</summary>
    public void Enable(string updatedBy)
    {
        if (IsEnabled)
            return;

        IsEnabled = true;
        UpdatedAtUtc = DateTime.UtcNow;
        UpdatedBy = updatedBy;
    }

    /// <summary>禁用任务</summary>
    public void Disable(string updatedBy)
    {
        if (!IsEnabled)
            return;

        IsEnabled = false;
        NextExecutionAtUtc = null;
        UpdatedAtUtc = DateTime.UtcNow;
        UpdatedBy = updatedBy;
    }

    /// <summary>更新任务执行结果</summary>
    public void UpdateExecutionResult(
        JobExecutionStatus status,
        long? archivedRowCount,
        string? errorMessage,
        DateTime executionTimeUtc,
        string updatedBy)
    {
        LastExecutionStatus = status;
        LastExecutionAtUtc = executionTimeUtc;
        LastArchivedRowCount = archivedRowCount;
        LastExecutionError = errorMessage;

        TotalExecutionCount++;

        if (status == JobExecutionStatus.Success)
        {
            if (archivedRowCount.HasValue)
                TotalArchivedRowCount += archivedRowCount.Value;

            ConsecutiveFailureCount = 0;
        }
        else if (status == JobExecutionStatus.Failed)
        {
            ConsecutiveFailureCount++;

            // 达到最大连续失败次数,自动禁用任务
            if (ConsecutiveFailureCount >= MaxConsecutiveFailures)
            {
                IsEnabled = false;
            }
        }
        else if (status == JobExecutionStatus.Skipped)
        {
            // 跳过时重置连续失败计数
            ConsecutiveFailureCount = 0;
        }

        UpdatedAtUtc = DateTime.UtcNow;
        UpdatedBy = updatedBy;
    }

    /// <summary>设置下次执行时间</summary>
    public void SetNextExecutionTime(DateTime? nextExecutionAtUtc, string updatedBy)
    {
        NextExecutionAtUtc = nextExecutionAtUtc;
        UpdatedAtUtc = DateTime.UtcNow;
        UpdatedBy = updatedBy;
    }

    /// <summary>重置统计信息</summary>
    public void ResetStatistics(string updatedBy)
    {
        TotalExecutionCount = 0;
        TotalArchivedRowCount = 0;
        ConsecutiveFailureCount = 0;
        LastExecutionAtUtc = null;
        LastExecutionStatus = JobExecutionStatus.NotStarted;
        LastExecutionError = null;
        LastArchivedRowCount = null;

        UpdatedAtUtc = DateTime.UtcNow;
        UpdatedBy = updatedBy;
    }
}
