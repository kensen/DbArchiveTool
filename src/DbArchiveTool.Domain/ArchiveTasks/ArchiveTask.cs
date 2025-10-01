using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.ArchiveTasks;

/// <summary>归档任务聚合根,记录单次归档执行的全生命周期信息。</summary>
public sealed class ArchiveTask : AggregateRoot
{
    /// <summary>执行任务关联的数据源标识。</summary>
    public Guid DataSourceId { get; private set; }

    /// <summary>需要归档的来源表名称。</summary>
    public string SourceTableName { get; private set; } = string.Empty;

    /// <summary>归档后的目标表名称。</summary>
    public string TargetTableName { get; private set; } = string.Empty;

    /// <summary>任务当前状态,默认处于待处理。</summary>
    public ArchiveTaskStatus Status { get; private set; } = ArchiveTaskStatus.Pending;

    /// <summary>标识是否由自动调度创建。</summary>
    public bool IsAutoArchive { get; private set; }

    /// <summary>任务开始执行的 UTC 时间。</summary>
    public DateTime? StartedAtUtc { get; private set; }

    /// <summary>任务完成或失败的 UTC 时间。</summary>
    public DateTime? CompletedAtUtc { get; private set; }

    /// <summary>来源库参与归档的数据行数。</summary>
    public long? SourceRowCount { get; private set; }

    /// <summary>目标库写入的数据行数。</summary>
    public long? TargetRowCount { get; private set; }

    /// <summary>兼容历史记录的操作编号。</summary>
    public string? LegacyOperationRecordId { get; private set; }

    /// <summary>仅供 ORM 使用的无参构造函数。</summary>
    private ArchiveTask() { }

    /// <summary>创建新的归档任务实例。</summary>
    /// <param name="dataSourceId">关联的数据源标识。</param>
    /// <param name="sourceTableName">来源表名称。</param>
    /// <param name="targetTableName">目标表名称。</param>
    /// <param name="isAutoArchive">是否由自动调度创建。</param>
    public ArchiveTask(Guid dataSourceId, string sourceTableName, string targetTableName, bool isAutoArchive)
    {
        DataSourceId = dataSourceId;
        SourceTableName = sourceTableName;
        TargetTableName = targetTableName;
        IsAutoArchive = isAutoArchive;
    }

    /// <summary>将任务标记为执行中并记录开始时间。</summary>
    public void MarkRunning()
    {
        Status = ArchiveTaskStatus.Running;
        StartedAtUtc = DateTime.UtcNow;
    }

    /// <summary>结束任务并记录成功结果。</summary>
    /// <param name="sourceCount">来源库数据行数。</param>
    /// <param name="targetCount">目标库数据行数。</param>
    public void MarkCompleted(long? sourceCount, long? targetCount)
    {
        Status = ArchiveTaskStatus.Succeeded;
        CompletedAtUtc = DateTime.UtcNow;
        SourceRowCount = sourceCount;
        TargetRowCount = targetCount;
    }

    /// <summary>标记任务失败并发布失败领域事件。</summary>
    /// <param name="failureReason">失败原因描述。</param>
    public void MarkFailed(string failureReason)
    {
        Status = ArchiveTaskStatus.Failed;
        CompletedAtUtc = DateTime.UtcNow;
        RaiseDomainEvent(new ArchiveTaskFailedEvent(Id, failureReason));
    }

    /// <summary>记录历史操作流水号,用于兼容旧有数据结构。</summary>
    /// <param name="legacyId">历史操作记录编号。</param>
    public void UseLegacyId(string legacyId)
    {
        LegacyOperationRecordId = legacyId;
    }
}
