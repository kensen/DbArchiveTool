using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.ArchiveTasks;

public sealed class ArchiveTask : AggregateRoot
{
    public Guid DataSourceId { get; private set; }
    public string SourceTableName { get; private set; } = string.Empty;
    public string TargetTableName { get; private set; } = string.Empty;
    public ArchiveTaskStatus Status { get; private set; } = ArchiveTaskStatus.Pending;
    public bool IsAutoArchive { get; private set; }
    public DateTime? StartedAtUtc { get; private set; }
    public DateTime? CompletedAtUtc { get; private set; }
    public long? SourceRowCount { get; private set; }
    public long? TargetRowCount { get; private set; }
    public string? LegacyOperationRecordId { get; private set; }

    private ArchiveTask() { }

    public ArchiveTask(Guid dataSourceId, string sourceTableName, string targetTableName, bool isAutoArchive)
    {
        DataSourceId = dataSourceId;
        SourceTableName = sourceTableName;
        TargetTableName = targetTableName;
        IsAutoArchive = isAutoArchive;
    }

    public void MarkRunning()
    {
        Status = ArchiveTaskStatus.Running;
        StartedAtUtc = DateTime.UtcNow;
    }

    public void MarkCompleted(long? sourceCount, long? targetCount)
    {
        Status = ArchiveTaskStatus.Succeeded;
        CompletedAtUtc = DateTime.UtcNow;
        SourceRowCount = sourceCount;
        TargetRowCount = targetCount;
    }

    public void MarkFailed(string failureReason)
    {
        Status = ArchiveTaskStatus.Failed;
        CompletedAtUtc = DateTime.UtcNow;
        RaiseDomainEvent(new ArchiveTaskFailedEvent(Id, failureReason));
    }

    public void UseLegacyId(string legacyId)
    {
        LegacyOperationRecordId = legacyId;
    }
}
