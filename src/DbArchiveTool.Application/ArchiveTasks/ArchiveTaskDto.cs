namespace DbArchiveTool.Application.ArchiveTasks;

/// <summary>归档任务信息传输对象,用于向界面层展示任务状态。</summary>
public sealed record class ArchiveTaskDto
{
    /// <summary>创建传输对象并赋值各字段。</summary>
    public ArchiveTaskDto(
        Guid id,
        Guid dataSourceId,
        string sourceTableName,
        string targetTableName,
        int status,
        bool isAutoArchive,
        DateTime? startedAtUtc,
        DateTime? completedAtUtc,
        long? sourceRowCount,
        long? targetRowCount,
        string? legacyOperationRecordId)
    {
        Id = id;
        DataSourceId = dataSourceId;
        SourceTableName = sourceTableName;
        TargetTableName = targetTableName;
        Status = status;
        IsAutoArchive = isAutoArchive;
        StartedAtUtc = startedAtUtc;
        CompletedAtUtc = completedAtUtc;
        SourceRowCount = sourceRowCount;
        TargetRowCount = targetRowCount;
        LegacyOperationRecordId = legacyOperationRecordId;
    }

    /// <summary>任务主键标识。</summary>
    public Guid Id { get; init; }

    /// <summary>关联的数据源标识。</summary>
    public Guid DataSourceId { get; init; }

    /// <summary>来源表名称。</summary>
    public string SourceTableName { get; init; } = string.Empty;

    /// <summary>目标表名称。</summary>
    public string TargetTableName { get; init; } = string.Empty;

    /// <summary>任务状态,与领域层的 ArchiveTaskStatus 枚举值保持一致。</summary>
    public int Status { get; init; }

    /// <summary>是否由自动调度创建。</summary>
    public bool IsAutoArchive { get; init; }

    /// <summary>任务开始执行的 UTC 时间。</summary>
    public DateTime? StartedAtUtc { get; init; }

    /// <summary>任务完成或失败的 UTC 时间。</summary>
    public DateTime? CompletedAtUtc { get; init; }

    /// <summary>来源库数据行数。</summary>
    public long? SourceRowCount { get; init; }

    /// <summary>目标库数据行数。</summary>
    public long? TargetRowCount { get; init; }

    /// <summary>兼容历史记录的操作编号。</summary>
    public string? LegacyOperationRecordId { get; init; }
}
