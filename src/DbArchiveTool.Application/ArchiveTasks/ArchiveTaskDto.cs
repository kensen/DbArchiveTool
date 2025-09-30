namespace DbArchiveTool.Application.ArchiveTasks;

public sealed record ArchiveTaskDto(
    Guid Id,
    Guid DataSourceId,
    string SourceTableName,
    string TargetTableName,
    int Status,
    bool IsAutoArchive,
    DateTime? StartedAtUtc,
    DateTime? CompletedAtUtc,
    long? SourceRowCount,
    long? TargetRowCount,
    string? LegacyOperationRecordId
);
