namespace DbArchiveTool.Application.ArchiveTasks;

public sealed record CreateArchiveTaskRequest(
    Guid DataSourceId,
    string SourceTableName,
    string TargetTableName,
    bool IsAutoArchive,
    string? LegacyOperationRecordId
);
