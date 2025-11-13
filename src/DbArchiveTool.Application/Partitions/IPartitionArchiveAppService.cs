using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 归档方案相关的应用服务。
/// </summary>
public interface IPartitionArchiveAppService
{
    Task<Result<ArchivePlanDto>> PlanArchiveWithBcpAsync(BcpArchivePlanRequest request, CancellationToken cancellationToken = default);

    Task<Result<ArchivePlanDto>> PlanArchiveWithBulkCopyAsync(BulkCopyArchivePlanRequest request, CancellationToken cancellationToken = default);

    Task<Result<ArchiveInspectionResultDto>> InspectBcpAsync(BcpArchiveInspectRequest request, CancellationToken cancellationToken = default);

    Task<Result<ArchiveInspectionResultDto>> InspectBulkCopyAsync(BulkCopyArchiveInspectRequest request, CancellationToken cancellationToken = default);

    Task<Result<Guid>> ExecuteWithBcpAsync(BcpArchiveExecuteRequest request, CancellationToken cancellationToken = default);

    Task<Result<Guid>> ExecuteWithBulkCopyAsync(BulkCopyArchiveExecuteRequest request, CancellationToken cancellationToken = default);

    Task<Result<string>> ExecuteAutoFixAsync(ArchiveAutoFixRequest request, CancellationToken cancellationToken = default);
}

/// <summary>
/// BCP 归档请求（占位）。
/// </summary>
public sealed record BcpArchivePlanRequest(
    Guid PartitionConfigurationId,
    string RequestedBy,
    string? TargetConnectionString,
    string? TargetDatabase,
    string? TargetTable);

/// <summary>
/// BulkCopy 归档请求（占位）。
/// </summary>
public sealed record BulkCopyArchivePlanRequest(
    Guid PartitionConfigurationId,
    string RequestedBy,
    string? TargetConnectionString,
    string? TargetDatabase,
    string? TargetTable);

/// <summary>
/// 归档计划结果。
/// </summary>
public sealed record ArchivePlanDto(
    string Scheme,
    string Status,
    string Message);

/// <summary>
/// BCP 归档预检请求。
/// </summary>
public sealed record BcpArchiveInspectRequest(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    string TempDirectory,
    string RequestedBy);

/// <summary>
/// BCP 归档执行请求。
/// </summary>
public sealed record BcpArchiveExecuteRequest(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    string TempDirectory,
    int BatchSize,
    bool UseNativeFormat,
    int MaxErrors,
    int TimeoutSeconds,
    bool BackupConfirmed,
    string RequestedBy);

/// <summary>
/// BulkCopy 归档预检请求。
/// </summary>
public sealed record BulkCopyArchiveInspectRequest(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    string RequestedBy);

/// <summary>
/// BulkCopy 归档执行请求。
/// </summary>
public sealed record BulkCopyArchiveExecuteRequest(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    int BatchSize,
    int NotifyAfterRows,
    int TimeoutSeconds,
    bool EnableStreaming,
    bool BackupConfirmed,
    string RequestedBy);

/// <summary>
/// 归档预检结果 DTO。
/// </summary>
public sealed record ArchiveInspectionResultDto(
    bool CanExecute,
    bool TargetTableExists,
    bool HasRequiredPermissions,
    string? BcpCommandPath,
    List<ArchiveInspectionIssue> BlockingIssues,
    List<ArchiveInspectionIssue> Warnings,
    List<ArchiveInspectionAutoFixStep> AutoFixSteps);

/// <summary>
/// 归档预检问题。
/// </summary>
public sealed record ArchiveInspectionIssue(
    string Code,
    string Message,
    string? Recommendation);

/// <summary>
/// 归档预检自动补齐步骤。
/// </summary>
public sealed record ArchiveInspectionAutoFixStep(
    string Code,
    string Description,
    string Action);

/// <summary>
/// 归档自动修复请求。
/// </summary>
public sealed record ArchiveAutoFixRequest(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string TargetTable,
    string? TargetDatabase,
    string FixCode,
    string RequestedBy);
