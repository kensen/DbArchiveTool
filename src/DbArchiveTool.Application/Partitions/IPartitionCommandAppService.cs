using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 定义分区命令应用服务接口，负责预览、执行与审批。
/// </summary>
public interface IPartitionCommandAppService
{
    Task<Result<PartitionCommandPreviewDto>> PreviewSplitAsync(SplitPartitionRequest request, CancellationToken cancellationToken = default);
    Task<Result<Guid>> ExecuteSplitAsync(SplitPartitionRequest request, CancellationToken cancellationToken = default);

    Task<Result<PartitionCommandPreviewDto>> PreviewMergeAsync(MergePartitionRequest request, CancellationToken cancellationToken = default);
    Task<Result<Guid>> ExecuteMergeAsync(MergePartitionRequest request, CancellationToken cancellationToken = default);

    Task<Result<PartitionCommandPreviewDto>> PreviewSwitchAsync(SwitchPartitionRequest request, CancellationToken cancellationToken = default);
    Task<Result<Guid>> ExecuteSwitchAsync(SwitchPartitionRequest request, CancellationToken cancellationToken = default);

    Task<Result> ApproveAsync(Guid commandId, string approver, CancellationToken cancellationToken = default);
    Task<Result> RejectAsync(Guid commandId, string approver, string reason, CancellationToken cancellationToken = default);
    Task<Result<PartitionCommandStatusDto>> GetStatusAsync(Guid commandId, CancellationToken cancellationToken = default);
}

public sealed record SplitPartitionRequest(Guid DataSourceId, string SchemaName, string TableName, IReadOnlyList<string> Boundaries, bool BackupConfirmed, string RequestedBy);

public sealed record MergePartitionRequest(Guid DataSourceId, string SchemaName, string TableName, string BoundaryKey, bool BackupConfirmed, string RequestedBy);

public sealed record SwitchPartitionRequest(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    bool CreateStagingTable,
    bool BackupConfirmed,
    string RequestedBy);

/// <summary>
/// 预览结果 DTO，携带脚本与风险提示。
/// </summary>
public sealed record PartitionCommandPreviewDto(string Script, IReadOnlyList<string> RiskWarnings);

/// <summary>
/// 命令状态 DTO，用于展示后台执行进度。
/// </summary>
public sealed record PartitionCommandStatusDto(
    Guid CommandId,
    PartitionCommandStatus Status,
    DateTimeOffset RequestedAt,
    DateTimeOffset? ExecutedAt,
    DateTimeOffset? CompletedAt,
    string? FailureReason,
    string? ExecutionLog);
