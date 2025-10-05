using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

public interface IPartitionCommandAppService
{
    Task<Result<PartitionCommandPreviewDto>> PreviewSplitAsync(SplitPartitionRequest request, CancellationToken cancellationToken = default);
    Task<Result<Guid>> ExecuteSplitAsync(SplitPartitionRequest request, CancellationToken cancellationToken = default);
    Task<Result> ApproveAsync(Guid commandId, string approver, CancellationToken cancellationToken = default);
    Task<Result> RejectAsync(Guid commandId, string approver, string reason, CancellationToken cancellationToken = default);
}

public sealed record SplitPartitionRequest(Guid DataSourceId, string SchemaName, string TableName, IReadOnlyList<string> Boundaries, bool BackupConfirmed, string RequestedBy);

public sealed record PartitionCommandPreviewDto(string Script, IReadOnlyList<string> Warnings);
