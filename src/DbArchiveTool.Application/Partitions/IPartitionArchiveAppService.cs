using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 归档方案相关的应用服务（占位实现）。
/// </summary>
public interface IPartitionArchiveAppService
{
    Task<Result<ArchivePlanDto>> PlanArchiveWithBcpAsync(BcpArchivePlanRequest request, CancellationToken cancellationToken = default);

    Task<Result<ArchivePlanDto>> PlanArchiveWithBulkCopyAsync(BulkCopyArchivePlanRequest request, CancellationToken cancellationToken = default);
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
