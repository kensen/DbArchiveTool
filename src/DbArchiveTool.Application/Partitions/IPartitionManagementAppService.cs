using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 定义分区管理相关的应用服务契约。
/// </summary>
public interface IPartitionManagementAppService
{
    /// <summary>获取目标表的分区概览信息。</summary>
    Task<Result<PartitionOverviewDto>> GetOverviewAsync(PartitionOverviewRequest request, CancellationToken cancellationToken = default);

    /// <summary>查询指定分区边界的安全状态。</summary>
    Task<Result<PartitionBoundarySafetyDto>> GetBoundarySafetyAsync(PartitionBoundarySafetyRequest request, CancellationToken cancellationToken = default);
}

/// <summary>
/// 分区概览请求。
/// </summary>
public sealed record PartitionOverviewRequest(Guid DataSourceId, string SchemaName, string TableName);

/// <summary>
/// 分区概览返回 DTO。
/// </summary>
public sealed record PartitionOverviewDto(
    string TableName,
    bool IsRangeRight,
    IReadOnlyList<PartitionBoundaryItemDto> Boundaries);

/// <summary>
/// 分区概览中的单个边界项。
/// </summary>
public sealed record PartitionBoundaryItemDto(string Key, string LiteralValue);

/// <summary>
/// 分区安全请求。
/// </summary>
public sealed record PartitionBoundarySafetyRequest(Guid DataSourceId, string SchemaName, string TableName, string BoundaryKey);

/// <summary>
/// 分区安全返回 DTO。
/// </summary>
public sealed record PartitionBoundarySafetyDto(string BoundaryKey, long RowCount, bool HasData, bool SuggestSwitch);
