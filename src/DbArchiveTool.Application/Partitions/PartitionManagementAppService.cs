using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区管理应用服务，实现分区概览与安全读取。
/// </summary>
internal sealed class PartitionManagementAppService : IPartitionManagementAppService
{
    private readonly IPartitionMetadataRepository repository;
    private readonly ILogger<PartitionManagementAppService> logger;

    public PartitionManagementAppService(IPartitionMetadataRepository repository, ILogger<PartitionManagementAppService> logger)
    {
        this.repository = repository;
        this.logger = logger;
    }

    /// <inheritdoc />
    public async Task<Result<PartitionOverviewDto>> GetOverviewAsync(PartitionOverviewRequest request, CancellationToken cancellationToken = default)
    {
        var config = await repository.GetConfigurationAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (config is null)
        {
            return Result<PartitionOverviewDto>.Failure("未找到分区配置，请确认目标表已启用分区。");
        }

        var boundaries = await repository.ListBoundariesAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (boundaries.Count == 0)
        {
            logger.LogWarning("未从分区函数中解析到任何边界: {Schema}.{Table}", request.SchemaName, request.TableName);
        }

        var items = boundaries
            .Select(x => new PartitionBoundaryItemDto(x.SortKey, x.Value.ToLiteral()))
            .ToList();

        var dto = new PartitionOverviewDto(
            $"[{config.SchemaName}].[{config.TableName}]",
            config.IsRangeRight,
            items);

        return Result<PartitionOverviewDto>.Success(dto);
    }

    /// <inheritdoc />
    public async Task<Result<PartitionBoundarySafetyDto>> GetBoundarySafetyAsync(PartitionBoundarySafetyRequest request, CancellationToken cancellationToken = default)
    {
        var safety = await repository.GetSafetySnapshotAsync(request.DataSourceId, request.SchemaName, request.TableName, request.BoundaryKey, cancellationToken);
        var dto = new PartitionBoundarySafetyDto(safety.BoundaryKey, safety.RowCount, safety.HasData, safety.RequiresSwitchStaging);
        return Result<PartitionBoundarySafetyDto>.Success(dto);
    }
}
