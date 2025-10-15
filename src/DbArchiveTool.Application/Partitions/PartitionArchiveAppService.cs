using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区归档占位服务，实现跨实例归档方案的计划响应。
/// </summary>
internal sealed class PartitionArchiveAppService : IPartitionArchiveAppService
{
    public Task<Result<ArchivePlanDto>> PlanArchiveWithBcpAsync(BcpArchivePlanRequest request, CancellationToken cancellationToken = default)
    {
        var message = BuildPlannedMessage(
            "BCP",
            request.TargetConnectionString,
            request.TargetDatabase,
            request.TargetTable);
        var dto = new ArchivePlanDto("BCP", "Planned", message);
        return Task.FromResult(Result<ArchivePlanDto>.Success(dto));
    }

    public Task<Result<ArchivePlanDto>> PlanArchiveWithBulkCopyAsync(BulkCopyArchivePlanRequest request, CancellationToken cancellationToken = default)
    {
        var message = BuildPlannedMessage(
            "BulkCopy",
            request.TargetConnectionString,
            request.TargetDatabase,
            request.TargetTable);
        var dto = new ArchivePlanDto("BulkCopy", "Planned", message);
        return Task.FromResult(Result<ArchivePlanDto>.Success(dto));
    }

    private static string BuildPlannedMessage(string scheme, string? connection, string? database, string? table)
    {
        var targetInfo = string.Join(" / ", new[]
        {
            string.IsNullOrWhiteSpace(database) ? "目标库未指定" : database!,
            string.IsNullOrWhiteSpace(table) ? "目标表未指定" : table!
        });

        var connectionInfo = string.IsNullOrWhiteSpace(connection)
            ? "请在后续实现中提供目标实例连接。"
            : $"目标连接：{connection}";

        return $"{scheme} 归档方案规划中，当前仅输出计划结果。\n{connectionInfo}\n目标: {targetInfo}\n请选择其他方案或等待后续版本启用实际执行能力。";
    }
}
