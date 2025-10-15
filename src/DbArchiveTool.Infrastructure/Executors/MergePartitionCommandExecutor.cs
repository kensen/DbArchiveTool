using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 负责执行合并分区命令。
/// </summary>
internal sealed class MergePartitionCommandExecutor : IPartitionCommandExecutor
{
    private readonly IPartitionMetadataRepository metadataRepository;
    private readonly SqlPartitionCommandExecutor sqlExecutor;
    private readonly ILogger<MergePartitionCommandExecutor> logger;

    public MergePartitionCommandExecutor(
        IPartitionMetadataRepository metadataRepository,
        SqlPartitionCommandExecutor sqlExecutor,
        ILogger<MergePartitionCommandExecutor> logger)
    {
        this.metadataRepository = metadataRepository;
        this.sqlExecutor = sqlExecutor;
        this.logger = logger;
    }

    public PartitionCommandType CommandType => PartitionCommandType.Merge;

    public async Task<SqlExecutionResult> ExecuteAsync(PartitionCommand command, CancellationToken cancellationToken)
    {
        if (command is null)
        {
            throw new ArgumentNullException(nameof(command));
        }

        var boundaryKey = ParseBoundaryKey(command.Payload);
        if (string.IsNullOrWhiteSpace(boundaryKey))
        {
            logger.LogWarning("合并命令 {CommandId} 未提供边界键。", command.Id);
            return SqlExecutionResult.Failure("合并命令缺少边界键。", 0, null);
        }

        var configuration = await metadataRepository.GetConfigurationAsync(
            command.DataSourceId,
            command.SchemaName,
            command.TableName,
            cancellationToken);

        if (configuration is null)
        {
            var message = $"未找到 {command.SchemaName}.{command.TableName} 的分区配置，无法执行合并。";
            logger.LogWarning("合并命令 {CommandId} 执行失败: {Message}", command.Id, message);
            return SqlExecutionResult.Failure(message, 0, null);
        }

        if (!configuration.Boundaries.Any(b => b.SortKey.Equals(boundaryKey, StringComparison.Ordinal)))
        {
            var message = $"分区边界 {boundaryKey} 不存在，终止合并。";
            logger.LogWarning("合并命令 {CommandId} 找不到边界 {BoundaryKey}。", command.Id, boundaryKey);
            return SqlExecutionResult.Failure(message, 0, null);
        }

        logger.LogInformation(
            "开始执行合并命令 {CommandId}，目标表 {Schema}.{Table}，移除边界 {BoundaryKey}。",
            command.Id,
            command.SchemaName,
            command.TableName,
            boundaryKey);

        return await sqlExecutor.ExecuteMergeWithTransactionAsync(
            command.DataSourceId,
            configuration,
            boundaryKey,
            cancellationToken);
    }

    private static string? ParseBoundaryKey(string payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(payload);
            if (document.RootElement.TryGetProperty("BoundaryKey", out var element) && element.ValueKind == JsonValueKind.String)
            {
                return element.GetString();
            }
        }
        catch (JsonException)
        {
            return null;
        }

        return null;
    }
}
