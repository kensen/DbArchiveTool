using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 负责执行拆分分区命令，解析命令负载并调用底层 SQL 执行器。
/// </summary>
internal sealed class SplitPartitionCommandExecutor : IPartitionCommandExecutor
{
    private readonly IPartitionMetadataRepository metadataRepository;
    private readonly SqlPartitionCommandExecutor sqlExecutor;
    private readonly ILogger<SplitPartitionCommandExecutor> logger;

    public SplitPartitionCommandExecutor(
        IPartitionMetadataRepository metadataRepository,
        SqlPartitionCommandExecutor sqlExecutor,
        ILogger<SplitPartitionCommandExecutor> logger)
    {
        this.metadataRepository = metadataRepository;
        this.sqlExecutor = sqlExecutor;
        this.logger = logger;
    }

    public PartitionCommandType CommandType => PartitionCommandType.Split;

    public async Task<SqlExecutionResult> ExecuteAsync(PartitionCommand command, CancellationToken cancellationToken)
    {
        if (command is null)
        {
            throw new ArgumentNullException(nameof(command));
        }

        var boundaryValues = ParseBoundaries(command.Payload);
        if (boundaryValues.Count == 0)
        {
            logger.LogWarning("拆分命令 {CommandId} 未提供任何边界值。", command.Id);
            return SqlExecutionResult.Failure("拆分命令缺少边界值。", 0, null);
        }

        var configuration = await metadataRepository.GetConfigurationAsync(
            command.DataSourceId,
            command.SchemaName,
            command.TableName,
            cancellationToken);

        if (configuration is null)
        {
            var message = $"未找到 {command.SchemaName}.{command.TableName} 的分区配置，无法执行拆分。";
            logger.LogWarning("拆分命令 {CommandId} 执行失败: {Message}", command.Id, message);
            return SqlExecutionResult.Failure(message, 0, null);
        }

        PartitionIndexInspection indexInspection;
        try
        {
            indexInspection = await metadataRepository.GetIndexInspectionAsync(
                command.DataSourceId,
                command.SchemaName,
                command.TableName,
                configuration.PartitionColumn.Name,
                cancellationToken);
        }
        catch (Exception ex)
        {
            var message = $"索引检查失败：{ex.Message}";
            logger.LogError(ex, "拆分命令 {CommandId} 索引检查失败。", command.Id);
            return SqlExecutionResult.Failure(message, 0, ex.ToString());
        }

        List<PartitionValue> partitions;
        try
        {
            partitions = boundaryValues
                .Select(value => PartitionValue.FromInvariantString(configuration.PartitionColumn.ValueKind, value))
                .ToList();
        }
        catch (Exception ex)
        {
            var message = $"解析分区边界失败：{ex.Message}";
            logger.LogError(ex, "拆分命令 {CommandId} 边界解析失败。", command.Id);
            return SqlExecutionResult.Failure(message, 0, ex.ToString());
        }

        logger.LogInformation(
            "开始执行拆分命令 {CommandId}，目标表 {Schema}.{Table}，新增边界数 {Count}。",
            command.Id,
            command.SchemaName,
            command.TableName,
            partitions.Count);

        return await sqlExecutor.ExecuteSplitWithTransactionAsync(
            command.DataSourceId,
            configuration,
            partitions,
            indexInspection,
            cancellationToken);
    }

    private static IReadOnlyList<string> ParseBoundaries(string payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
        {
            return Array.Empty<string>();
        }

        try
        {
            using var document = JsonDocument.Parse(payload);
            if (!document.RootElement.TryGetProperty("Boundaries", out var boundariesElement) ||
                boundariesElement.ValueKind != JsonValueKind.Array)
            {
                return Array.Empty<string>();
            }

            var results = new List<string>();
            foreach (var element in boundariesElement.EnumerateArray())
            {
                if (element.ValueKind == JsonValueKind.String)
                {
                    var value = element.GetString();
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        results.Add(value);
                    }
                }
            }

            return results;
        }
        catch (JsonException)
        {
            return Array.Empty<string>();
        }
    }
}
