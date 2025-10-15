using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 执行分区切换命令的执行器。
/// </summary>
internal sealed class SwitchPartitionCommandExecutor : IPartitionCommandExecutor
{
    private readonly IPartitionConfigurationRepository configurationRepository;
    private readonly IPartitionSwitchInspectionService inspectionService;
    private readonly SqlPartitionCommandExecutor sqlExecutor;
    private readonly ILogger<SwitchPartitionCommandExecutor> logger;

    public SwitchPartitionCommandExecutor(
        IPartitionConfigurationRepository configurationRepository,
        IPartitionSwitchInspectionService inspectionService,
        SqlPartitionCommandExecutor sqlExecutor,
        ILogger<SwitchPartitionCommandExecutor> logger)
    {
        this.configurationRepository = configurationRepository;
        this.inspectionService = inspectionService;
        this.sqlExecutor = sqlExecutor;
        this.logger = logger;
    }

    public PartitionCommandType CommandType => PartitionCommandType.Switch;

    public async Task<SqlExecutionResult> ExecuteAsync(PartitionCommand command, CancellationToken cancellationToken)
    {
        if (command is null)
        {
            throw new ArgumentNullException(nameof(command));
        }

        var payload = DeserializePayload(command.Payload);
        if (payload.ConfigurationId == Guid.Empty)
        {
            return SqlExecutionResult.Failure("命令缺少配置标识。", 0, command.Payload);
        }

        var configuration = await configurationRepository.GetByIdAsync(payload.ConfigurationId, cancellationToken);
        if (configuration is null)
        {
            return SqlExecutionResult.Failure("未找到对应的分区配置，无法执行切换。", 0, null);
        }

        var inspectionContext = new PartitionSwitchInspectionContext(
            payload.SourcePartitionKey,
            payload.TargetSchema,
            payload.TargetTable,
            payload.CreateStagingTable);

        var inspection = await inspectionService.InspectAsync(
            command.DataSourceId,
            configuration,
            inspectionContext,
            cancellationToken);

        if (!inspection.CanSwitch)
        {
            var reason = inspection.BlockingIssues.Count > 0
                ? inspection.BlockingIssues[0].Message
                : "分区切换检查未通过。";

            return SqlExecutionResult.Failure(reason, 0, JsonSerializer.Serialize(inspection.BlockingIssues));
        }

        var sqlPayload = new SwitchPayload(
            payload.SourcePartitionKey,
            payload.TargetSchema,
            payload.TargetTable,
            payload.CreateStagingTable,
            payload.StagingTableName,
            payload.FilegroupName,
            payload.AdditionalProperties);

        logger.LogInformation(
            "Executing switch command {CommandId} on {Schema}.{Table} -> {TargetSchema}.{TargetTable}",
            command.Id,
            configuration.SchemaName,
            configuration.TableName,
            payload.TargetSchema,
            payload.TargetTable);

        return await sqlExecutor.ExecuteSwitchWithTransactionAsync(
            command.DataSourceId,
            configuration,
            sqlPayload,
            cancellationToken);
    }

    private static SwitchCommandPayload DeserializePayload(string json)
    {
        try
        {
            var payload = JsonSerializer.Deserialize<SwitchCommandPayload>(json);
            return payload ?? new SwitchCommandPayload();
        }
        catch (JsonException)
        {
            return new SwitchCommandPayload();
        }
    }

    private sealed record SwitchCommandPayload
    {
        public Guid ConfigurationId { get; init; }
        public string SourcePartitionKey { get; init; } = string.Empty;
        public string TargetSchema { get; init; } = string.Empty;
        public string TargetTable { get; init; } = string.Empty;
        public bool CreateStagingTable { get; init; }
        public string? StagingTableName { get; init; }
        public string? FilegroupName { get; init; }
        public IReadOnlyDictionary<string, object>? AdditionalProperties { get; init; }
    }
}
