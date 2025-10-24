using System.Globalization;
using System.Linq;
using System.Text.Json;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区命令应用服务，负责验证输入、生成脚本并持久化命令。
/// </summary>
internal sealed class PartitionCommandAppService : IPartitionCommandAppService
{
    private readonly IPartitionMetadataRepository metadataRepository;
    private readonly IPartitionCommandRepository commandRepository;
    private readonly IPartitionCommandScriptGenerator scriptGenerator;
    private readonly IPartitionCommandQueue commandQueue;
    private readonly PartitionValueParser parser;
    private readonly ILogger<PartitionCommandAppService> logger;
    private readonly IBackgroundTaskRepository taskRepository;
    private readonly IBackgroundTaskLogRepository logRepository;
    private readonly IPartitionAuditLogRepository auditLogRepository;
    private readonly IBackgroundTaskDispatcher dispatcher;

    public PartitionCommandAppService(
        IPartitionMetadataRepository metadataRepository,
        IPartitionCommandRepository commandRepository,
        IPartitionCommandScriptGenerator scriptGenerator,
        IPartitionCommandQueue commandQueue,
        PartitionValueParser parser,
        ILogger<PartitionCommandAppService> logger,
        IBackgroundTaskRepository taskRepository,
        IBackgroundTaskLogRepository logRepository,
        IPartitionAuditLogRepository auditLogRepository,
        IBackgroundTaskDispatcher dispatcher)
    {
        this.metadataRepository = metadataRepository;
        this.commandRepository = commandRepository;
        this.scriptGenerator = scriptGenerator;
        this.commandQueue = commandQueue;
        this.parser = parser;
        this.logger = logger;
        this.taskRepository = taskRepository;
        this.logRepository = logRepository;
        this.auditLogRepository = auditLogRepository;
        this.dispatcher = dispatcher;
    }

    /// <inheritdoc />
    public async Task<Result<PartitionCommandPreviewDto>> PreviewSplitAsync(SplitPartitionRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateRequest(request);
        if (!validation.IsSuccess)
        {
            return Result<PartitionCommandPreviewDto>.Failure(validation.Error!);
        }

        var configuration = await metadataRepository.GetConfigurationAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (configuration is null)
        {
            return Result<PartitionCommandPreviewDto>.Failure("未找到分区配置，请确认目标表启用了分区。");
        }

        var parseResult = parser.ParseValues(configuration.PartitionColumn, request.Boundaries);
        if (!parseResult.IsSuccess)
        {
            return Result<PartitionCommandPreviewDto>.Failure(parseResult.Error!);
        }

        var scriptResult = scriptGenerator.GenerateSplitScript(
            configuration, 
            parseResult.Value!, 
            request.FilegroupName);  // 传递用户选择的文件组
        if (!scriptResult.IsSuccess)
        {
            return Result<PartitionCommandPreviewDto>.Failure(scriptResult.Error!);
        }

        var warnings = BuildSplitWarnings(request);
        var dto = new PartitionCommandPreviewDto(scriptResult.Value!, warnings);
        return Result<PartitionCommandPreviewDto>.Success(dto);
    }

    /// <inheritdoc />
    public async Task<Result<Guid>> ExecuteSplitAsync(SplitPartitionRequest request, CancellationToken cancellationToken = default)
    {
        if (!request.BackupConfirmed)
        {
            return Result<Guid>.Failure("执行拆分前需要确认已有备份或快照。");
        }

        var preview = await PreviewSplitAsync(request, cancellationToken);
        if (!preview.IsSuccess)
        {
            return Result<Guid>.Failure(preview.Error!);
        }

        var configuration = await metadataRepository.GetConfigurationAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (configuration is null)
        {
            return Result<Guid>.Failure("未找到分区配置，请确认目标表启用了分区。");
        }

        var values = parser.ParseValues(configuration.PartitionColumn, request.Boundaries);
        if (!values.IsSuccess)
        {
            return Result<Guid>.Failure(values.Error!);
        }

        var script = preview.Value!.Script;
        var boundaryValues = values.Value!.Select(v => v.ToInvariantString()).ToArray();
        
        // 准备任务上下文
        var resourceId = $"{request.DataSourceId}/{request.SchemaName}/{request.TableName}";
        var boundariesDisplay = boundaryValues.Length == 1 
            ? $"'{boundaryValues[0]}'" 
            : $"{boundaryValues.Length} 个边界值 ({string.Join(", ", boundaryValues.Take(3))}{(boundaryValues.Length > 3 ? "..." : "")})";
        var summary = $"拆分表 {request.SchemaName}.{request.TableName} 的分区边界: {boundariesDisplay}";
        
        var payload = JsonSerializer.Serialize(new
        {
            request.SchemaName,
            request.TableName,
            configuration.PartitionFunctionName,
            configuration.PartitionSchemeName,
            Boundaries = boundaryValues,
            DdlScript = script,
            request.BackupConfirmed,
            request.FilegroupName  // 保存用户选择的文件组
        });

        try
        {
            // 创建临时的分区配置ID（用于关联任务）
            var tempConfigurationId = Guid.NewGuid();

            // 创建执行任务 (使用 SplitBoundary 操作类型)
            var task = BackgroundTask.Create(
                partitionConfigurationId: tempConfigurationId,
                dataSourceId: request.DataSourceId,
                requestedBy: request.RequestedBy,
                createdBy: request.RequestedBy,
                backupReference: null,
                notes: $"批量拆分 {boundaryValues.Length} 个边界值",
                priority: 0,
                operationType: Shared.Partitions.BackgroundTaskOperationType.SplitBoundary,
                archiveScheme: null,
                archiveTargetConnection: null,
                archiveTargetDatabase: null,
                archiveTargetTable: null);

            // 保存配置快照
            task.SaveConfigurationSnapshot(payload, request.RequestedBy);

            await taskRepository.AddAsync(task, cancellationToken);

            // 记录初始日志
            var initialLog = BackgroundTaskLogEntry.Create(
                task.Id,
                "Info",
                "任务创建",
                $"由 {request.RequestedBy} 发起的分区拆分任务已创建。" +
                $"表：{request.SchemaName}.{request.TableName}，边界值数量：{boundaryValues.Length}");

            await logRepository.AddAsync(initialLog, cancellationToken);

            // 记录DDL脚本到日志
            var scriptLog = BackgroundTaskLogEntry.Create(
                task.Id,
                "Info",
                "生成DDL脚本",
                $"已生成 ALTER PARTITION FUNCTION 脚本，长度: {script.Length} 字符，包含 {boundaryValues.Length} 个 SPLIT 操作");

            await logRepository.AddAsync(scriptLog, cancellationToken);

            // 记录审计日志
            var auditLog = PartitionAuditLog.Create(
                request.RequestedBy,
                Shared.Partitions.BackgroundTaskOperationType.SplitBoundary.ToString(),
                "PartitionedTable",
                resourceId,
                summary,
                payload,
                "Queued",
                script);

            await auditLogRepository.AddAsync(auditLog, cancellationToken);

            // 将任务分派到执行队列
            await dispatcher.DispatchAsync(task.Id, request.DataSourceId, cancellationToken);

            logger.LogInformation(
                "Successfully created execution task {TaskId} for splitting boundaries in table {Schema}.{Table}, boundary count: {Count}",
                task.Id,
                request.SchemaName,
                request.TableName,
                boundaryValues.Length);

            return Result<Guid>.Success(task.Id);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create split task for table {Schema}.{Table}", request.SchemaName, request.TableName);
            return Result<Guid>.Failure("创建拆分任务时发生异常,请稍后重试。");
        }
    }

    public async Task<Result> ApproveAsync(Guid commandId, string approver, CancellationToken cancellationToken = default)
    {
        if (commandId == Guid.Empty)
        {
            return Result.Failure("命令标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(approver))
        {
            return Result.Failure("审批人不能为空。");
        }

        var command = await commandRepository.GetByIdAsync(commandId, cancellationToken);
        if (command is null)
        {
            return Result.Failure("未找到指定的分区命令。");
        }

        try
        {
            command.Approve(approver);
            await commandRepository.UpdateAsync(command, cancellationToken);

            await commandQueue.EnqueueAsync(command.Id, cancellationToken);

            logger.LogInformation(
                "Partition command {CommandId} approved by {Approver} and enqueued for execution",
                commandId,
                approver);
            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to approve partition command {CommandId}", commandId);
            return Result.Failure("PARTITION_COMMAND_APPROVE_FAILED");
        }
    }

    public async Task<Result> RejectAsync(Guid commandId, string approver, string reason, CancellationToken cancellationToken = default)
    {
        if (commandId == Guid.Empty)
        {
            return Result.Failure("命令标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(approver))
        {
            return Result.Failure("审批人不能为空。");
        }

        if (string.IsNullOrWhiteSpace(reason))
        {
            return Result.Failure("拒绝原因不能为空。");
        }

        var command = await commandRepository.GetByIdAsync(commandId, cancellationToken);
        if (command is null)
        {
            return Result.Failure("未找到指定的分区命令。");
        }

        try
        {
            command.Reject(approver, reason);
            await commandRepository.UpdateAsync(command, cancellationToken);
            logger.LogInformation("Partition command {CommandId} rejected by {Approver}", commandId, approver);
            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to reject partition command {CommandId}", commandId);
            return Result.Failure("PARTITION_COMMAND_REJECT_FAILED");
        }
    }

    private static Result ValidateRequest(SplitPartitionRequest request)
    {
        if (request.DataSourceId == Guid.Empty)
        {
            return Result.Failure("数据源标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.SchemaName) || string.IsNullOrWhiteSpace(request.TableName))
        {
            return Result.Failure("架构名称与表名称不能为空。");
        }

        if (request.Boundaries.Count == 0)
        {
            return Result.Failure("请至少提供一个分区边界。");
        }

        if (string.IsNullOrWhiteSpace(request.RequestedBy))
        {
            return Result.Failure("请求人不能为空。");
        }

        return Result.Success();
    }

    private static IReadOnlyList<string> BuildSplitWarnings(SplitPartitionRequest request)
    {
        var warnings = new List<string>();
        if (!request.BackupConfirmed)
        {
            warnings.Add("尚未确认备份，执行前请确保已有最新备份。");
        }

        if (request.Boundaries.Count > 1)
        {
            warnings.Add("一次拆分多个边界，建议逐个执行以降低风险。");
        }

        return warnings;
    }

    private static Result ValidateMergeRequest(MergePartitionRequest request)
    {
        if (request.DataSourceId == Guid.Empty)
        {
            return Result.Failure("数据源标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.SchemaName) || string.IsNullOrWhiteSpace(request.TableName))
        {
            return Result.Failure("架构名称与表名称不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.BoundaryKey))
        {
            return Result.Failure("请指定需要合并的分区边界。");
        }

        if (string.IsNullOrWhiteSpace(request.RequestedBy))
        {
            return Result.Failure("请求人不能为空。");
        }

        return Result.Success();
    }

    private static IReadOnlyList<string> BuildMergeWarnings(MergePartitionRequest request, PartitionBoundary boundary)
    {
        var warnings = new List<string>();
        if (!request.BackupConfirmed)
        {
            warnings.Add("尚未确认备份，执行前请确保已有最新备份。");
        }

        warnings.Add($"合并边界 {boundary.SortKey} ({boundary.Value.ToLiteral()}) 将移除一个分区，请确保该分区已清空。");
        return warnings;
    }

    private static Result ValidateSwitchRequest(SwitchPartitionRequest request)
    {
        if (request.DataSourceId == Guid.Empty)
        {
            return Result.Failure("数据源标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.SchemaName) || string.IsNullOrWhiteSpace(request.TableName))
        {
            return Result.Failure("架构名称与表名称不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.SourcePartitionKey))
        {
            return Result.Failure("源分区编号不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.TargetTable))
        {
            return Result.Failure("目标表不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.RequestedBy))
        {
            return Result.Failure("请求人不能为空。");
        }

        return Result.Success();
    }

    private static IReadOnlyList<string> BuildSwitchWarnings(SwitchPartitionRequest request)
    {
        var warnings = new List<string>();
        if (!request.BackupConfirmed)
        {
            warnings.Add("尚未确认备份，执行前请确保已有最新备份。");
        }

        if (request.CreateStagingTable)
        {
            warnings.Add("当前版本尚未自动创建临时表，请确保目标表已提前准备或取消临时表选项。");
        }

        return warnings;
    }

    private static string FormatQualifiedName(string schema, string table)
        => $"[{schema}].[{table}]";

    private static NormalizedTarget NormalizeTargetQualifiedName(string raw, string defaultSchema)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return NormalizedTarget.Invalid("目标表不能为空。");
        }

        var trimmed = raw.Trim();
        string schema = defaultSchema;
        string table;

        if (TrySplitQualifiedName(trimmed, out var parsedSchema, out var parsedTable))
        {
            schema = parsedSchema;
            table = parsedTable;
        }
        else
        {
            table = trimmed.Trim('[', ']');
        }

        if (string.IsNullOrWhiteSpace(table))
        {
            return NormalizedTarget.Invalid("目标表名称解析失败，请使用 schema.table 格式。");
        }

        return NormalizedTarget.Valid(schema.Trim('[', ']'), table.Trim('[', ']'));
    }

    private static bool TrySplitQualifiedName(string input, out string schema, out string table)
    {
        input = input.Trim();
        schema = string.Empty;
        table = string.Empty;

        if (input.StartsWith("[", StringComparison.Ordinal) && input.Contains("].[", StringComparison.Ordinal))
        {
            var parts = input.Split(new[] { "].[" }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 2)
            {
                schema = parts[0].Trim('[', ']');
                table = parts[1].Trim('[', ']');
                return true;
            }
        }

        var tokens = input.Split('.', StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length == 2)
        {
            schema = tokens[0].Trim('[', ']');
            table = tokens[1].Trim('[', ']');
            return true;
        }

        return false;
    }

    private sealed record NormalizedTarget(bool IsValid, string Schema, string Table, string? Error)
    {
        public static NormalizedTarget Valid(string schema, string table)
            => new(true, schema, table, null);

        public static NormalizedTarget Invalid(string error)
            => new(false, string.Empty, string.Empty, error);
    }

    public async Task<Result<PartitionCommandPreviewDto>> PreviewMergeAsync(MergePartitionRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateMergeRequest(request);
        if (!validation.IsSuccess)
        {
            return Result<PartitionCommandPreviewDto>.Failure(validation.Error!);
        }

        var configuration = await metadataRepository.GetConfigurationAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (configuration is null)
        {
            return Result<PartitionCommandPreviewDto>.Failure("未找到分区配置，请确认目标表启用了分区。");
        }

        var boundary = configuration.Boundaries.FirstOrDefault(x => x.SortKey.Equals(request.BoundaryKey, StringComparison.Ordinal));
        if (boundary is null)
        {
            return Result<PartitionCommandPreviewDto>.Failure($"未找到分区边界 {request.BoundaryKey}，请刷新后重试。");
        }

        var scriptResult = scriptGenerator.GenerateMergeScript(configuration, request.BoundaryKey);
        if (!scriptResult.IsSuccess)
        {
            return Result<PartitionCommandPreviewDto>.Failure(scriptResult.Error!);
        }

        var warnings = BuildMergeWarnings(request, boundary);
        var dto = new PartitionCommandPreviewDto(scriptResult.Value!, warnings);
        return Result<PartitionCommandPreviewDto>.Success(dto);
    }

    public async Task<Result<Guid>> ExecuteMergeAsync(MergePartitionRequest request, CancellationToken cancellationToken = default)
    {
        if (!request.BackupConfirmed)
        {
            return Result<Guid>.Failure("执行合并前需要确认已有备份或快照。");
        }

        var preview = await PreviewMergeAsync(request, cancellationToken);
        if (!preview.IsSuccess)
        {
            return Result<Guid>.Failure(preview.Error!);
        }

        var configuration = await metadataRepository.GetConfigurationAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (configuration is null)
        {
            return Result<Guid>.Failure("未找到分区配置，请确认目标表启用了分区。");
        }

        var script = preview.Value!.Script;
        
        // 准备任务上下文
        var resourceId = $"{request.DataSourceId}/{request.SchemaName}/{request.TableName}";
        var summary = $"合并表 {request.SchemaName}.{request.TableName} 的分区边界: '{request.BoundaryKey}'";
        
        var payload = JsonSerializer.Serialize(new
        {
            request.SchemaName,
            request.TableName,
            configuration.PartitionFunctionName,
            configuration.PartitionSchemeName,
            request.BoundaryKey,
            DdlScript = script,
            request.BackupConfirmed
        });

        try
        {
            // 创建临时的分区配置ID（用于关联任务）
            var tempConfigurationId = Guid.NewGuid();

            // 创建执行任务 (使用 MergeBoundary 操作类型)
            var task = BackgroundTask.Create(
                partitionConfigurationId: tempConfigurationId,
                dataSourceId: request.DataSourceId,
                requestedBy: request.RequestedBy,
                createdBy: request.RequestedBy,
                backupReference: null,
                notes: $"合并边界值: {request.BoundaryKey}",
                priority: 0,
                operationType: Shared.Partitions.BackgroundTaskOperationType.MergeBoundary,
                archiveScheme: null,
                archiveTargetConnection: null,
                archiveTargetDatabase: null,
                archiveTargetTable: null);

            // 保存配置快照
            task.SaveConfigurationSnapshot(payload, request.RequestedBy);

            await taskRepository.AddAsync(task, cancellationToken);

            // 记录初始日志
            var initialLog = BackgroundTaskLogEntry.Create(
                task.Id,
                "Info",
                "任务创建",
                $"由 {request.RequestedBy} 发起的分区合并任务已创建。" +
                $"表：{request.SchemaName}.{request.TableName}，边界值：{request.BoundaryKey}");

            await logRepository.AddAsync(initialLog, cancellationToken);

            // 记录DDL脚本到日志
            var scriptLog = BackgroundTaskLogEntry.Create(
                task.Id,
                "Info",
                "生成DDL脚本",
                $"已生成 ALTER PARTITION FUNCTION MERGE RANGE 脚本，长度: {script.Length} 字符");

            await logRepository.AddAsync(scriptLog, cancellationToken);

            // 记录审计日志
            var auditLog = PartitionAuditLog.Create(
                request.RequestedBy,
                Shared.Partitions.BackgroundTaskOperationType.MergeBoundary.ToString(),
                "PartitionedTable",
                resourceId,
                summary,
                payload,
                "Queued",
                script);

            await auditLogRepository.AddAsync(auditLog, cancellationToken);

            // 将任务分派到执行队列
            await dispatcher.DispatchAsync(task.Id, request.DataSourceId, cancellationToken);

            logger.LogInformation(
                "Successfully created execution task {TaskId} for merging boundary {BoundaryKey} in table {Schema}.{Table}",
                task.Id,
                request.BoundaryKey,
                request.SchemaName,
                request.TableName);

            return Result<Guid>.Success(task.Id);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create merge task for boundary {BoundaryKey} in table {Schema}.{Table}", 
                request.BoundaryKey, request.SchemaName, request.TableName);
            return Result<Guid>.Failure("创建合并任务时发生异常,请稍后重试。");
        }
    }

    public async Task<Result<PartitionCommandPreviewDto>> PreviewSwitchAsync(SwitchPartitionRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateSwitchRequest(request);
        if (!validation.IsSuccess)
        {
            return Result<PartitionCommandPreviewDto>.Failure(validation.Error!);
        }

        var configuration = await metadataRepository.GetConfigurationAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (configuration is null)
        {
            return Result<PartitionCommandPreviewDto>.Failure("未找到分区配置，请确认目标表启用了分区。");
        }

        var target = NormalizeTargetQualifiedName(request.TargetTable, configuration.SchemaName);
        if (!target.IsValid)
        {
            return Result<PartitionCommandPreviewDto>.Failure(target.Error!);
        }

        var payload = new SwitchPayload(
            request.SourcePartitionKey,
            target.Schema,
            target.Table,
            request.CreateStagingTable,
            null,
            null,
            null);

        var scriptResult = scriptGenerator.GenerateSwitchOutScript(configuration, payload);
        if (!scriptResult.IsSuccess)
        {
            return Result<PartitionCommandPreviewDto>.Failure(scriptResult.Error!);
        }

        var warnings = BuildSwitchWarnings(request);
        var dto = new PartitionCommandPreviewDto(scriptResult.Value!, warnings);
        return Result<PartitionCommandPreviewDto>.Success(dto);
    }

    public async Task<Result<Guid>> ExecuteSwitchAsync(SwitchPartitionRequest request, CancellationToken cancellationToken = default)
    {
        if (!request.BackupConfirmed)
        {
            return Result<Guid>.Failure("执行切换前需要确认已有备份或快照。");
        }

        var preview = await PreviewSwitchAsync(request, cancellationToken);
        if (!preview.IsSuccess)
        {
            return Result<Guid>.Failure(preview.Error!);
        }

        var configuration = await metadataRepository.GetConfigurationAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (configuration is null)
        {
            return Result<Guid>.Failure("未找到分区配置，请确认目标表启用了分区。");
        }

        var target = NormalizeTargetQualifiedName(request.TargetTable, configuration.SchemaName);
        if (!target.IsValid)
        {
            return Result<Guid>.Failure(target.Error!);
        }

        var payloadJson = JsonSerializer.Serialize(new
        {
            configurationId = configuration.Id,
            request.SchemaName,
            request.TableName,
            request.SourcePartitionKey,
            targetSchema = target.Schema,
            targetTable = target.Table,
            request.CreateStagingTable
        });

        var command = PartitionCommand.CreateSwitch(
            request.DataSourceId,
            request.SchemaName,
            request.TableName,
            preview.Value!.Script,
            payloadJson,
            request.RequestedBy);

        await commandRepository.AddAsync(command, cancellationToken);
        logger.LogInformation(
            "Partition switch command {CommandId} created for {Schema}.{Table} (Target {TargetSchema}.{TargetTable})",
            command.Id,
            request.SchemaName,
            request.TableName,
            target.Schema,
            target.Table);

        return Result<Guid>.Success(command.Id);
    }

    public async Task<Result<PartitionCommandStatusDto>> GetStatusAsync(Guid commandId, CancellationToken cancellationToken = default)
    {
        if (commandId == Guid.Empty)
        {
            return Result<PartitionCommandStatusDto>.Failure("命令标识不能为空。");
        }

        var command = await commandRepository.GetByIdAsync(commandId, cancellationToken);
        if (command is null)
        {
            return Result<PartitionCommandStatusDto>.Failure("未找到指定的分区命令。");
        }

        var dto = new PartitionCommandStatusDto(
            command.Id,
            command.Status,
            command.RequestedAt,
            command.ExecutedAt,
            command.CompletedAt,
            command.FailureReason,
            command.ExecutionLog);

        return Result<PartitionCommandStatusDto>.Success(dto);
    }
}
