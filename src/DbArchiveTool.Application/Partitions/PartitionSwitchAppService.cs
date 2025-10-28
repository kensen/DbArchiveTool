using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区切换应用服务，负责调用检查服务并提交切换命令。
/// </summary>
internal sealed class PartitionSwitchAppService : IPartitionSwitchAppService
{
    private readonly IPartitionConfigurationRepository configurationRepository;
    private readonly IPartitionSwitchInspectionService inspectionService;
    private readonly IPartitionCommandAppService commandAppService;
    private readonly IPartitionAuditLogRepository auditLogRepository;
    private readonly IDataSourceRepository dataSourceRepository;
    private readonly IPartitionSwitchAutoFixExecutor autoFixExecutor;
    private readonly ILogger<PartitionSwitchAppService> logger;

    public PartitionSwitchAppService(
        IPartitionConfigurationRepository configurationRepository,
        IPartitionSwitchInspectionService inspectionService,
        IPartitionCommandAppService commandAppService,
        IPartitionAuditLogRepository auditLogRepository,
        IDataSourceRepository dataSourceRepository,
        IPartitionSwitchAutoFixExecutor autoFixExecutor,
        ILogger<PartitionSwitchAppService> logger)
    {
        this.configurationRepository = configurationRepository;
        this.inspectionService = inspectionService;
        this.commandAppService = commandAppService;
        this.auditLogRepository = auditLogRepository;
        this.dataSourceRepository = dataSourceRepository;
        this.autoFixExecutor = autoFixExecutor;
        this.logger = logger;
    }

    public async Task<Result<PartitionSwitchInspectionResultDto>> InspectAsync(SwitchPartitionInspectionRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateInspectionRequest(request);
        if (!validation.IsSuccess)
        {
            return Result<PartitionSwitchInspectionResultDto>.Failure(validation.Error!);
        }

        var configuration = await configurationRepository.GetByIdAsync(request.PartitionConfigurationId, cancellationToken);
        if (configuration is null)
        {
            return Result<PartitionSwitchInspectionResultDto>.Failure("未找到指定的分区配置。");
        }

        var dataSource = await dataSourceRepository.GetAsync(configuration.ArchiveDataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return Result<PartitionSwitchInspectionResultDto>.Failure("未找到归档数据源配置，请检查数据源列表。");
        }

        var target = NormalizeTargetTable(
            request.TargetTable,
            configuration.SchemaName,
            request.TargetDatabase,
            ResolveDefaultTargetDatabase(dataSource));
        if (!target.IsValid)
        {
            return Result<PartitionSwitchInspectionResultDto>.Failure(target.Error!);
        }

        var targetDatabaseValidation = EnsureTargetDatabaseAllowed(dataSource, target.Database);
        if (!targetDatabaseValidation.IsSuccess)
        {
            return Result<PartitionSwitchInspectionResultDto>.Failure(targetDatabaseValidation.Error!);
        }

        var context = new PartitionSwitchInspectionContext(
            request.SourcePartitionKey,
            target.Schema,
            target.Table,
            request.CreateStagingTable,
            target.Database,
            dataSource.DatabaseName,
            dataSource.UseSourceAsTarget);

        var inspection = await inspectionService.InspectAsync(
            configuration.ArchiveDataSourceId,
            configuration,
            context,
            cancellationToken);

        var dto = MapToDto(inspection);
        return Result<PartitionSwitchInspectionResultDto>.Success(dto);
    }

    public async Task<Result<Guid>> ArchiveBySwitchAsync(SwitchPartitionExecuteRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateExecuteRequest(request);
        if (!validation.IsSuccess)
        {
            return Result<Guid>.Failure(validation.Error!);
        }

        var configuration = await configurationRepository.GetByIdAsync(request.PartitionConfigurationId, cancellationToken);
        if (configuration is null)
        {
            return Result<Guid>.Failure("未找到指定的分区配置。");
        }

        var dataSource = await dataSourceRepository.GetAsync(configuration.ArchiveDataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return Result<Guid>.Failure("未找到归档数据源配置，请检查数据源列表。");
        }

        var target = NormalizeTargetTable(
            request.TargetTable,
            configuration.SchemaName,
            request.TargetDatabase,
            ResolveDefaultTargetDatabase(dataSource));
        if (!target.IsValid)
        {
            return Result<Guid>.Failure(target.Error!);
        }

        var targetDatabaseValidation = EnsureTargetDatabaseAllowed(dataSource, target.Database);
        if (!targetDatabaseValidation.IsSuccess)
        {
            return Result<Guid>.Failure(targetDatabaseValidation.Error!);
        }

        var inspectionContext = new PartitionSwitchInspectionContext(
            request.SourcePartitionKey,
            target.Schema,
            target.Table,
            request.CreateStagingTable,
            target.Database,
            dataSource.DatabaseName,
            dataSource.UseSourceAsTarget);

        var inspection = await inspectionService.InspectAsync(
            configuration.ArchiveDataSourceId,
            configuration,
            inspectionContext,
            cancellationToken);

        if (!inspection.CanSwitch)
        {
            var reason = inspection.BlockingIssues.FirstOrDefault()?.Message ?? "分区切换检查未通过。";
            return Result<Guid>.Failure(reason);
        }

        var previewRequest = new SwitchPartitionRequest(
            configuration.ArchiveDataSourceId,
            configuration.SchemaName,
            configuration.TableName,
            request.SourcePartitionKey,
            FormatQualifiedName(target.Schema, target.Table),
            target.Database,
            request.CreateStagingTable,
            request.BackupConfirmed,
            request.RequestedBy);

        var preview = await commandAppService.PreviewSwitchAsync(previewRequest, cancellationToken);
        if (!preview.IsSuccess)
        {
            return Result<Guid>.Failure(preview.Error!);
        }

        var executeResult = await commandAppService.ExecuteSwitchAsync(previewRequest, cancellationToken);
        if (!executeResult.IsSuccess)
        {
            return executeResult;
        }

        await WriteAuditLogAsync(
            configuration,
            request,
            target,
            preview.Value!.Script,
            executeResult.Value,
            cancellationToken);

        logger.LogInformation("Switch command {CommandId} created for configuration {ConfigurationId}", executeResult.Value, configuration.Id);
        return executeResult;
    }

    private async Task WriteAuditLogAsync(
        PartitionConfiguration configuration,
        SwitchPartitionExecuteRequest request,
        NormalizedTarget target,
        string script,
        Guid commandId,
        CancellationToken cancellationToken)
    {
        var payload = new
        {
            configurationId = configuration.Id,
            configuration.SchemaName,
            configuration.TableName,
            request.SourcePartitionKey,
            target.Schema,
            target.Table,
            target.Database,
            request.CreateStagingTable,
            request.BackupConfirmed,
            commandId
        };

        var audit = PartitionAuditLog.Create(
            request.RequestedBy,
            BackgroundTaskOperationType.ArchiveSwitch.ToString(),
            nameof(PartitionConfiguration),
            configuration.Id.ToString(),
            "提交分区切换任务",
            JsonSerializer.Serialize(payload),
            "Success",
            script);

        await auditLogRepository.AddAsync(audit, cancellationToken);
    }

    /// <summary>
    /// 执行用户勾选的自动补齐步骤，返回执行明细。
    /// </summary>
    public async Task<Result<PartitionSwitchAutoFixResultDto>> AutoFixAsync(SwitchPartitionAutoFixRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateAutoFixRequest(request);
        if (!validation.IsSuccess)
        {
            return Result<PartitionSwitchAutoFixResultDto>.Failure(validation.Error!);
        }

        var configuration = await configurationRepository.GetByIdAsync(request.PartitionConfigurationId, cancellationToken);
        if (configuration is null)
        {
            return Result<PartitionSwitchAutoFixResultDto>.Failure("未找到指定的分区配置。");
        }

        var dataSource = await dataSourceRepository.GetAsync(configuration.ArchiveDataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return Result<PartitionSwitchAutoFixResultDto>.Failure("未找到归档数据源配置，请检查数据源列表。");
        }

        var target = NormalizeTargetTable(
            request.TargetTable,
            configuration.SchemaName,
            request.TargetDatabase,
            ResolveDefaultTargetDatabase(dataSource));
        if (!target.IsValid)
        {
            return Result<PartitionSwitchAutoFixResultDto>.Failure(target.Error!);
        }

        var targetDatabaseValidation = EnsureTargetDatabaseAllowed(dataSource, target.Database);
        if (!targetDatabaseValidation.IsSuccess)
        {
            return Result<PartitionSwitchAutoFixResultDto>.Failure(targetDatabaseValidation.Error!);
        }

        var context = new PartitionSwitchInspectionContext(
            request.SourcePartitionKey,
            target.Schema,
            target.Table,
            request.CreateStagingTable,
            target.Database,
            dataSource.DatabaseName,
            dataSource.UseSourceAsTarget);

        // 重新执行检查，确保最新结构状态并获取补齐计划
        var inspection = await inspectionService.InspectAsync(
            configuration.ArchiveDataSourceId,
            configuration,
            context,
            cancellationToken);

        if (inspection.BlockingIssues.Count > 0)
        {
            var reason = inspection.BlockingIssues.First().Message;
            return Result<PartitionSwitchAutoFixResultDto>.Failure($"存在阻塞项无法自动补齐：{reason}");
        }

        var available = inspection.Plan.AutoFixes
            .ToDictionary(step => step.Code, step => step, StringComparer.OrdinalIgnoreCase);

        var selected = new List<PartitionSwitchPlanAutoFix>(request.AutoFixStepCodes.Count);
        foreach (var code in request.AutoFixStepCodes)
        {
            if (!available.TryGetValue(code, out var step))
            {
                return Result<PartitionSwitchAutoFixResultDto>.Failure($"未在预检结果中找到自动补齐步骤：{code}。");
            }

            selected.Add(step);
        }

        var autoFixResult = await autoFixExecutor.ExecuteAsync(
            configuration.ArchiveDataSourceId,
            configuration,
            context,
            selected,
            cancellationToken);

        var dto = MapAutoFixResult(autoFixResult);
        return Result<PartitionSwitchAutoFixResultDto>.Success(dto);
    }

    private static Result ValidateInspectionRequest(SwitchPartitionInspectionRequest request)
    {
        if (request.PartitionConfigurationId == Guid.Empty)
        {
            return Result.Failure("配置标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.SourcePartitionKey))
        {
            return Result.Failure("源分区编号不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.TargetTable))
        {
            return Result.Failure("目标表不能为空。");
        }

        if (request.TargetDatabase is not null && string.IsNullOrWhiteSpace(request.TargetDatabase))
        {
            return Result.Failure("目标数据库名称格式不正确。");
        }

        if (string.IsNullOrWhiteSpace(request.RequestedBy))
        {
            return Result.Failure("操作人不能为空。");
        }

        return Result.Success();
    }

    private static Result ValidateAutoFixRequest(SwitchPartitionAutoFixRequest request)
    {
        if (request.PartitionConfigurationId == Guid.Empty)
        {
            return Result.Failure("配置标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.SourcePartitionKey))
        {
            return Result.Failure("源分区编号不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.TargetTable))
        {
            return Result.Failure("目标表不能为空。");
        }

        if (request.TargetDatabase is not null && string.IsNullOrWhiteSpace(request.TargetDatabase))
        {
            return Result.Failure("目标数据库名称格式不正确。");
        }

        if (string.IsNullOrWhiteSpace(request.RequestedBy))
        {
            return Result.Failure("操作人不能为空。");
        }

        if (request.AutoFixStepCodes is null || request.AutoFixStepCodes.Count == 0)
        {
            return Result.Failure("请至少选择一个自动补齐步骤。");
        }

        return Result.Success();
    }

    private static Result ValidateExecuteRequest(SwitchPartitionExecuteRequest request)
    {
        var inspectionValidation = ValidateInspectionRequest(new SwitchPartitionInspectionRequest(
            request.PartitionConfigurationId,
            request.SourcePartitionKey,
            request.TargetTable,
            request.TargetDatabase,
            request.CreateStagingTable,
            request.RequestedBy));
        if (!inspectionValidation.IsSuccess)
        {
            return inspectionValidation;
        }

        if (!request.BackupConfirmed)
        {
            return Result.Failure("执行前请确认已完成最新备份。");
        }

        return Result.Success();
    }

    private static PartitionSwitchInspectionResultDto MapToDto(PartitionSwitchInspectionResult result)
    {
        return new PartitionSwitchInspectionResultDto(
            result.CanSwitch,
            result.BlockingIssues.Select(MapIssue).ToList(),
            result.Warnings.Select(MapIssue).ToList(),
            result.AutoFixSteps.Select(MapAutoFix).ToList(),
            MapTable(result.SourceTable),
            MapTable(result.TargetTable),
            MapPlan(result.Plan));
    }

    private static PartitionSwitchIssueDto MapIssue(PartitionSwitchIssue issue)
        => new(issue.Code, issue.Message, issue.Recommendation);

    private static PartitionSwitchAutoFixStepDto MapAutoFix(PartitionSwitchAutoFixStep step)
        => new(step.Code, step.Description, step.Recommendation);

    private static PartitionSwitchTableInfoDto MapTable(PartitionSwitchTableInfo table)
        => new(
            table.SchemaName,
            table.TableName,
            table.RowCount,
            table.Columns.Select(MapColumn).ToList());

    private static PartitionSwitchColumnDto MapColumn(PartitionSwitchColumnInfo column)
        => new(
            column.Name,
            column.DataType,
            column.MaxLength,
            column.Precision,
            column.Scale,
            column.IsNullable,
            column.IsIdentity,
            column.IsComputed);

    private static PartitionSwitchAutoFixResultDto MapAutoFixResult(PartitionSwitchAutoFixResult result)
        => new(
            result.Succeeded,
            result.Executions.Select(MapAutoFixExecution).ToList(),
            result.CombinedLog);

    private static PartitionSwitchAutoFixExecutionDto MapAutoFixExecution(PartitionSwitchAutoFixExecution execution)
        => new(
            execution.Code,
            execution.Succeeded,
            execution.Message,
            execution.Script,
            execution.ElapsedMilliseconds);

    // 将领域层的补齐计划映射为前端可直接消费的 DTO
    private static PartitionSwitchPlanDto MapPlan(PartitionSwitchPlan plan)
        => new(
            plan.Blockers.Select(MapPlanBlocker).ToList(),
            plan.AutoFixes.Select(MapPlanAutoFix).ToList(),
            plan.Warnings.Select(MapPlanWarning).ToList());

    // 映射需要人工处理的阻塞项
    private static PartitionSwitchPlanBlockerDto MapPlanBlocker(PartitionSwitchPlanBlocker blocker)
        => new(blocker.Code, blocker.Title, blocker.Description, blocker.ResolutionSuggestion);

    // 映射单个自动补齐步骤，保留分类和执行命令
    private static PartitionSwitchPlanAutoFixDto MapPlanAutoFix(PartitionSwitchPlanAutoFix autoFix)
        => new(
            autoFix.Code,
            autoFix.Title,
            autoFix.Category.ToString(),
            autoFix.ImpactScope,
            autoFix.Commands.Select(MapPlanCommand).ToList(),
            autoFix.Prerequisite,
            autoFix.RequiresExclusiveLock);

    // 映射自动补齐命令明细
    private static PartitionSwitchPlanCommandDto MapPlanCommand(PartitionSwitchPlanCommand command)
        => new(command.CommandText, command.Description);

    // 映射风险提示，方便前端展示与重点提示
    private static PartitionSwitchPlanWarningDto MapPlanWarning(PartitionSwitchPlanWarning warning)
        => new(warning.Code, warning.Title, warning.Description, warning.Guidance);

    private static NormalizedTarget NormalizeTargetTable(string raw, string defaultSchema, string? explicitDatabase, string defaultDatabase)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return NormalizedTarget.Invalid("目标表不能为空。");
        }

        var trimmed = raw.Trim();
        var segments = SplitQualifiedName(trimmed);

        string database = defaultDatabase;
        string schema = defaultSchema;
        string table = string.Empty;

        switch (segments.Length)
        {
            case 3:
                database = segments[0];
                schema = segments[1];
                table = segments[2];
                break;
            case 2:
                schema = segments[0];
                table = segments[1];
                break;
            case 1:
                table = segments[0];
                break;
            default:
                return NormalizedTarget.Invalid("目标表名称解析失败，请使用 [database.][schema.]table 格式。");
        }

        if (!string.IsNullOrWhiteSpace(explicitDatabase))
        {
            database = explicitDatabase.Trim('[', ']');
        }

        if (string.IsNullOrWhiteSpace(database))
        {
            database = defaultDatabase;
        }

        if (string.IsNullOrWhiteSpace(schema))
        {
            schema = defaultSchema;
        }

        if (string.IsNullOrWhiteSpace(table))
        {
            return NormalizedTarget.Invalid("目标表名称解析失败，请检查输入格式。");
        }

        return NormalizedTarget.Valid(database.Trim(), schema.Trim(), table.Trim());
    }

    private static string[] SplitQualifiedName(string input)
    {
        input = input.Trim();
        if (input.Contains("].[", StringComparison.Ordinal))
        {
            return input.Split(new[] { "].[" }, StringSplitOptions.RemoveEmptyEntries)
                .Select(part => part.Trim('[', ']')).ToArray();
        }

        return input.Split('.', StringSplitOptions.RemoveEmptyEntries)
            .Select(part => part.Trim('[', ']'))
            .ToArray();
    }

    private static string FormatQualifiedName(string schema, string table)
        => $"[{schema}].[{table}]";

    private static string ResolveDefaultTargetDatabase(ArchiveDataSource dataSource)
    {
        if (dataSource.UseSourceAsTarget)
        {
            return dataSource.DatabaseName;
        }

        return string.IsNullOrWhiteSpace(dataSource.TargetDatabaseName)
            ? dataSource.DatabaseName
            : dataSource.TargetDatabaseName!;
    }

    private static Result EnsureTargetDatabaseAllowed(ArchiveDataSource dataSource, string targetDatabase)
    {
        if (dataSource.UseSourceAsTarget)
        {
            if (!string.Equals(targetDatabase, dataSource.DatabaseName, StringComparison.OrdinalIgnoreCase))
            {
                return Result.Failure($"当前数据源仅允许在源数据库 {dataSource.DatabaseName} 内进行分区切换。");
            }

            return Result.Success();
        }

        var allowedDatabase = string.IsNullOrWhiteSpace(dataSource.TargetDatabaseName)
            ? dataSource.DatabaseName
            : dataSource.TargetDatabaseName!;

        if (!string.Equals(targetDatabase, allowedDatabase, StringComparison.OrdinalIgnoreCase))
        {
            return Result.Failure($"目标数据库必须为 {allowedDatabase}，请检查归档目标配置。");
        }

        return Result.Success();
    }

    private sealed record NormalizedTarget(bool IsValid, string Schema, string Table, string Database, string? Error)
    {
        public static NormalizedTarget Valid(string database, string schema, string table)
            => new(true, schema, table, database, null);

        public static NormalizedTarget Invalid(string error)
            => new(false, string.Empty, string.Empty, string.Empty, error);
    }
}
