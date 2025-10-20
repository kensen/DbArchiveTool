using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
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
    private readonly ILogger<PartitionSwitchAppService> logger;

    public PartitionSwitchAppService(
        IPartitionConfigurationRepository configurationRepository,
        IPartitionSwitchInspectionService inspectionService,
        IPartitionCommandAppService commandAppService,
        IPartitionAuditLogRepository auditLogRepository,
        ILogger<PartitionSwitchAppService> logger)
    {
        this.configurationRepository = configurationRepository;
        this.inspectionService = inspectionService;
        this.commandAppService = commandAppService;
        this.auditLogRepository = auditLogRepository;
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

        var target = NormalizeTargetTable(request.TargetTable, configuration.SchemaName);
        if (!target.IsValid)
        {
            return Result<PartitionSwitchInspectionResultDto>.Failure(target.Error!);
        }

        var context = new PartitionSwitchInspectionContext(
            request.SourcePartitionKey,
            target.Schema,
            target.Table,
            request.CreateStagingTable);

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

        var target = NormalizeTargetTable(request.TargetTable, configuration.SchemaName);
        if (!target.IsValid)
        {
            return Result<Guid>.Failure(target.Error!);
        }

        var inspectionContext = new PartitionSwitchInspectionContext(
            request.SourcePartitionKey,
            target.Schema,
            target.Table,
            request.CreateStagingTable);

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

        if (string.IsNullOrWhiteSpace(request.RequestedBy))
        {
            return Result.Failure("操作人不能为空。");
        }

        return Result.Success();
    }

    private static Result ValidateExecuteRequest(SwitchPartitionExecuteRequest request)
    {
        var inspectionValidation = ValidateInspectionRequest(new SwitchPartitionInspectionRequest(
            request.PartitionConfigurationId,
            request.SourcePartitionKey,
            request.TargetTable,
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
            MapTable(result.SourceTable),
            MapTable(result.TargetTable));
    }

    private static PartitionSwitchIssueDto MapIssue(PartitionSwitchIssue issue)
        => new(issue.Code, issue.Message, issue.Recommendation);

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

    private static NormalizedTarget NormalizeTargetTable(string raw, string defaultSchema)
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

    private static string FormatQualifiedName(string schema, string table)
        => $"[{schema}].[{table}]";

    private sealed record NormalizedTarget(bool IsValid, string Schema, string Table, string? Error)
    {
        public static NormalizedTarget Valid(string schema, string table)
            => new(true, schema, table, null);

        public static NormalizedTarget Invalid(string error)
            => new(false, string.Empty, string.Empty, error);
    }
}
