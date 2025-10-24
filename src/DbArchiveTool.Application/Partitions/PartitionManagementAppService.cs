using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区管理应用服务,实现分区概览与安全读取。
/// </summary>
internal sealed class PartitionManagementAppService : IPartitionManagementAppService
{
    private readonly IPartitionMetadataRepository repository;
    private readonly IPartitionCommandScriptGenerator scriptGenerator;
    private readonly IPartitionAuditLogRepository auditLogRepository;
    private readonly IBackgroundTaskRepository taskRepository;
    private readonly IBackgroundTaskLogRepository logRepository;
    private readonly IBackgroundTaskDispatcher dispatcher;
    private readonly PartitionValueParser valueParser;
    private readonly ILogger<PartitionManagementAppService> logger;

    public PartitionManagementAppService(
        IPartitionMetadataRepository repository,
        IPartitionCommandScriptGenerator scriptGenerator,
        IPartitionAuditLogRepository auditLogRepository,
        IBackgroundTaskRepository taskRepository,
        IBackgroundTaskLogRepository logRepository,
        IBackgroundTaskDispatcher dispatcher,
        PartitionValueParser valueParser,
        ILogger<PartitionManagementAppService> logger)
    {
        this.repository = repository;
        this.scriptGenerator = scriptGenerator;
        this.auditLogRepository = auditLogRepository;
        this.taskRepository = taskRepository;
        this.logRepository = logRepository;
        this.dispatcher = dispatcher;
        this.valueParser = valueParser;
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

    /// <inheritdoc />
    public async Task<Result<PartitionMetadataDto>> GetPartitionMetadataAsync(PartitionMetadataRequest request, CancellationToken cancellationToken = default)
    {
        var config = await repository.GetConfigurationAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (config is null)
        {
            return Result<PartitionMetadataDto>.Failure("未找到分区配置,请确认目标表已启用分区。");
        }

        var boundaries = config.Boundaries
            .Select(b => new PartitionBoundaryItemDto(b.SortKey, b.Value.ToLiteral()))
            .ToList();

        var mappings = config.FilegroupMappings
            .Select((m, index) => new PartitionFilegroupMappingDto(index + 1, m.FilegroupName))
            .ToList();

        var dto = new PartitionMetadataDto(
            config.PartitionColumn.Name,
            config.PartitionColumn.ValueKind.ToString(),
            config.PartitionColumn.IsNullable,
            config.IsRangeRight,
            config.PartitionFunctionName,
            config.PartitionSchemeName,
            boundaries,
            mappings);

        return Result<PartitionMetadataDto>.Success(dto);
    }

    /// <inheritdoc />
    public async Task<Result> AddBoundaryToPartitionedTableAsync(AddBoundaryToPartitionedTableRequest request, CancellationToken cancellationToken = default)
    {
        // 1. 验证输入
        if (string.IsNullOrWhiteSpace(request.BoundaryValue))
        {
            return Result.Failure("边界值不能为空。");
        }

        // 2. 获取表的实际分区元数据
        var config = await repository.GetConfigurationAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (config is null)
        {
            logger.LogWarning(
                "Table {Schema}.{Table} is not partitioned or partition metadata cannot be retrieved",
                request.SchemaName,
                request.TableName);
            return Result.Failure("未找到分区配置,请确认目标表已启用分区。");
        }

        // 3. 解析并验证边界值
        var parseResult = valueParser.ParseValues(config.PartitionColumn, new[] { request.BoundaryValue });
        if (!parseResult.IsSuccess || parseResult.Value is null || parseResult.Value.Count == 0)
        {
            logger.LogWarning(
                "Invalid boundary value for table {Schema}.{Table}: {Value}",
                request.SchemaName,
                request.TableName,
                request.BoundaryValue);
            return Result.Failure($"边界值格式错误: {parseResult.Error}");
        }

        var newValue = parseResult.Value[0];
        var sortKey = newValue.ToInvariantString();

        // 4. 检查边界是否已存在
        if (config.Boundaries.Any(b => b.SortKey.Equals(sortKey, StringComparison.Ordinal)))
        {
            logger.LogInformation(
                "Boundary {Key} already exists in table {Schema}.{Table}",
                sortKey,
                request.SchemaName,
                request.TableName);
            return Result.Failure("该边界值已存在于分区函数中。");
        }

        // 5. 生成DDL脚本 - 针对单个边界值且可能指定了文件组
        string ddlScript;
        
        if (!string.IsNullOrWhiteSpace(request.FilegroupName))
        {
            // 用户指定了文件组,使用指定的文件组
            var literal = newValue.ToLiteral();
            ddlScript = $@"-- 添加分区边界值到指定文件组
BEGIN TRY
    BEGIN TRANSACTION
    ALTER PARTITION SCHEME [{config.PartitionSchemeName}] NEXT USED [{request.FilegroupName}];
    ALTER PARTITION FUNCTION [{config.PartitionFunctionName}]() SPLIT RANGE ({literal});
    COMMIT TRANSACTION
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    RAISERROR ('分区拆分失败: %s', 16, 1, @ErrorMessage);
END CATCH
";
        }
        else
        {
            // 用户未指定文件组,使用配置中的默认文件组
            var scriptResult = scriptGenerator.GenerateSplitScript(config, parseResult.Value, null);
            if (!scriptResult.IsSuccess || string.IsNullOrEmpty(scriptResult.Value))
            {
                logger.LogError(
                    "Failed to generate split script for table {Schema}.{Table}: {Error}",
                    request.SchemaName,
                    request.TableName,
                    scriptResult.Error);
                return Result.Failure($"生成DDL脚本失败: {scriptResult.Error}");
            }
            ddlScript = scriptResult.Value;
        }

        // 6. 准备任务上下文变量
        var resourceId = $"{request.DataSourceId}/{request.SchemaName}/{request.TableName}";
        var summary = $"为表 {request.SchemaName}.{request.TableName} 添加分区边界值 '{request.BoundaryValue}'";
        var payload = System.Text.Json.JsonSerializer.Serialize(new
        {
            request.SchemaName,
            request.TableName,
            config.PartitionFunctionName,
            config.PartitionSchemeName,
            request.BoundaryValue,
            request.FilegroupName,
            SortKey = sortKey,
            DdlScript = ddlScript
        });

        try
        {
            // 7. 创建临时的分区配置ID（用于关联任务）
            var tempConfigurationId = Guid.NewGuid();

            // 8. 创建执行任务
            var task = BackgroundTask.Create(
                partitionConfigurationId: tempConfigurationId,
                dataSourceId: request.DataSourceId,
                requestedBy: request.RequestedBy,
                createdBy: request.RequestedBy,
                backupReference: null,
                notes: request.Notes,
                priority: 0,
                operationType: BackgroundTaskOperationType.AddBoundary,
                archiveScheme: null,
                archiveTargetConnection: null,
                archiveTargetDatabase: null,
                archiveTargetTable: null);

            // 9. 保存配置快照
            task.SaveConfigurationSnapshot(payload, request.RequestedBy);

            await taskRepository.AddAsync(task, cancellationToken);

            // 10. 记录初始日志
            var initialLog = BackgroundTaskLogEntry.Create(
                task.Id,
                "Info",
                "任务创建",
                $"由 {request.RequestedBy} 发起的添加分区边界任务已创建。" +
                $"表：{request.SchemaName}.{request.TableName}，边界值：{request.BoundaryValue}");

            await logRepository.AddAsync(initialLog, cancellationToken);

            // 11. 记录DDL脚本到日志
            var scriptLog = BackgroundTaskLogEntry.Create(
                task.Id,
                "Info",
                "生成DDL脚本",
                $"已生成 ALTER PARTITION FUNCTION 脚本，长度: {ddlScript.Length} 字符");

            await logRepository.AddAsync(scriptLog, cancellationToken);

            // 12. 记录审计日志(包含DDL脚本)
            var auditLog = PartitionAuditLog.Create(
                request.RequestedBy,
                BackgroundTaskOperationType.AddBoundary.ToString(),
                "PartitionedTable",
                resourceId,
                summary,
                payload,
                "Queued",
                ddlScript);

            await auditLogRepository.AddAsync(auditLog, cancellationToken);

            // 13. 将任务分派到执行队列
            await dispatcher.DispatchAsync(task.Id, request.DataSourceId, cancellationToken);

            logger.LogInformation(
                "Successfully created execution task {TaskId} for adding boundary {Key} to table {Schema}.{Table}",
                task.Id,
                sortKey,
                request.SchemaName,
                request.TableName);

            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to add boundary to table {Schema}.{Table}", request.SchemaName, request.TableName);
            return Result.Failure("添加边界时发生异常,请稍后重试。");
        }
    }
}
