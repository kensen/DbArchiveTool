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
    private readonly PartitionValueParser valueParser;
    private readonly ILogger<PartitionManagementAppService> logger;

    public PartitionManagementAppService(
        IPartitionMetadataRepository repository,
        IPartitionCommandScriptGenerator scriptGenerator,
        IPartitionAuditLogRepository auditLogRepository,
        PartitionValueParser valueParser,
        ILogger<PartitionManagementAppService> logger)
    {
        this.repository = repository;
        this.scriptGenerator = scriptGenerator;
        this.auditLogRepository = auditLogRepository;
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

        // 5. 生成DDL脚本
        var scriptResult = scriptGenerator.GenerateSplitScript(config, parseResult.Value);
        if (!scriptResult.IsSuccess || string.IsNullOrEmpty(scriptResult.Value))
        {
            logger.LogError(
                "Failed to generate split script for table {Schema}.{Table}: {Error}",
                request.SchemaName,
                request.TableName,
                scriptResult.Error);
            return Result.Failure($"生成DDL脚本失败: {scriptResult.Error}");
        }

        var ddlScript = scriptResult.Value;

        try
        {
            // 6. 记录审计日志(包含DDL脚本)
            var resourceId = $"{request.SchemaName}.{request.TableName}";
            var summary = $"为已分区表 [{request.SchemaName}].[{request.TableName}] 添加边界值: {request.BoundaryValue}";
            var payload = System.Text.Json.JsonSerializer.Serialize(new
            {
                DataSourceId = request.DataSourceId,
                SchemaName = request.SchemaName,
                TableName = request.TableName,
                BoundaryValue = request.BoundaryValue,
                SortKey = sortKey,
                FilegroupName = request.FilegroupName,
                Notes = request.Notes,
                PartitionFunction = config.PartitionFunctionName,
                PartitionScheme = config.PartitionSchemeName
            });

            var auditLog = PartitionAuditLog.Create(
                request.RequestedBy,
                PartitionExecutionOperationType.AddBoundary.ToString(),
                "PartitionedTable",
                resourceId,
                summary,
                payload,
                "Success",
                ddlScript);

            await auditLogRepository.AddAsync(auditLog, cancellationToken);

            logger.LogInformation(
                "Successfully generated DDL for adding boundary {Key} to table {Schema}.{Table}. Audit log created: {AuditId}",
                sortKey,
                request.SchemaName,
                request.TableName,
                auditLog.Id);

            // 注意: 此处仅生成脚本并记录审计日志,实际DDL执行需要:
            // 1. 用户手动执行脚本,或
            // 2. 通过PartitionExecutionProcessor自动执行(如果集成)
            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to add boundary to table {Schema}.{Table}", request.SchemaName, request.TableName);
            return Result.Failure("添加边界时发生异常,请稍后重试。");
        }
    }
}
