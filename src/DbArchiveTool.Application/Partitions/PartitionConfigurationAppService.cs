using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区配置向导的应用服务实现。
/// </summary>
internal sealed class PartitionConfigurationAppService : IPartitionConfigurationAppService
{
    private static readonly IComparer<PartitionBoundary> BoundaryComparer = Comparer<PartitionBoundary>.Create((left, right) => left.CompareTo(right));

    private readonly IPartitionMetadataRepository metadataRepository;
    private readonly IPartitionConfigurationRepository configurationRepository;
    private readonly IBackgroundTaskRepository executionTaskRepository;
    private readonly IBackgroundTaskLogRepository executionLogRepository;
    private readonly IPartitionAuditLogRepository auditLogRepository;
    private readonly IPartitionCommandScriptGenerator scriptGenerator;
    private readonly PartitionValueParser valueParser;
    private readonly ILogger<PartitionConfigurationAppService> logger;

    public PartitionConfigurationAppService(
        IPartitionMetadataRepository metadataRepository,
        IPartitionConfigurationRepository configurationRepository,
        IBackgroundTaskRepository executionTaskRepository,
        IBackgroundTaskLogRepository executionLogRepository,
        IPartitionAuditLogRepository auditLogRepository,
        IPartitionCommandScriptGenerator scriptGenerator,
        PartitionValueParser valueParser,
        ILogger<PartitionConfigurationAppService> logger)
    {
        this.metadataRepository = metadataRepository;
        this.configurationRepository = configurationRepository;
        this.executionTaskRepository = executionTaskRepository;
        this.executionLogRepository = executionLogRepository;
        this.auditLogRepository = auditLogRepository;
        this.scriptGenerator = scriptGenerator;
        this.valueParser = valueParser;
        this.logger = logger;
    }

    public async Task<Result<Guid>> CreateAsync(CreatePartitionConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateCreateRequest(request);
        if (!validation.IsSuccess)
        {
            return Result<Guid>.Failure(validation.Error!);
        }

        // 检查列的可空性要求
        if (request.PartitionColumnIsNullable && !request.RequirePartitionColumnNotNull)
        {
            return Result<Guid>.Failure("目标分区列当前允许 NULL，请勾选'去可空'以确保脚本生成 ALTER COLUMN。");
        }

        // 检查是否已存在配置
        var duplicate = await configurationRepository.GetByTableAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (duplicate is not null)
        {
            return Result<Guid>.Failure("已存在该表的分区配置草稿，请勿重复创建。");
        }

        // 构建存储设置
        var storageResult = BuildStorageSettings(
            request.StorageMode,
            request.TableName,
            "PRIMARY",
            request.FilegroupName,
            request.DataFileDirectory,
            request.DataFileName,
            request.InitialFileSizeMb,
            request.AutoGrowthMb);
        if (!storageResult.IsSuccess)
        {
            return Result<Guid>.Failure(storageResult.Error!);
        }

        // 构建目标表信息
        PartitionTargetTable targetTable;
        try
        {
            var targetSchema = string.IsNullOrWhiteSpace(request.TargetSchemaName) ? request.SchemaName : request.TargetSchemaName!;
            targetTable = PartitionTargetTable.Create(request.TargetDatabaseName, targetSchema, request.TargetTableName, request.Remarks);
        }
        catch (Exception ex) when (ex is ArgumentException or ArgumentOutOfRangeException)
        {
            return Result<Guid>.Failure(ex.Message);
        }

        // 使用前端传递的列信息构建 PartitionColumn
        var partitionColumn = new PartitionColumn(request.PartitionColumnName, request.PartitionColumnKind, request.PartitionColumnIsNullable);
        
        // 使用默认的文件组策略（对于新建分区，使用 PRIMARY）
        var filegroupStrategy = PartitionFilegroupStrategy.Default("PRIMARY");

        // 生成默认的分区函数和方案名称
        var partitionFunctionName = $"PF_{request.SchemaName}_{request.TableName}_{DateTime.UtcNow:yyyyMMdd}";
        var partitionSchemeName = $"PS_{request.SchemaName}_{request.TableName}_{DateTime.UtcNow:yyyyMMdd}";
        
        // 默认使用 RANGE RIGHT（边界值属于右侧分区）
        var isRangeRight = true;

        var configuration = new PartitionConfiguration(
            request.DataSourceId,
            request.SchemaName,
            request.TableName,
            partitionFunctionName,
            partitionSchemeName,
            partitionColumn,
            filegroupStrategy,
            isRangeRight,
            retentionPolicy: null,
            safetyRule: null,
            storageSettings: storageResult.Value!,
            targetTable: targetTable,
            requirePartitionColumnNotNull: request.RequirePartitionColumnNotNull,
            remarks: request.Remarks);

        try
        {
            configuration.InitializeAudit(request.CreatedBy);
            await configurationRepository.AddAsync(configuration, cancellationToken);
            logger.LogInformation("Partition configuration {ConfigurationId} created for {Schema}.{Table} by {User}", configuration.Id, request.SchemaName, request.TableName, request.CreatedBy);
            return Result<Guid>.Success(configuration.Id);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create partition configuration for {Schema}.{Table}", request.SchemaName, request.TableName);
            return Result<Guid>.Failure("创建分区配置时发生错误，请稍后重试。");
        }
    }

    public async Task<Result> AddBoundaryAsync(Guid configurationId, AddPartitionBoundaryRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateAddBoundaryRequest(configurationId, request);
        if (!validation.IsSuccess)
        {
            return validation;
        }

        var configuration = await configurationRepository.GetByIdAsync(configurationId, cancellationToken);
        if (configuration is null)
        {
            return Result.Failure("未找到指定的分区配置。");
        }

        // 对于已提交的配置,需要执行DDL;对于草稿,直接修改配置对象
        if (configuration.IsCommitted)
        {
            // 已分区表:执行ALTER PARTITION FUNCTION
            return await AddBoundaryToCommittedConfigurationAsync(configuration, request, cancellationToken);
        }
        else
        {
            // 草稿配置:直接修改边界值列表
            return await AddBoundaryToDraftConfigurationAsync(configuration, request, cancellationToken);
        }
    }

    /// <summary>
    /// 为草稿配置添加边界值(修改内存对象)
    /// </summary>
    private async Task<Result> AddBoundaryToDraftConfigurationAsync(
        PartitionConfiguration configuration,
        AddPartitionBoundaryRequest request,
        CancellationToken cancellationToken)
    {

        var parse = valueParser.ParseValues(configuration.PartitionColumn, new[] { request.BoundaryValue });
        if (!parse.IsSuccess)
        {
            return Result.Failure(parse.Error!);
        }

        var boundaryValue = parse.Value![0];
        var boundaryKey = GenerateBoundaryKey();
        var boundary = new PartitionBoundary(boundaryKey, boundaryValue);
        var literalForLog = boundaryValue.ToLiteral();

        var addResult = configuration.TryAddBoundary(boundary);
        if (!addResult.IsSuccess)
        {
            return Result.Failure(addResult.ErrorMessage ?? "新增分区边界失败。");
        }

        if (!string.IsNullOrWhiteSpace(request.FilegroupName))
        {
            var assignResult = configuration.TryAssignFilegroup(boundaryKey, request.FilegroupName!);
            if (!assignResult.IsSuccess)
            {
                configuration.TryRemoveBoundary(boundaryKey);
                return Result.Failure(assignResult.ErrorMessage ?? "文件组分配失败。");
            }
        }

        try
        {
            configuration.Touch(request.RequestedBy);
            await configurationRepository.UpdateAsync(configuration, cancellationToken);

            var filegroupLabel = string.IsNullOrWhiteSpace(request.FilegroupName) ? "默认" : request.FilegroupName!;
            var message = $"新增边界 {boundaryKey} = {literalForLog} (文件组: {filegroupLabel})。当前边界数: {configuration.Boundaries.Count}";
            var auditPayload = new
            {
                boundaryKey,
                boundaryValue = literalForLog,
                filegroup = filegroupLabel,
                boundaryCount = configuration.Boundaries.Count
            };

            await RecordBoundaryOperationAsync(
                configuration,
                BackgroundTaskOperationType.AddBoundary,
                request.RequestedBy,
                "新增分区边界",
                message,
                auditPayload,
                cancellationToken);

            logger.LogInformation("Boundary {BoundaryKey} added to draft configuration {ConfigurationId} by {User}", 
                boundaryKey, configuration.Id, request.RequestedBy);
            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to add boundary to draft configuration {ConfigurationId}", configuration.Id);
            configuration.TryRemoveBoundary(boundaryKey);
            return Result.Failure("新增边界时发生异常，请稍后重试。");
        }
    }

    /// <summary>
    /// 为已提交的配置添加边界值(执行DDL)
    /// </summary>
    private async Task<Result> AddBoundaryToCommittedConfigurationAsync(
        PartitionConfiguration configuration,
        AddPartitionBoundaryRequest request,
        CancellationToken cancellationToken)
    {
        // 1. 解析并验证边界值
        var parseResult = valueParser.ParseValues(configuration.PartitionColumn, new[] { request.BoundaryValue });
        if (!parseResult.IsSuccess || parseResult.Value is null || parseResult.Value.Count == 0)
        {
            logger.LogWarning(
                "Invalid boundary value for committed configuration {ConfigurationId}: {Value}",
                configuration.Id,
                request.BoundaryValue);
            return Result.Failure($"边界值格式错误: {parseResult.Error}");
        }

        var newValue = parseResult.Value[0];
        var sortKey = newValue.ToInvariantString();
        var newBoundary = new PartitionBoundary(sortKey, newValue);

        // 检查边界是否已存在
        if (configuration.Boundaries.Any(b => b.SortKey.Equals(sortKey, StringComparison.Ordinal)))
        {
            logger.LogInformation(
                "Boundary {Key} already exists in committed configuration {ConfigurationId}",
                sortKey,
                configuration.Id);
            return Result.Failure("该边界值已存在于分区函数中。");
        }

        // 2. 生成DDL脚本
        var scriptResult = scriptGenerator.GenerateSplitScript(configuration, parseResult.Value, null);
        if (!scriptResult.IsSuccess || string.IsNullOrEmpty(scriptResult.Value))
        {
            logger.LogError(
                "Failed to generate split script for configuration {ConfigurationId}: {Error}",
                configuration.Id,
                scriptResult.Error);
            return Result.Failure($"生成DDL脚本失败: {scriptResult.Error}");
        }

        var ddlScript = scriptResult.Value;

        // 3. 添加边界到配置对象(在执行DDL前先更新,保持一致性)
        var addResult = configuration.TryAddBoundary(newBoundary);
        if (!addResult.IsSuccess)
        {
            logger.LogWarning(
                "Failed to add boundary to committed configuration {ConfigurationId}: {Error}",
                configuration.Id,
                addResult.ErrorMessage);
            return Result.Failure(addResult.ErrorMessage ?? "添加边界失败");
        }

        try
        {
            // 4. 保存配置更新
            await configurationRepository.UpdateAsync(configuration, cancellationToken);

            // 5. 记录审计日志
            await RecordBoundaryOperationAsync(
                configuration,
                BackgroundTaskOperationType.AddBoundary,
                request.RequestedBy,
                "添加分区边界值",
                $"为已分区表 [{configuration.SchemaName}].[{configuration.TableName}] 添加边界值: {request.BoundaryValue}",
                new { BoundaryValue = request.BoundaryValue, FilegroupName = request.FilegroupName },
                cancellationToken,
                ddlScript);

            logger.LogInformation(
                "Successfully added boundary {Key} to committed configuration {ConfigurationId}. DDL script generated.",
                sortKey,
                configuration.Id);

            // 注意:此处仅生成脚本并记录审计,实际DDL执行由用户通过执行任务触发
            // 或者可以集成到BackgroundTaskProcessor中自动执行
            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to add boundary to committed configuration {ConfigurationId}", configuration.Id);
            configuration.TryRemoveBoundary(sortKey); // 回滚配置对象更改
            return Result.Failure("添加边界时发生异常，请稍后重试。");
        }
    }

    public async Task<Result> SplitBoundaryAsync(Guid configurationId, SplitPartitionBoundaryRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateSplitBoundaryRequest(configurationId, request);
        if (!validation.IsSuccess)
        {
            return validation;
        }

        var configuration = await configurationRepository.GetByIdAsync(configurationId, cancellationToken);
        if (configuration is null)
        {
            return Result.Failure("未找到指定的分区配置。");
        }

        if (configuration.IsCommitted)
        {
            return Result.Failure("配置已提交执行，暂不允许直接修改边界。");
        }

        var ordered = configuration.Boundaries
            .OrderBy(b => b, BoundaryComparer)
            .ToList();
        var anchorIndex = ordered.FindIndex(b => b.SortKey.Equals(request.BoundaryKey, StringComparison.Ordinal));
        if (anchorIndex < 0)
        {
            return Result.Failure("目标分区边界不存在。");
        }

        var parse = valueParser.ParseValues(configuration.PartitionColumn, new[] { request.NewBoundaryValue });
        if (!parse.IsSuccess)
        {
            return Result.Failure(parse.Error!);
        }

        var newBoundaryValue = parse.Value![0];
        if (!IsValueWithinSplitRange(configuration.IsRangeRight, newBoundaryValue, anchorIndex, ordered))
        {
            return Result.Failure("新边界值不在允许拆分的范围内。");
        }

        var newBoundaryKey = GenerateBoundaryKey();
        var newBoundary = new PartitionBoundary(newBoundaryKey, newBoundaryValue);
        var addResult = configuration.TryInsertBoundary(newBoundary);
        if (!addResult.IsSuccess)
        {
            return Result.Failure(addResult.ErrorMessage ?? "拆分分区失败。");
        }

        if (!string.IsNullOrWhiteSpace(request.FilegroupName))
        {
            var assignResult = configuration.TryAssignFilegroup(newBoundaryKey, request.FilegroupName!);
            if (!assignResult.IsSuccess)
            {
                configuration.TryRemoveBoundary(newBoundaryKey);
                return Result.Failure(assignResult.ErrorMessage ?? "文件组分配失败。");
            }
        }

        try
        {
            configuration.Touch(request.RequestedBy);
            await configurationRepository.UpdateAsync(configuration, cancellationToken);

            var filegroupLabel = string.IsNullOrWhiteSpace(request.FilegroupName) ? "继承文件组" : request.FilegroupName!;
            var newLiteral = newBoundaryValue.ToLiteral();
            var splitMessage = $"拆分边界 {request.BoundaryKey}，插入新边界 {newBoundaryKey} = {newLiteral} (文件组: {filegroupLabel})。当前边界数: {configuration.Boundaries.Count}";
            var auditPayload = new
            {
                originalBoundaryKey = request.BoundaryKey,
                newBoundaryKey,
                newBoundaryValue = newLiteral,
                filegroup = filegroupLabel,
                boundaryCount = configuration.Boundaries.Count
            };

            await RecordBoundaryOperationAsync(
                configuration,
                BackgroundTaskOperationType.SplitBoundary,
                request.RequestedBy,
                "拆分分区边界",
                splitMessage,
                auditPayload,
                cancellationToken);

            logger.LogInformation("Boundary {BoundaryKey} split in configuration {ConfigurationId} by {User}, new boundary {NewBoundaryKey}", request.BoundaryKey, configurationId, request.RequestedBy, newBoundaryKey);
            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to split boundary {BoundaryKey} for configuration {ConfigurationId}", request.BoundaryKey, configurationId);
            configuration.TryRemoveBoundary(newBoundaryKey);
            return Result.Failure("拆分分区时发生异常，请稍后重试。");
        }
    }

    public async Task<Result> MergeBoundaryAsync(Guid configurationId, MergePartitionBoundaryRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateMergeBoundaryRequest(configurationId, request);
        if (!validation.IsSuccess)
        {
            return validation;
        }

        var configuration = await configurationRepository.GetByIdAsync(configurationId, cancellationToken);
        if (configuration is null)
        {
            return Result.Failure("未找到指定的分区配置。");
        }

        if (configuration.IsCommitted)
        {
            return Result.Failure("配置已提交执行，暂不允许直接修改边界。");
        }

        var mergeResult = configuration.TryRemoveBoundary(request.BoundaryKey);
        if (!mergeResult.IsSuccess)
        {
            return Result.Failure(mergeResult.ErrorMessage ?? "合并分区失败。");
        }

        try
        {
            configuration.Touch(request.RequestedBy);
            await configurationRepository.UpdateAsync(configuration, cancellationToken);
            var mergeMessage = $"合并边界 {request.BoundaryKey} 后，剩余边界数：{configuration.Boundaries.Count}";
            var auditPayload = new
            {
                boundaryKey = request.BoundaryKey,
                boundaryCount = configuration.Boundaries.Count
            };

            await RecordBoundaryOperationAsync(
                configuration,
                BackgroundTaskOperationType.MergeBoundary,
                request.RequestedBy,
                "合并分区边界",
                mergeMessage,
                auditPayload,
                cancellationToken);
            logger.LogInformation("Boundary {BoundaryKey} merged in configuration {ConfigurationId} by {User}", request.BoundaryKey, configurationId, request.RequestedBy);
            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to merge boundary {BoundaryKey} for configuration {ConfigurationId}", request.BoundaryKey, configurationId);
            return Result.Failure("合并分区时发生异常，请稍后重试。");
        }
    }

    public async Task<Result<PartitionConfigurationDetailDto>> GetAsync(Guid configurationId, CancellationToken cancellationToken = default)
    {
        if (configurationId == Guid.Empty)
        {
            return Result<PartitionConfigurationDetailDto>.Failure("配置标识不能为空。");
        }

        var configuration = await configurationRepository.GetByIdAsync(configurationId, cancellationToken);
        if (configuration is null)
        {
            return Result<PartitionConfigurationDetailDto>.Failure("未找到分区配置。");
        }

        var metadata = await metadataRepository.GetConfigurationAsync(
            configuration.ArchiveDataSourceId,
            configuration.SchemaName,
            configuration.TableName,
            cancellationToken);

        var dto = new PartitionConfigurationDetailDto
        {
            Id = configuration.Id,
            DataSourceId = configuration.ArchiveDataSourceId,
            SchemaName = configuration.SchemaName,
            TableName = configuration.TableName,
            PartitionFunctionName = configuration.PartitionFunctionName,
            PartitionSchemeName = configuration.PartitionSchemeName,
            PartitionColumnName = configuration.PartitionColumn.Name,
            PartitionColumnKind = configuration.PartitionColumn.ValueKind,
            PartitionColumnIsNullable = configuration.PartitionColumn.IsNullable,
            StorageMode = configuration.StorageSettings.Mode,
            FilegroupName = configuration.StorageSettings.FilegroupName,
            DataFileDirectory = configuration.StorageSettings.DataFileDirectory,
            DataFileName = configuration.StorageSettings.DataFileName,
            InitialFileSizeMb = configuration.StorageSettings.InitialSizeMb,
            AutoGrowthMb = configuration.StorageSettings.AutoGrowthMb,
            TargetDatabaseName = configuration.TargetTable?.DatabaseName ?? configuration.SchemaName,
            TargetSchemaName = configuration.TargetTable?.SchemaName ?? configuration.SchemaName,
            TargetTableName = configuration.TargetTable?.TableName ?? configuration.TableName,
            RequirePartitionColumnNotNull = configuration.RequirePartitionColumnNotNull,
            Remarks = configuration.Remarks,
            IsCommitted = configuration.IsCommitted,
            SourceTableIsPartitioned = metadata is not null,
            BoundaryValues = configuration.Boundaries
                .OrderBy(x => x.SortKey, StringComparer.Ordinal)
                .Select(x => x.Value.ToInvariantString())
                .ToList(),
            CreatedBy = configuration.CreatedBy,
            CreatedAtUtc = configuration.CreatedAtUtc,
            UpdatedAtUtc = configuration.UpdatedAtUtc
        };

        return Result<PartitionConfigurationDetailDto>.Success(dto);
    }

    public async Task<Result> UpdateAsync(Guid configurationId, UpdatePartitionConfigurationRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateUpdateRequest(configurationId, request);
        if (!validation.IsSuccess)
        {
            return validation;
        }

        var configuration = await configurationRepository.GetByIdAsync(configurationId, cancellationToken);
        if (configuration is null)
        {
            return Result.Failure("未找到分区配置。");
        }

        if (configuration.IsCommitted)
        {
            return Result.Failure("配置已执行，禁止修改。");
        }

        var existing = await metadataRepository.GetConfigurationAsync(
            configuration.ArchiveDataSourceId,
            configuration.SchemaName,
            configuration.TableName,
            cancellationToken);
        if (existing is not null)
        {
            return Result.Failure("目标表已是分区表，禁止修改方案，请通过分区操作功能调整边界。");
        }

        var storageResult = BuildStorageSettings(
            request.StorageMode,
            configuration.TableName,
            configuration.FilegroupStrategy.PrimaryFilegroup,
            request.FilegroupName,
            request.DataFileDirectory,
            request.DataFileName,
            request.InitialFileSizeMb,
            request.AutoGrowthMb);
        if (!storageResult.IsSuccess)
        {
            return Result.Failure(storageResult.Error!);
        }

        PartitionTargetTable targetTable;
        try
        {
            var targetSchema = string.IsNullOrWhiteSpace(request.TargetSchemaName)
                ? configuration.SchemaName
                : request.TargetSchemaName!;

            targetTable = PartitionTargetTable.Create(
                request.TargetDatabaseName,
                targetSchema,
                request.TargetTableName,
                request.Remarks);
        }
        catch (Exception ex) when (ex is ArgumentException or ArgumentOutOfRangeException)
        {
            return Result.Failure(ex.Message);
        }

        configuration.UpdateStorageSettings(storageResult.Value!);
        configuration.UpdateTargetTable(targetTable);
        configuration.SetPartitionColumnNotNullRequirement(request.RequirePartitionColumnNotNull);
        configuration.UpdateRemarks(request.Remarks);
        configuration.Touch(request.UpdatedBy);

        try
        {
            await configurationRepository.UpdateAsync(configuration, cancellationToken);
            logger.LogInformation("Partition configuration {ConfigurationId} updated by {User}", configuration.Id, request.UpdatedBy);
            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to update partition configuration {ConfigurationId}", configurationId);
            return Result.Failure("更新分区配置时发生错误，请稍后重试。");
        }
    }

    public async Task<Result> ReplaceValuesAsync(Guid configurationId, ReplacePartitionValuesRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateReplaceRequest(configurationId, request);
        if (!validation.IsSuccess)
        {
            return validation;
        }

        var configuration = await configurationRepository.GetByIdAsync(configurationId, cancellationToken);
        if (configuration is null)
        {
            return Result.Failure("未找到分区配置。");
        }

        if (configuration.IsCommitted)
        {
            return Result.Failure("配置已执行，禁止修改分区值。");
        }

        var parseResult = valueParser.ParseValues(configuration.PartitionColumn, request.BoundaryValues);
        if (!parseResult.IsSuccess)
        {
            return Result.Failure(parseResult.Error!);
        }

        var replaceResult = configuration.ReplaceBoundaries(parseResult.Value!);
        if (!replaceResult.IsSuccess)
        {
            return Result.Failure(replaceResult.ErrorMessage ?? "分区边界校验失败。");
        }

        try
        {
            configuration.Touch(request.UpdatedBy);
            await configurationRepository.UpdateAsync(configuration, cancellationToken);
            logger.LogInformation("Partition configuration {ConfigurationId} updated with {Count} boundaries by {User}", configuration.Id, parseResult.Value!.Count, request.UpdatedBy);
            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to update partition configuration {ConfigurationId}", configurationId);
            return Result.Failure("保存分区值时发生错误，请稍后重试。");
        }
    }

    public async Task<Result<List<PartitionConfigurationSummaryDto>>> GetByDataSourceAsync(Guid dataSourceId, CancellationToken cancellationToken = default)
    {
        try
        {
            var configurations = await configurationRepository.GetByDataSourceAsync(dataSourceId, cancellationToken);
            
            // 查询所有关联的执行任务，用于动态获取最新状态
            var configIds = configurations.Select(c => c.Id).ToHashSet();
            var allTasks = await executionTaskRepository.ListRecentAsync(dataSourceId, 1000, cancellationToken);
            var taskLookup = allTasks
                .Where(t => configIds.Contains(t.PartitionConfigurationId))
                .GroupBy(t => t.PartitionConfigurationId)
                .ToDictionary(
                    g => g.Key,
                    g => g.OrderByDescending(t => t.CreatedAtUtc).First());

            var summaries = configurations.Select(c =>
            {
                // 默认使用配置自身的 ExecutionStage
                var executionStage = c.ExecutionStage;
                var lastTaskId = c.LastExecutionTaskId;

                // 如果有关联的任务，使用任务的最新状态
                if (taskLookup.TryGetValue(c.Id, out var latestTask))
                {
                    executionStage = MapTaskStatusToStage(latestTask.Status);
                    lastTaskId = latestTask.Id;
                }

                return new PartitionConfigurationSummaryDto
                {
                    Id = c.Id,
                    SchemaName = c.SchemaName,
                    TableName = c.TableName,
                    PartitionColumnName = c.PartitionColumn.Name,
                    PartitionFunctionName = c.PartitionFunctionName,
                    PartitionSchemeName = c.PartitionSchemeName,
                    BoundaryCount = c.Boundaries.Count,
                    StorageMode = c.StorageSettings.Mode.ToString(),
                    TargetTableName = c.TargetTable?.TableName ?? c.TableName,
                    CreatedAtUtc = c.CreatedAtUtc,
                    CreatedBy = c.CreatedBy,
                    UpdatedAtUtc = c.UpdatedAtUtc,
                    Remarks = c.Remarks,
                    IsCommitted = c.IsCommitted,
                    ExecutionStage = executionStage,
                    LastExecutionTaskId = lastTaskId
                };
            }).ToList();

            return Result<List<PartitionConfigurationSummaryDto>>.Success(summaries);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get configurations for data source {DataSourceId}", dataSourceId);
            return Result<List<PartitionConfigurationSummaryDto>>.Failure("获取配置列表失败，请稍后重试。");
        }
    }

    /// <summary>
    /// 将 BackgroundTaskStatus 映射为 ExecutionStage 字符串
    /// </summary>
    private static string MapTaskStatusToStage(BackgroundTaskStatus status)
    {
        return status switch
        {
            BackgroundTaskStatus.PendingValidation => "PendingValidation",
            BackgroundTaskStatus.Validating => "Validating",
            BackgroundTaskStatus.Queued => "Queued",
            BackgroundTaskStatus.Running => "Running",
            BackgroundTaskStatus.Succeeded => "Succeeded",
            BackgroundTaskStatus.Failed => "Failed",
            BackgroundTaskStatus.Cancelled => "Cancelled",
            _ => status.ToString()
        };
    }

    public async Task<Result> DeleteAsync(Guid configurationId, CancellationToken cancellationToken = default)
    {
        try
        {
            var configuration = await configurationRepository.GetByIdAsync(configurationId, cancellationToken);
            if (configuration is null)
            {
                return Result.Failure("配置不存在。");
            }

            if (configuration.IsCommitted)
            {
                return Result.Failure("配置已执行，禁止删除。");
            }

            await configurationRepository.DeleteAsync(configurationId, cancellationToken);
            logger.LogInformation("Partition configuration {ConfigurationId} deleted", configurationId);
            return Result.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to delete partition configuration {ConfigurationId}", configurationId);
            return Result.Failure("删除配置失败，请稍后重试。");
        }
    }

    private static Result ValidateCreateRequest(CreatePartitionConfigurationRequest request)
    {
        if (request is null)
        {
            return Result.Failure("请求体不能为空。");
        }

        if (request.DataSourceId == Guid.Empty)
        {
            return Result.Failure("数据源标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.SchemaName))
        {
            return Result.Failure("源表架构名称不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.TableName))
        {
            return Result.Failure("源表名称不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.PartitionColumnName))
        {
            return Result.Failure("分区列不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.TargetDatabaseName))
        {
            return Result.Failure("目标数据库名称不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.TargetTableName))
        {
            return Result.Failure("目标表名称不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.CreatedBy))
        {
            return Result.Failure("操作人不能为空。");
        }

        if (request.StorageMode == PartitionStorageMode.DedicatedFilegroupSingleFile)
        {
            if (!request.InitialFileSizeMb.HasValue || request.InitialFileSizeMb.Value <= 0)
            {
                return Result.Failure("请填写有效的数据文件初始大小。");
            }

            if (!request.AutoGrowthMb.HasValue || request.AutoGrowthMb.Value <= 0)
            {
                return Result.Failure("请填写有效的自动增长大小。");
            }

            if (string.IsNullOrWhiteSpace(request.DataFileDirectory) || string.IsNullOrWhiteSpace(request.DataFileName))
            {
                return Result.Failure("请填写数据文件目录与文件名。");
            }
        }

        return Result.Success();
    }

    private static Result ValidateUpdateRequest(Guid configurationId, UpdatePartitionConfigurationRequest request)
    {
        if (configurationId == Guid.Empty)
        {
            return Result.Failure("配置标识不能为空。");
        }

        if (request is null)
        {
            return Result.Failure("请求体不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.TargetDatabaseName))
        {
            return Result.Failure("目标数据库名称不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.TargetTableName))
        {
            return Result.Failure("目标表名称不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.UpdatedBy))
        {
            return Result.Failure("操作人不能为空。");
        }

        if (request.StorageMode == PartitionStorageMode.DedicatedFilegroupSingleFile)
        {
            if (!request.InitialFileSizeMb.HasValue || request.InitialFileSizeMb.Value <= 0)
            {
                return Result.Failure("请填写有效的数据文件初始大小。");
            }

            if (!request.AutoGrowthMb.HasValue || request.AutoGrowthMb.Value <= 0)
            {
                return Result.Failure("请填写有效的自动增长大小。");
            }

            if (string.IsNullOrWhiteSpace(request.DataFileDirectory) || string.IsNullOrWhiteSpace(request.DataFileName))
            {
                return Result.Failure("请填写数据文件目录与文件名。");
            }
        }

        return Result.Success();
    }

    private static Result ValidateReplaceRequest(Guid configurationId, ReplacePartitionValuesRequest request)
    {
        if (configurationId == Guid.Empty)
        {
            return Result.Failure("配置标识不能为空。");
        }

        if (request is null)
        {
            return Result.Failure("请求体不能为空。");
        }

        if (request.BoundaryValues is null || request.BoundaryValues.Count == 0)
        {
            return Result.Failure("至少提供一个分区边界。");
        }

        if (string.IsNullOrWhiteSpace(request.UpdatedBy))
        {
            return Result.Failure("操作人不能为空。");
        }

        return Result.Success();
    }

    private static Result ValidateAddBoundaryRequest(Guid configurationId, AddPartitionBoundaryRequest request)
    {
        if (configurationId == Guid.Empty)
        {
            return Result.Failure("配置标识不能为空。");
        }

        if (request is null)
        {
            return Result.Failure("请求不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.BoundaryValue))
        {
            return Result.Failure("需要提供新的分区边界值。");
        }

        if (string.IsNullOrWhiteSpace(request.RequestedBy))
        {
            return Result.Failure("操作人不能为空。");
        }

        return Result.Success();
    }

    private static Result ValidateSplitBoundaryRequest(Guid configurationId, SplitPartitionBoundaryRequest request)
    {
        if (configurationId == Guid.Empty)
        {
            return Result.Failure("配置标识不能为空。");
        }

        if (request is null)
        {
            return Result.Failure("请求不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.BoundaryKey))
        {
            return Result.Failure("需要指定要拆分的分区边界。");
        }

        if (string.IsNullOrWhiteSpace(request.NewBoundaryValue))
        {
            return Result.Failure("需要提供新的分区边界值。");
        }

        if (string.IsNullOrWhiteSpace(request.RequestedBy))
        {
            return Result.Failure("操作人不能为空。");
        }

        return Result.Success();
    }

    private static Result ValidateMergeBoundaryRequest(Guid configurationId, MergePartitionBoundaryRequest request)
    {
        if (configurationId == Guid.Empty)
        {
            return Result.Failure("配置标识不能为空。");
        }

        if (request is null)
        {
            return Result.Failure("请求不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.BoundaryKey))
        {
            return Result.Failure("需要指定要合并的分区边界。");
        }

        if (string.IsNullOrWhiteSpace(request.RequestedBy))
        {
            return Result.Failure("操作人不能为空。");
        }

        return Result.Success();
    }

    private static string GenerateBoundaryKey() => $"AUTO_{DateTime.UtcNow:yyyyMMddHHmmssfff}_{Guid.NewGuid():N}";

    private async Task RecordBoundaryOperationAsync(
        PartitionConfiguration configuration,
        BackgroundTaskOperationType operationType,
        string requestedBy,
        string title,
        string message,
        object? auditPayload,
        CancellationToken cancellationToken,
        string? script = null)
    {
        try
        {
            var targetDatabase = configuration.TargetTable?.DatabaseName ?? configuration.SchemaName;
            var targetTable = configuration.TargetTable is not null
                ? $"{configuration.TargetTable.SchemaName}.{configuration.TargetTable.TableName}"
                : $"{configuration.SchemaName}.{configuration.TableName}";

            var task = BackgroundTask.Create(
                configuration.Id,
                configuration.ArchiveDataSourceId,
                requestedBy,
                requestedBy,
                operationType: operationType,
                archiveTargetDatabase: targetDatabase,
                archiveTargetTable: targetTable);

            task.MarkValidating(requestedBy);
            task.MarkSucceeded(requestedBy);

            await executionTaskRepository.AddAsync(task, cancellationToken);

            var log = BackgroundTaskLogEntry.Create(task.Id, "Info", title, message);
            await executionLogRepository.AddAsync(log, cancellationToken);

            var payloadEnvelope = new
            {
                message,
                data = auditPayload,
                targetDatabase,
                targetTable,
                configurationId = configuration.Id,
                schema = configuration.SchemaName,
                table = configuration.TableName
            };

            var payloadJson = JsonSerializer.Serialize(payloadEnvelope);
            var auditLog = PartitionAuditLog.Create(
                requestedBy,
                operationType.ToString(),
                nameof(PartitionConfiguration),
                configuration.Id.ToString(),
                title,
                payloadJson,
                "Success",
                script);

            await auditLogRepository.AddAsync(auditLog, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "记录分区边界操作日志失败: ConfigurationId={ConfigurationId}, Operation={Operation}", configuration.Id, operationType);
        }
    }

    private static bool IsValueWithinSplitRange(bool isRangeRight, PartitionValue newValue, int anchorIndex, IReadOnlyList<PartitionBoundary> ordered)
    {
        if (ordered.Count == 0)
        {
            return false;
        }

        if (isRangeRight)
        {
            var lowerBound = anchorIndex > 0 ? ordered[anchorIndex - 1].Value : null;
            var upperBound = ordered[anchorIndex].Value;

            if (lowerBound is not null && newValue.CompareTo(lowerBound) <= 0)
            {
                return false;
            }

            if (newValue.CompareTo(upperBound) >= 0)
            {
                return false;
            }
        }
        else
        {
            var lowerBound = ordered[anchorIndex].Value;
            var upperBound = anchorIndex + 1 < ordered.Count ? ordered[anchorIndex + 1].Value : null;

            if (newValue.CompareTo(lowerBound) <= 0)
            {
                return false;
            }

            if (upperBound is not null && newValue.CompareTo(upperBound) >= 0)
            {
                return false;
            }
        }

        return true;
    }

    private static Result<PartitionStorageSettings> BuildStorageSettings(
        PartitionStorageMode storageMode,
        string tableName,
        string primaryFilegroup,
        string? filegroupName,
        string? dataFileDirectory,
        string? dataFileName,
        int? initialFileSizeMb,
        int? autoGrowthMb)
    {
        try
        {
            return storageMode switch
            {
                PartitionStorageMode.PrimaryFilegroup => Result<PartitionStorageSettings>.Success(
                    PartitionStorageSettings.UsePrimary(primaryFilegroup)),
                PartitionStorageMode.DedicatedFilegroupSingleFile => CreateDedicatedStorage(
                    tableName,
                    filegroupName,
                    dataFileDirectory!,
                    dataFileName!,
                    initialFileSizeMb!.Value,
                    autoGrowthMb!.Value),
                _ => Result<PartitionStorageSettings>.Failure("不支持的存放模式。")
            };
        }
        catch (Exception ex) when (ex is ArgumentException or ArgumentOutOfRangeException)
        {
            return Result<PartitionStorageSettings>.Failure(ex.Message);
        }
    }

    private static Result<PartitionStorageSettings> CreateDedicatedStorage(
        string tableName,
        string? filegroupName,
        string dataFileDirectory,
        string dataFileName,
        int initialFileSizeMb,
        int autoGrowthMb)
    {
        var finalFilegroupName = string.IsNullOrWhiteSpace(filegroupName)
            ? $"{tableName}_FG_{DateTime.UtcNow:yyyyMMdd}"
            : filegroupName!;

        var settings = PartitionStorageSettings.CreateDedicated(
            finalFilegroupName,
            dataFileDirectory,
            dataFileName,
            initialFileSizeMb,
            autoGrowthMb);

        return Result<PartitionStorageSettings>.Success(settings);
    }

    private static PartitionFilegroupStrategy CloneStrategy(PartitionFilegroupStrategy original)
    {
        var strategy = PartitionFilegroupStrategy.Default(original.PrimaryFilegroup);
        foreach (var filegroup in original.AdditionalFilegroups)
        {
            strategy.AddFilegroup(filegroup);
        }

        return strategy;
    }
}
