using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区配置向导的应用服务实现。
/// </summary>
internal sealed class PartitionConfigurationAppService : IPartitionConfigurationAppService
{
    private readonly IPartitionMetadataRepository metadataRepository;
    private readonly IPartitionConfigurationRepository configurationRepository;
    private readonly PartitionValueParser valueParser;
    private readonly ILogger<PartitionConfigurationAppService> logger;

    public PartitionConfigurationAppService(
        IPartitionMetadataRepository metadataRepository,
        IPartitionConfigurationRepository configurationRepository,
        PartitionValueParser valueParser,
        ILogger<PartitionConfigurationAppService> logger)
    {
        this.metadataRepository = metadataRepository;
        this.configurationRepository = configurationRepository;
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
            var summaries = configurations.Select(c => new PartitionConfigurationSummaryDto
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
                ExecutionStage = c.ExecutionStage,
                LastExecutionTaskId = c.LastExecutionTaskId
            }).ToList();

            return Result<List<PartitionConfigurationSummaryDto>>.Success(summaries);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get configurations for data source {DataSourceId}", dataSourceId);
            return Result<List<PartitionConfigurationSummaryDto>>.Failure("获取配置列表失败，请稍后重试。");
        }
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
