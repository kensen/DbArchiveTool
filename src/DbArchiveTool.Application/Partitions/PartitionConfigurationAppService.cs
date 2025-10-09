using System;
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

        var metadata = await metadataRepository.GetConfigurationAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (metadata is null)
        {
            return Result<Guid>.Failure("未找到目标表的分区元数据信息。");
        }

        if (metadata.PartitionColumn.IsNullable && !request.RequirePartitionColumnNotNull)
        {
            return Result<Guid>.Failure("目标分区列当前允许 NULL，请勾选“去可空”以确保脚本生成 ALTER COLUMN。");
        }

        var duplicate = await configurationRepository.GetByTableAsync(request.DataSourceId, request.SchemaName, request.TableName, cancellationToken);
        if (duplicate is not null)
        {
            return Result<Guid>.Failure("已存在该表的分区配置草稿，请勿重复创建。");
        }

        var storageResult = BuildStorageSettings(request, metadata.FilegroupStrategy.PrimaryFilegroup, metadata.TableName);
        if (!storageResult.IsSuccess)
        {
            return Result<Guid>.Failure(storageResult.Error!);
        }

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

        var partitionColumn = new PartitionColumn(metadata.PartitionColumn.Name, metadata.PartitionColumn.ValueKind, metadata.PartitionColumn.IsNullable);
        var filegroupStrategy = CloneStrategy(metadata.FilegroupStrategy);

        var configuration = new PartitionConfiguration(
            request.DataSourceId,
            request.SchemaName,
            request.TableName,
            metadata.PartitionFunctionName,
            metadata.PartitionSchemeName,
            partitionColumn,
            filegroupStrategy,
            metadata.IsRangeRight,
            retentionPolicy: metadata.RetentionPolicy,
            safetyRule: metadata.SafetyRule,
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
        CreatePartitionConfigurationRequest request,
        string primaryFilegroup,
        string tableName)
    {
        try
        {
            return request.StorageMode switch
            {
                PartitionStorageMode.PrimaryFilegroup => Result<PartitionStorageSettings>.Success(
                    PartitionStorageSettings.UsePrimary(primaryFilegroup)),
                PartitionStorageMode.DedicatedFilegroupSingleFile => CreateDedicatedStorage(request, tableName),
                _ => Result<PartitionStorageSettings>.Failure("不支持的存放模式。")
            };
        }
        catch (Exception ex) when (ex is ArgumentException or ArgumentOutOfRangeException)
        {
            return Result<PartitionStorageSettings>.Failure(ex.Message);
        }
    }

    private static Result<PartitionStorageSettings> CreateDedicatedStorage(CreatePartitionConfigurationRequest request, string tableName)
    {
        var filegroupName = string.IsNullOrWhiteSpace(request.FilegroupName)
            ? $"{tableName}_FG_{DateTime.UtcNow:yyyyMMdd}"
            : request.FilegroupName!;

        var settings = PartitionStorageSettings.CreateDedicated(
            filegroupName,
            request.DataFileDirectory!,
            request.DataFileName!,
            request.InitialFileSizeMb!.Value,
            request.AutoGrowthMb!.Value);

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
