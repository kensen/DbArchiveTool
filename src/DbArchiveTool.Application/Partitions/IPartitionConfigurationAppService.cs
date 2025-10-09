using System.Collections.Generic;

using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 定义分区配置向导相关的用例。
/// </summary>
public interface IPartitionConfigurationAppService
{
    /// <summary>创建新的分区配置草稿。</summary>
    Task<Result<Guid>> CreateAsync(CreatePartitionConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>重置分区配置的边界列表。</summary>
    Task<Result> ReplaceValuesAsync(Guid configurationId, ReplacePartitionValuesRequest request, CancellationToken cancellationToken = default);
}

/// <summary>
/// 创建分区配置的请求。
/// </summary>
public sealed record CreatePartitionConfigurationRequest(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    PartitionStorageMode StorageMode,
    string? FilegroupName,
    string? DataFileDirectory,
    string? DataFileName,
    int? InitialFileSizeMb,
    int? AutoGrowthMb,
    string TargetDatabaseName,
    string? TargetSchemaName,
    string TargetTableName,
    bool RequirePartitionColumnNotNull,
    string CreatedBy,
    string? Remarks);

/// <summary>
/// 重置分区值的请求。
/// </summary>
public sealed record ReplacePartitionValuesRequest(
    IReadOnlyList<string> BoundaryValues,
    string UpdatedBy);
