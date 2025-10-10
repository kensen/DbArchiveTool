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

    /// <summary>获取数据源下的所有配置草稿。</summary>
    Task<Result<List<PartitionConfigurationSummaryDto>>> GetByDataSourceAsync(Guid dataSourceId, CancellationToken cancellationToken = default);

    /// <summary>删除配置草稿。</summary>
    Task<Result> DeleteAsync(Guid configurationId, CancellationToken cancellationToken = default);
}

/// <summary>
/// 创建分区配置的请求。
/// </summary>
public sealed record CreatePartitionConfigurationRequest(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string PartitionColumnName,
    PartitionValueKind PartitionColumnKind,
    bool PartitionColumnIsNullable,
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

/// <summary>
/// 分区配置摘要信息。
/// </summary>
public sealed class PartitionConfigurationSummaryDto
{
    public Guid Id { get; set; }
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string PartitionColumnName { get; set; } = string.Empty;
    public string PartitionFunctionName { get; set; } = string.Empty;
    public string PartitionSchemeName { get; set; } = string.Empty;
    public int BoundaryCount { get; set; }
    public string StorageMode { get; set; } = string.Empty;
    public string TargetTableName { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
    public string CreatedBy { get; set; } = string.Empty;
    public DateTime UpdatedAtUtc { get; set; }
    public string? Remarks { get; set; }
}
