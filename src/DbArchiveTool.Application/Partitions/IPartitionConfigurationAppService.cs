using System.Collections.Generic;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区配置相关的应用服务。
/// </summary>
public interface IPartitionConfigurationAppService
{
    /// <summary>创建新的分区配置。</summary>
    Task<Result<Guid>> CreateAsync(CreatePartitionConfigurationRequest request, CancellationToken cancellationToken = default);

    /// <summary>替换分区配置的边界列表。</summary>
    Task<Result> ReplaceValuesAsync(Guid configurationId, ReplacePartitionValuesRequest request, CancellationToken cancellationToken = default);

    /// <summary>新增单个分区边界。</summary>
    Task<Result> AddBoundaryAsync(Guid configurationId, AddPartitionBoundaryRequest request, CancellationToken cancellationToken = default);

    /// <summary>对指定分区执行拆分操作。</summary>
    Task<Result> SplitBoundaryAsync(Guid configurationId, SplitPartitionBoundaryRequest request, CancellationToken cancellationToken = default);

    /// <summary>合并相邻分区。</summary>
    Task<Result> MergeBoundaryAsync(Guid configurationId, MergePartitionBoundaryRequest request, CancellationToken cancellationToken = default);

    /// <summary>获取数据源下的所有分区配置。</summary>
    Task<Result<List<PartitionConfigurationSummaryDto>>> GetByDataSourceAsync(Guid dataSourceId, CancellationToken cancellationToken = default);

    /// <summary>删除分区配置。</summary>
    Task<Result> DeleteAsync(Guid configurationId, CancellationToken cancellationToken = default);

    /// <summary>获取指定分区配置详情。</summary>
    Task<Result<PartitionConfigurationDetailDto>> GetAsync(Guid configurationId, CancellationToken cancellationToken = default);

    /// <summary>更新分区配置的基础信息。</summary>
    Task<Result> UpdateAsync(Guid configurationId, UpdatePartitionConfigurationRequest request, CancellationToken cancellationToken = default);
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
/// 替换边界值请求。
/// </summary>
public sealed record ReplacePartitionValuesRequest(
    IReadOnlyList<string> BoundaryValues,
    string UpdatedBy);

/// <summary>
/// 新增分区边界请求。
/// </summary>
public sealed record AddPartitionBoundaryRequest(
    string BoundaryValue,
    string? FilegroupName,
    string RequestedBy);

/// <summary>
/// 拆分分区请求。
/// </summary>
public sealed record SplitPartitionBoundaryRequest(
    string BoundaryKey,
    string NewBoundaryValue,
    string? FilegroupName,
    string RequestedBy);

/// <summary>
/// 合并分区请求。
/// </summary>
public sealed record MergePartitionBoundaryRequest(
    string BoundaryKey,
    string RequestedBy);

/// <summary>
/// 更新分区配置请求。
/// </summary>
public sealed record UpdatePartitionConfigurationRequest(
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
    string UpdatedBy,
    string? Remarks);

/// <summary>
/// 分区配置概要信息。
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
    public bool IsCommitted { get; set; }
    /// <summary>
    /// 执行阶段(Queued/Running/Completed/Failed等)
    /// </summary>
    public string? ExecutionStage { get; set; }
    /// <summary>
    /// 最后一次执行任务ID
    /// </summary>
    public Guid? LastExecutionTaskId { get; set; }
}

/// <summary>
/// 分区配置详情信息。
/// </summary>
public sealed class PartitionConfigurationDetailDto
{
    public Guid Id { get; set; }
    public Guid DataSourceId { get; set; }
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string PartitionFunctionName { get; set; } = string.Empty;
    public string PartitionSchemeName { get; set; } = string.Empty;
    public string PartitionColumnName { get; set; } = string.Empty;
    public PartitionValueKind PartitionColumnKind { get; set; }
    public bool PartitionColumnIsNullable { get; set; }
    public PartitionStorageMode StorageMode { get; set; }
    public string? FilegroupName { get; set; }
    public string? DataFileDirectory { get; set; }
    public string? DataFileName { get; set; }
    public int? InitialFileSizeMb { get; set; }
    public int? AutoGrowthMb { get; set; }
    public string TargetDatabaseName { get; set; } = string.Empty;
    public string TargetSchemaName { get; set; } = string.Empty;
    public string TargetTableName { get; set; } = string.Empty;
    public bool RequirePartitionColumnNotNull { get; set; }
    public string? Remarks { get; set; }
    public bool IsCommitted { get; set; }
    public bool SourceTableIsPartitioned { get; set; }
    public List<string> BoundaryValues { get; set; } = new();
    public string CreatedBy { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}
