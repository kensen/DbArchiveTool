using System;
using System.Collections.Generic;
using DbArchiveTool.Domain.Partitions;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 创建分区配置的请求模型。
/// </summary>
public sealed class CreatePartitionConfigurationRequestModel
{
    public Guid DataSourceId { get; set; }
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    
    /// <summary>分区列名称。</summary>
    public string PartitionColumnName { get; set; } = string.Empty;
    
    /// <summary>分区列数据类型（用于确定 PartitionValueKind）。</summary>
    public PartitionValueKind PartitionColumnKind { get; set; }
    
    /// <summary>分区列是否可空。</summary>
    public bool PartitionColumnIsNullable { get; set; }
    
    public PartitionStorageMode StorageMode { get; set; }
    public string? FilegroupName { get; set; }
    public string? DataFileDirectory { get; set; }
    public string? DataFileName { get; set; }
    public int? InitialFileSizeMb { get; set; }
    public int? AutoGrowthMb { get; set; }
    public string TargetDatabaseName { get; set; } = string.Empty;
    public string? TargetSchemaName { get; set; }
    public string TargetTableName { get; set; } = string.Empty;
    public bool RequirePartitionColumnNotNull { get; set; }
    public string CreatedBy { get; set; } = string.Empty;
    public string? Remarks { get; set; }
}

/// <summary>
/// 更新分区值的请求模型。
/// </summary>
public sealed class ReplacePartitionValuesRequestModel
{
    public List<string> BoundaryValues { get; set; } = new();
    public string UpdatedBy { get; set; } = string.Empty;
}

/// <summary>
/// 分区配置摘要信息（用于列表显示）。
/// </summary>
public sealed class PartitionConfigurationSummaryModel
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
    public DateTime? UpdatedAtUtc { get; set; }
    public string? Remarks { get; set; }
    public bool IsCommitted { get; set; }
}

/// <summary>
/// 更新分区配置基础信息的请求模型。
/// </summary>
public sealed class UpdatePartitionConfigurationRequestModel
{
    public PartitionStorageMode StorageMode { get; set; }
    public string? FilegroupName { get; set; }
    public string? DataFileDirectory { get; set; }
    public string? DataFileName { get; set; }
    public int? InitialFileSizeMb { get; set; }
    public int? AutoGrowthMb { get; set; }
    public string TargetDatabaseName { get; set; } = string.Empty;
    public string? TargetSchemaName { get; set; }
    public string TargetTableName { get; set; } = string.Empty;
    public bool RequirePartitionColumnNotNull { get; set; }
    public string UpdatedBy { get; set; } = string.Empty;
    public string? Remarks { get; set; }
}

/// <summary>
/// 分区配置草稿详情。
/// </summary>
public sealed class PartitionConfigurationDetailModel
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
