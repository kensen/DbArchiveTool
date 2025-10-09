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

