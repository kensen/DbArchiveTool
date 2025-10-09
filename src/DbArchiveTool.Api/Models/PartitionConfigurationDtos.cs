using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Domain.Partitions;

namespace DbArchiveTool.Api.Models;

/// <summary>
/// 创建分区配置的请求体。
/// </summary>
public sealed class CreatePartitionConfigurationDto
{
    [Required]
    public Guid DataSourceId { get; set; }

    [Required]
    public string SchemaName { get; set; } = string.Empty;

    [Required]
    public string TableName { get; set; } = string.Empty;

    [Required]
    public PartitionStorageMode StorageMode { get; set; }

    public string? FilegroupName { get; set; }

    public string? DataFileDirectory { get; set; }

    public string? DataFileName { get; set; }

    public int? InitialFileSizeMb { get; set; }

    public int? AutoGrowthMb { get; set; }

    [Required]
    public string TargetDatabaseName { get; set; } = string.Empty;

    public string? TargetSchemaName { get; set; }

    [Required]
    public string TargetTableName { get; set; } = string.Empty;

    public bool RequirePartitionColumnNotNull { get; set; }

    [Required]
    public string CreatedBy { get; set; } = string.Empty;

    public string? Remarks { get; set; }

    public CreatePartitionConfigurationRequest ToApplicationRequest() =>
        new(
            DataSourceId,
            SchemaName,
            TableName,
            StorageMode,
            FilegroupName,
            DataFileDirectory,
            DataFileName,
            InitialFileSizeMb,
            AutoGrowthMb,
            TargetDatabaseName,
            TargetSchemaName,
            TargetTableName,
            RequirePartitionColumnNotNull,
            CreatedBy,
            Remarks);
}

/// <summary>
/// 更新分区值的请求体。
/// </summary>
public sealed class ReplacePartitionValuesDto
{
    [Required]
    public List<string> BoundaryValues { get; set; } = new();

    [Required]
    public string UpdatedBy { get; set; } = string.Empty;

    public ReplacePartitionValuesRequest ToApplicationRequest() =>
        new(BoundaryValues, UpdatedBy);
}
