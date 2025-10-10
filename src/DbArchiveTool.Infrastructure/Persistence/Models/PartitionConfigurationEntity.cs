using System;
using System.Collections.Generic;

namespace DbArchiveTool.Infrastructure.Persistence.Models;

/// <summary>
/// EF Core 映射用的分区配置实体。
/// </summary>
public class PartitionConfigurationEntity
{
    public Guid Id { get; set; }
    public Guid ArchiveDataSourceId { get; set; }
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string PartitionFunctionName { get; set; } = string.Empty;
    public string PartitionSchemeName { get; set; } = string.Empty;
    public string PartitionColumnName { get; set; } = string.Empty;
    public int PartitionColumnKind { get; set; }
    public bool PartitionColumnIsNullable { get; set; }
    public string PrimaryFilegroup { get; set; } = "PRIMARY";
    public bool IsRangeRight { get; set; }
    public bool RequirePartitionColumnNotNull { get; set; }
    public string? Remarks { get; set; }

    public int StorageMode { get; set; }
    public string StorageFilegroupName { get; set; } = string.Empty;
    public string? StorageDataFileDirectory { get; set; }
    public string? StorageDataFileName { get; set; }
    public int? StorageInitialSizeMb { get; set; }
    public int? StorageAutoGrowthMb { get; set; }

    public string TargetDatabaseName { get; set; } = string.Empty;
    public string TargetSchemaName { get; set; } = string.Empty;
    public string TargetTableName { get; set; } = string.Empty;
    public string? TargetRemarks { get; set; }

    public bool IsCommitted { get; set; }

    public DateTime CreatedAtUtc { get; set; }
    public string CreatedBy { get; set; } = string.Empty;
    public DateTime UpdatedAtUtc { get; set; }
    public string UpdatedBy { get; set; } = string.Empty;
    public bool IsDeleted { get; set; }

    public ICollection<PartitionConfigurationBoundaryEntity> Boundaries { get; set; } = new List<PartitionConfigurationBoundaryEntity>();
    public ICollection<PartitionConfigurationFilegroupEntity> AdditionalFilegroups { get; set; } = new List<PartitionConfigurationFilegroupEntity>();
    public ICollection<PartitionConfigurationFilegroupMappingEntity> FilegroupMappings { get; set; } = new List<PartitionConfigurationFilegroupMappingEntity>();
}

/// <summary>
/// 分区边界持久化实体。
/// </summary>
public class PartitionConfigurationBoundaryEntity
{
    public Guid Id { get; set; }
    public Guid ConfigurationId { get; set; }
    public string SortKey { get; set; } = string.Empty;
    public int ValueKind { get; set; }
    public string RawValue { get; set; } = string.Empty;

    public PartitionConfigurationEntity Configuration { get; set; } = null!;
}

/// <summary>
/// 附加文件组记录。
/// </summary>
public class PartitionConfigurationFilegroupEntity
{
    public Guid Id { get; set; }
    public Guid ConfigurationId { get; set; }
    public string FilegroupName { get; set; } = string.Empty;
    public int SortOrder { get; set; }

    public PartitionConfigurationEntity Configuration { get; set; } = null!;
}

/// <summary>
/// 边界与文件组的映射实体。
/// </summary>
public class PartitionConfigurationFilegroupMappingEntity
{
    public Guid Id { get; set; }
    public Guid ConfigurationId { get; set; }
    public string BoundaryKey { get; set; } = string.Empty;
    public string FilegroupName { get; set; } = string.Empty;

    public PartitionConfigurationEntity Configuration { get; set; } = null!;
}
