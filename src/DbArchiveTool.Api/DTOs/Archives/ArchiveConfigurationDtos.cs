using DbArchiveTool.Shared.Archive;

namespace DbArchiveTool.Api.DTOs.Archives;

/// <summary>
/// 归档配置列表项 DTO
/// </summary>
public sealed class ArchiveConfigurationListItemDto
{
    /// <summary>配置ID</summary>
    public Guid Id { get; set; }

    /// <summary>配置名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>描述</summary>
    public string? Description { get; set; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; set; }

    /// <summary>源架构名</summary>
    public string SourceSchemaName { get; set; } = string.Empty;

    /// <summary>源表名</summary>
    public string SourceTableName { get; set; } = string.Empty;

    /// <summary>目标架构名</summary>
    public string? TargetSchemaName { get; set; }

    /// <summary>目标表名</summary>
    public string? TargetTableName { get; set; }

    /// <summary>是否分区表</summary>
    public bool IsPartitionedTable { get; set; }

    /// <summary>归档方法</summary>
    public ArchiveMethod ArchiveMethod { get; set; }

    /// <summary>创建时间</summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>更新时间</summary>
    public DateTime UpdatedAtUtc { get; set; }
}

/// <summary>
/// 归档配置详情 DTO
/// </summary>
public sealed class ArchiveConfigurationDetailDto
{
    /// <summary>配置ID</summary>
    public Guid Id { get; set; }

    /// <summary>配置名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>描述</summary>
    public string? Description { get; set; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; set; }

    /// <summary>源架构名</summary>
    public string SourceSchemaName { get; set; } = string.Empty;

    /// <summary>源表名</summary>
    public string SourceTableName { get; set; } = string.Empty;

    /// <summary>目标架构名</summary>
    public string? TargetSchemaName { get; set; }

    /// <summary>目标表名</summary>
    public string? TargetTableName { get; set; }

    /// <summary>是否分区表</summary>
    public bool IsPartitionedTable { get; set; }

    /// <summary>分区配置ID</summary>
    public Guid? PartitionConfigurationId { get; set; }

    /// <summary>归档过滤列名</summary>
    public string? ArchiveFilterColumn { get; set; }

    /// <summary>归档过滤条件</summary>
    public string? ArchiveFilterCondition { get; set; }

    /// <summary>归档方法</summary>
    public ArchiveMethod ArchiveMethod { get; set; }

    /// <summary>是否在归档后删除源数据</summary>
    public bool DeleteSourceDataAfterArchive { get; set; }

    /// <summary>批次大小</summary>
    public int BatchSize { get; set; }

    /// <summary>创建时间</summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>创建人</summary>
    public string CreatedBy { get; set; } = string.Empty;

    /// <summary>更新时间</summary>
    public DateTime UpdatedAtUtc { get; set; }

    /// <summary>更新人</summary>
    public string UpdatedBy { get; set; } = string.Empty;
}

/// <summary>
/// 创建归档配置请求 DTO
/// </summary>
public sealed class CreateArchiveConfigurationRequest
{
    /// <summary>配置名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>描述</summary>
    public string? Description { get; set; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; set; }

    /// <summary>源架构名</summary>
    public string SourceSchemaName { get; set; } = "dbo";

    /// <summary>源表名</summary>
    public string SourceTableName { get; set; } = string.Empty;

    /// <summary>目标架构名</summary>
    public string? TargetSchemaName { get; set; }

    /// <summary>目标表名</summary>
    public string? TargetTableName { get; set; }

    /// <summary>是否分区表</summary>
    public bool IsPartitionedTable { get; set; }

    /// <summary>分区配置ID</summary>
    public Guid? PartitionConfigurationId { get; set; }

    /// <summary>归档过滤列名</summary>
    public string? ArchiveFilterColumn { get; set; }

    /// <summary>归档过滤条件</summary>
    public string? ArchiveFilterCondition { get; set; }

    /// <summary>归档方法</summary>
    public ArchiveMethod ArchiveMethod { get; set; }

    /// <summary>是否在归档后删除源数据</summary>
    public bool DeleteSourceDataAfterArchive { get; set; } = true;

    /// <summary>批次大小</summary>
    public int BatchSize { get; set; } = 10000;
}

/// <summary>
/// 更新归档配置请求 DTO
/// </summary>
public sealed class UpdateArchiveConfigurationRequest
{
    /// <summary>配置名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>描述</summary>
    public string? Description { get; set; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; set; }

    /// <summary>源架构名</summary>
    public string SourceSchemaName { get; set; } = "dbo";

    /// <summary>源表名</summary>
    public string SourceTableName { get; set; } = string.Empty;

    /// <summary>目标架构名</summary>
    public string? TargetSchemaName { get; set; }

    /// <summary>目标表名</summary>
    public string? TargetTableName { get; set; }

    /// <summary>是否分区表</summary>
    public bool IsPartitionedTable { get; set; }

    /// <summary>分区配置ID</summary>
    public Guid? PartitionConfigurationId { get; set; }

    /// <summary>归档过滤列名</summary>
    public string? ArchiveFilterColumn { get; set; }

    /// <summary>归档过滤条件</summary>
    public string? ArchiveFilterCondition { get; set; }

    /// <summary>归档方法</summary>
    public ArchiveMethod ArchiveMethod { get; set; }

    /// <summary>是否在归档后删除源数据</summary>
    public bool DeleteSourceDataAfterArchive { get; set; } = true;

    /// <summary>批次大小</summary>
    public int BatchSize { get; set; } = 10000;
}
