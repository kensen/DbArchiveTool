using System;
using System.Collections.Generic;

namespace DbArchiveTool.Application.Partitions.Dtos;

/// <summary>
/// 执行向导上下文DTO，包含配置信息、表统计、备份状态等
/// </summary>
public sealed class ExecutionWizardContextDto
{
    /// <summary>配置ID</summary>
    public Guid ConfigurationId { get; set; }

    /// <summary>数据源ID</summary>
    public Guid DataSourceId { get; set; }

    /// <summary>数据源名称</summary>
    public string DataSourceName { get; set; } = string.Empty;

    /// <summary>目标表完整名称(Schema.Table)</summary>
    public string FullTableName { get; set; } = string.Empty;

    /// <summary>架构名</summary>
    public string SchemaName { get; set; } = string.Empty;

    /// <summary>表名</summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>分区函数名</summary>
    public string PartitionFunctionName { get; set; } = string.Empty;

    /// <summary>分区方案名</summary>
    public string PartitionSchemeName { get; set; } = string.Empty;

    /// <summary>分区列名</summary>
    public string PartitionColumnName { get; set; } = string.Empty;

    /// <summary>分区列类型</summary>
    public string PartitionColumnType { get; set; } = string.Empty;

    /// <summary>是否Range Right</summary>
    public bool IsRangeRight { get; set; }

    /// <summary>是否要求分区列NOT NULL</summary>
    public bool RequirePartitionColumnNotNull { get; set; }

    /// <summary>主文件组</summary>
    public string PrimaryFilegroup { get; set; } = "PRIMARY";

    /// <summary>附加文件组列表</summary>
    public List<string> AdditionalFilegroups { get; set; } = new();

    /// <summary>分区边界值列表</summary>
    public List<PartitionBoundaryDto> Boundaries { get; set; } = new();

    /// <summary>索引对齐检查结果</summary>
    public IndexInspectionDto IndexInspection { get; set; } = new();

    /// <summary>表统计信息</summary>
    public TableStatisticsDto? TableStatistics { get; set; }

    /// <summary>配置备注</summary>
    public string? Remarks { get; set; }

    /// <summary>当前执行状态</summary>
    public string? ExecutionStage { get; set; }

    /// <summary>是否已提交</summary>
    public bool IsCommitted { get; set; }
}

/// <summary>
/// 分区边界DTO
/// </summary>
public sealed class PartitionBoundaryDto
{
    /// <summary>排序键</summary>
    public string SortKey { get; set; } = string.Empty;

    /// <summary>原始值</summary>
    public string RawValue { get; set; } = string.Empty;

    /// <summary>显示值</summary>
    public string DisplayValue { get; set; } = string.Empty;
}

/// <summary>
/// 表统计信息DTO
/// </summary>
public sealed class TableStatisticsDto
{
    /// <summary>表是否存在</summary>
    public bool TableExists { get; set; }

    /// <summary>总行数</summary>
    public long TotalRows { get; set; }

    /// <summary>数据大小(MB)</summary>
    public decimal DataSizeMB { get; set; }

    /// <summary>索引大小(MB)</summary>
    public decimal IndexSizeMB { get; set; }

    /// <summary>总大小(MB)</summary>
    public decimal TotalSizeMB { get; set; }

    /// <summary>索引数量</summary>
    public int IndexCount { get; set; }

    /// <summary>是否已分区</summary>
    public bool IsPartitioned { get; set; }

    /// <summary>当前分区数</summary>
    public int PartitionCount { get; set; }

    /// <summary>最后更新时间</summary>
    public DateTime? LastUpdated { get; set; }
}

/// <summary>
/// 备份检查结果DTO
/// </summary>
public sealed class BackupCheckResultDto
{
    /// <summary>是否通过检查</summary>
    public bool Passed { get; set; }

    /// <summary>检查消息</summary>
    public string Message { get; set; } = string.Empty;

    /// <summary>最近的备份记录</summary>
    public List<BackupRecordDto> RecentBackups { get; set; } = new();

    /// <summary>建议操作</summary>
    public string? Recommendation { get; set; }
}

/// <summary>
/// 备份记录DTO
/// </summary>
public sealed class BackupRecordDto
{
    /// <summary>备份类型</summary>
    public string BackupType { get; set; } = string.Empty;

    /// <summary>备份开始时间</summary>
    public DateTime BackupStartDate { get; set; }

    /// <summary>备份结束时间</summary>
    public DateTime BackupFinishDate { get; set; }

    /// <summary>备份大小(MB)</summary>
    public decimal BackupSizeMB { get; set; }

    /// <summary>执行人</summary>
    public string UserName { get; set; } = string.Empty;

    /// <summary>备份位置</summary>
    public string PhysicalDeviceName { get; set; } = string.Empty;

    /// <summary>距今小时数</summary>
    public double HoursAgo { get; set; }
}

/// <summary>
/// 安全检查结果DTO
/// </summary>
public sealed class SafetyCheckResultDto
{
    /// <summary>整体是否通过</summary>
    public bool AllPassed { get; set; }

    /// <summary>各项检查结果</summary>
    public List<SafetyCheckItemDto> CheckItems { get; set; } = new();

    /// <summary>警告列表</summary>
    public List<string> Warnings { get; set; } = new();
}

/// <summary>
/// 安全检查项DTO
/// </summary>
public sealed class SafetyCheckItemDto
{
    /// <summary>检查项名称</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>检查项描述</summary>
    public string Description { get; set; } = string.Empty;

    /// <summary>是否通过</summary>
    public bool Passed { get; set; }

    /// <summary>检查结果消息</summary>
    public string Message { get; set; } = string.Empty;

    /// <summary>严重级别(Info/Warning/Error)</summary>
    public string Severity { get; set; } = "Info";

    /// <summary>处理建议</summary>
    public string? Recommendation { get; set; }
}

/// <summary>
/// 索引对齐检查结果 DTO。
/// </summary>
public sealed class IndexInspectionDto
{
    /// <summary>是否存在聚集索引。</summary>
    public bool HasClusteredIndex { get; set; }

    /// <summary>聚集索引名称。</summary>
    public string? ClusteredIndexName { get; set; }

    /// <summary>聚集索引是否包含分区列。</summary>
    public bool ClusteredIndexContainsPartitionColumn { get; set; }

    /// <summary>聚集索引键列列表。</summary>
    public List<string> ClusteredIndexKeyColumns { get; set; } = new();

    /// <summary>唯一索引/约束列表。</summary>
    public List<IndexAlignmentItemDto> UniqueIndexes { get; set; } = new();

    /// <summary>需要补齐分区列的索引列表。</summary>
    public List<IndexAlignmentItemDto> IndexesNeedingAlignment { get; set; } = new();

    /// <summary>是否存在外部外键引用。</summary>
    public bool HasExternalForeignKeys { get; set; }

    /// <summary>外部外键名称列表。</summary>
    public List<string> ExternalForeignKeys { get; set; } = new();

    /// <summary>是否可以在执行阶段自动对齐。</summary>
    public bool CanAutoAlign { get; set; }

    /// <summary>阻断原因（若存在）。</summary>
    public string? BlockingReason { get; set; }
}

/// <summary>
/// 单个索引对齐项 DTO。
/// </summary>
public sealed class IndexAlignmentItemDto
{
    /// <summary>索引名称。</summary>
    public string IndexName { get; set; } = string.Empty;

    /// <summary>是否聚集索引。</summary>
    public bool IsClustered { get; set; }

    /// <summary>是否主键。</summary>
    public bool IsPrimaryKey { get; set; }

    /// <summary>是否唯一约束。</summary>
    public bool IsUniqueConstraint { get; set; }

    /// <summary>是否唯一索引。</summary>
    public bool IsUnique { get; set; }

    /// <summary>是否包含分区列。</summary>
    public bool ContainsPartitionColumn { get; set; }

    /// <summary>索引键列。</summary>
    public List<string> KeyColumns { get; set; } = new();
}
