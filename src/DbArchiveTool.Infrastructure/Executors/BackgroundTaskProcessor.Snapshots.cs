using System;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// BackgroundTaskProcessor 的部分类 - 快照数据结构定义
/// </summary>
internal sealed partial class BackgroundTaskProcessor
{
    /// <summary>
    /// 添加边界操作的快照数据结构
    /// </summary>
    private sealed class AddBoundarySnapshot
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string PartitionFunctionName { get; set; } = string.Empty;
        public string PartitionSchemeName { get; set; } = string.Empty;
        public string BoundaryValue { get; set; } = string.Empty;
        public string? FilegroupName { get; set; }
        public string SortKey { get; set; } = string.Empty;
        public string DdlScript { get; set; } = string.Empty;
    }

    /// <summary>
    /// 拆分边界操作的快照数据结构
    /// </summary>
    private sealed class SplitBoundarySnapshot
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string PartitionFunctionName { get; set; } = string.Empty;
        public string PartitionSchemeName { get; set; } = string.Empty;
        public string[] Boundaries { get; set; } = Array.Empty<string>();
        public string DdlScript { get; set; } = string.Empty;
        public bool BackupConfirmed { get; set; }
        public string? FilegroupName { get; set; }  // 用户指定的文件组
    }

    /// <summary>
    /// 合并边界操作的快照数据结构
    /// </summary>
    private sealed class MergeBoundarySnapshot
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string PartitionFunctionName { get; set; } = string.Empty;
        public string PartitionSchemeName { get; set; } = string.Empty;
        public string BoundaryKey { get; set; } = string.Empty;
        public string DdlScript { get; set; } = string.Empty;
        public bool BackupConfirmed { get; set; }
    }

    /// <summary>
    /// 分区切换(归档)操作的快照数据结构
    /// </summary>
    private sealed class ArchiveSwitchSnapshot
    {
        public Guid ConfigurationId { get; set; }
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string SourcePartitionKey { get; set; } = string.Empty;
        public string TargetSchema { get; set; } = string.Empty;
        public string TargetTable { get; set; } = string.Empty;
        public string TargetDatabase { get; set; } = string.Empty;
        public bool CreateStagingTable { get; set; }
        public string DdlScript { get; set; } = string.Empty;
    }

    /// <summary>
    /// BCP 归档快照结构
    /// </summary>
    private sealed class ArchiveBcpSnapshot
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string SourcePartitionKey { get; set; } = string.Empty;
        public string TargetTable { get; set; } = string.Empty;
        public string TargetDatabase { get; set; } = string.Empty;
        public string TempDirectory { get; set; } = string.Empty;
        public int BatchSize { get; set; }
        public bool UseNativeFormat { get; set; }
        public int MaxErrors { get; set; }
        public int TimeoutSeconds { get; set; }
    }

    /// <summary>
    /// BulkCopy 归档快照结构
    /// </summary>
    private sealed class ArchiveBulkCopySnapshot
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string SourcePartitionKey { get; set; } = string.Empty;
        public string TargetTable { get; set; } = string.Empty;
        public string TargetDatabase { get; set; } = string.Empty;
        public int BatchSize { get; set; }
        public int NotifyAfterRows { get; set; }
        public int TimeoutSeconds { get; set; }
        public bool EnableStreaming { get; set; }
    }

    /// <summary>
    /// 未对齐索引信息
    /// </summary>
    private sealed class UnalignedIndexInfo
    {
        public string IndexName { get; set; } = string.Empty;
        public string IndexType { get; set; } = string.Empty;
        public int IndexId { get; set; }
    }

    /// <summary>
    /// 索引详细信息（用于索引对齐）
    /// </summary>
    private sealed class IndexDetailsForAlign
    {
        public int IndexType { get; set; }
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }
        public string KeyColumns { get; set; } = string.Empty;
        public string? IncludedColumns { get; set; }
        public string? FilterDefinition { get; set; }
    }
}
