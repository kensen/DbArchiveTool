namespace DbArchiveTool.Domain.Partitions;

/// <summary>描述系统支持的数据归档方案类型。</summary>
public enum ArchiveMethodType
{
    /// <summary>通过分区切换方式完成归档。</summary>
    PartitionSwitch = 0,

    /// <summary>基于 BCP 命令行导入导出完成归档。</summary>
    Bcp = 1,

    /// <summary>基于流式批量写入的 BulkCopy 方案。</summary>
    BulkCopy = 2
}
