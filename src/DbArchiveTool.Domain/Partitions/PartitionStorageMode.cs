namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 表示分区数据的存放模式。
/// </summary>
public enum PartitionStorageMode
{
    /// <summary>使用表当前的主文件组，不额外创建文件。</summary>
    PrimaryFilegroup = 0,

    /// <summary>为分区创建单独的文件组与单个数据文件。</summary>
    DedicatedFilegroupSingleFile = 1
}

