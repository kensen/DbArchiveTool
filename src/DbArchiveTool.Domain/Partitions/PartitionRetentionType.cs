namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 标识分区保留策略的类型。
/// </summary>
public enum PartitionRetentionType
{
    /// <summary>不进行额外保留。</summary>
    None = 0,
    /// <summary>保留最新的若干分区。</summary>
    KeepLatestPartitions = 1,
    /// <summary>按照天数保留分区。</summary>
    KeepDays = 2
}
