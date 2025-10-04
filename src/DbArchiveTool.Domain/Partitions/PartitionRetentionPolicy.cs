namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 表示分区保留策略的具体配置。
/// </summary>
public sealed record PartitionRetentionPolicy(PartitionRetentionType Type, int Value)
{
    /// <summary>创建保留最新 N 个分区的策略。</summary>
    public static PartitionRetentionPolicy KeepLatest(int partitions) => new(PartitionRetentionType.KeepLatestPartitions, partitions);

    /// <summary>创建按天数保留分区的策略。</summary>
    public static PartitionRetentionPolicy KeepDays(int days) => new(PartitionRetentionType.KeepDays, days);
}
