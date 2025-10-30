namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 描述分区的当前行数等运行时指标。
/// </summary>
public sealed record PartitionRowStatistics(
    int PartitionNumber,
    long RowCount);
