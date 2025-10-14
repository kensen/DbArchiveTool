namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 表级统计信息，用于展示分区执行前的体量估算。
/// </summary>
public sealed record TableStatistics(
    bool TableExists,
    long TotalRows,
    decimal DataSizeMb,
    decimal IndexSizeMb,
    decimal TotalSizeMb)
{
    public static TableStatistics NotFound { get; } = new(false, 0, 0m, 0m, 0m);
}
