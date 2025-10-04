namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 记录某个分区边界当前的安全状态，用于指导后续操作。
/// </summary>
public sealed record PartitionSafetySnapshot(
    string BoundaryKey,
    long RowCount,
    bool HasData,
    bool RequiresSwitchStaging,
    string? WarningMessage);
