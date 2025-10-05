namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 支持的分区命令类型。
/// </summary>
public enum PartitionCommandType
{
    Split = 1,
    Merge = 2,
    Switch = 3
}
