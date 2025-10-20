using System.Text.Json.Serialization;

namespace DbArchiveTool.Shared.Partitions;

/// <summary>
/// 标识分区执行/归档任务的操作类型，便于任务调度平台与前端展示。
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum BackgroundTaskOperationType
{
    Unknown = 0,

    // 边界操作
    AddBoundary = 10,
    SplitBoundary = 11,
    MergeBoundary = 12,

    // 归档相关
    ArchiveSwitch = 30,
    ArchiveBcp = 31,
    ArchiveBulkCopy = 32,

    // 预留扩展
    Custom = 99
}
