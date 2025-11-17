using DbArchiveTool.Shared.Archive;

namespace DbArchiveTool.Application.Archives;

/// <summary>
/// 归档参数
/// 用于不依赖 ArchiveConfiguration 表的归档执行场景(如定时归档任务)
/// </summary>
public sealed class ArchiveParameters
{
    /// <summary>
    /// 数据源ID
    /// </summary>
    public required Guid DataSourceId { get; init; }

    /// <summary>
    /// 源表架构名
    /// </summary>
    public required string SourceSchemaName { get; init; }

    /// <summary>
    /// 源表名
    /// </summary>
    public required string SourceTableName { get; init; }

    /// <summary>
    /// 目标表架构名
    /// </summary>
    public required string TargetSchemaName { get; init; }

    /// <summary>
    /// 目标表名
    /// </summary>
    public required string TargetTableName { get; init; }

    /// <summary>
    /// 归档过滤列名
    /// </summary>
    public required string ArchiveFilterColumn { get; init; }

    /// <summary>
    /// 归档过滤条件
    /// </summary>
    public required string ArchiveFilterCondition { get; init; }

    /// <summary>
    /// 归档方法
    /// </summary>
    public required ArchiveMethod ArchiveMethod { get; init; }

    /// <summary>
    /// 归档后是否删除源数据
    /// </summary>
    public required bool DeleteSourceDataAfterArchive { get; init; }

    /// <summary>
    /// 批次大小
    /// </summary>
    public required int BatchSize { get; init; }
}
