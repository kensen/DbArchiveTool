using DbArchiveTool.Domain.ArchiveConfigurations;
using DbArchiveTool.Domain.DataSources;

namespace DbArchiveTool.Application.Archives;

/// <summary>
/// 归档执行器接口
/// </summary>
public interface IArchiveExecutor
{
    /// <summary>
    /// 执行归档
    /// </summary>
    /// <param name="config">归档配置</param>
    /// <param name="dataSource">数据源</param>
    /// <param name="targetConnectionString">目标连接字符串</param>
    /// <param name="partitionNumber">分区号(可选)</param>
    /// <param name="progressCallback">进度回调</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>归档结果</returns>
    Task<ArchiveExecutionResult> ExecuteAsync(
        ArchiveConfiguration config,
        ArchiveDataSource dataSource,
        string targetConnectionString,
        int? partitionNumber = null,
        Action<ArchiveProgressInfo>? progressCallback = null,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 归档进度信息
/// </summary>
public sealed class ArchiveProgressInfo
{
    /// <summary>进度消息</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>进度百分比 (0-100)</summary>
    public int ProgressPercentage { get; init; }

    /// <summary>已处理的行数</summary>
    public long RowsProcessed { get; init; }
}

/// <summary>
/// 归档执行结果
/// </summary>
public sealed class ArchiveExecutionResult
{
    /// <summary>是否成功</summary>
    public bool Success { get; init; }

    /// <summary>归档配置ID</summary>
    public Guid ConfigurationId { get; init; }

    /// <summary>归档配置名称</summary>
    public string? ConfigurationName { get; init; }

    /// <summary>源架构名</summary>
    public string? SourceSchemaName { get; init; }

    /// <summary>源表名</summary>
    public string? SourceTableName { get; init; }

    /// <summary>分区号</summary>
    public int? PartitionNumber { get; init; }

    /// <summary>归档方法</summary>
    public Shared.Archive.ArchiveMethod? ArchiveMethod { get; init; }

    /// <summary>归档的行数</summary>
    public long RowsArchived { get; init; }

    /// <summary>开始时间(UTC)</summary>
    public DateTime StartTimeUtc { get; init; }

    /// <summary>结束时间(UTC)</summary>
    public DateTime EndTimeUtc { get; init; }

    /// <summary>总耗时</summary>
    public TimeSpan Duration => EndTimeUtc - StartTimeUtc;

    /// <summary>结果消息</summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>错误详情</summary>
    public string? ErrorDetails { get; init; }
}

/// <summary>
/// 批量归档执行结果
/// </summary>
public sealed class BatchArchiveExecutionResult
{
    /// <summary>总任务数</summary>
    public int TotalTasks { get; init; }

    /// <summary>成功任务数</summary>
    public int SuccessCount { get; init; }

    /// <summary>失败任务数</summary>
    public int FailureCount { get; init; }

    /// <summary>总归档行数</summary>
    public long TotalRowsArchived { get; init; }

    /// <summary>各任务执行结果</summary>
    public IReadOnlyList<ArchiveExecutionResult> Results { get; init; } = Array.Empty<ArchiveExecutionResult>();

    /// <summary>开始时间(UTC)</summary>
    public DateTime StartTimeUtc { get; init; }

    /// <summary>结束时间(UTC)</summary>
    public DateTime EndTimeUtc { get; init; }

    /// <summary>总耗时</summary>
    public TimeSpan Duration { get; init; }
}
