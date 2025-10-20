using System;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 分区执行任务的持久化接口。
/// </summary>
public interface IBackgroundTaskRepository
{
    /// <summary>新增执行任务。</summary>
    Task AddAsync(BackgroundTask task, CancellationToken cancellationToken = default);

    /// <summary>更新执行任务。</summary>
    Task UpdateAsync(BackgroundTask task, CancellationToken cancellationToken = default);

    /// <summary>根据标识获取任务。</summary>
    Task<BackgroundTask?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);

    /// <summary>判断指定数据源是否存在未完成（校验、排队、执行中）的任务。</summary>
    Task<bool> HasActiveTaskAsync(Guid dataSourceId, CancellationToken cancellationToken = default);

    /// <summary>列出最近的任务，默认按创建时间倒序。</summary>
    Task<IReadOnlyList<BackgroundTask>> ListRecentAsync(Guid? dataSourceId, int maxCount, CancellationToken cancellationToken = default);

    /// <summary>查找心跳超时的任务，便于恢复。</summary>
    Task<IReadOnlyList<BackgroundTask>> ListStaleAsync(TimeSpan heartbeatTimeout, CancellationToken cancellationToken = default);
}
