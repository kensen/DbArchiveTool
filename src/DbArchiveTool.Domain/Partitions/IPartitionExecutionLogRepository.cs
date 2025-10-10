using System;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 分区执行日志持久化接口。
/// </summary>
public interface IPartitionExecutionLogRepository
{
    /// <summary>写入单条日志。</summary>
    Task AddAsync(PartitionExecutionLogEntry entry, CancellationToken cancellationToken = default);

    /// <summary>批量写入日志。</summary>
    Task AddRangeAsync(IReadOnlyCollection<PartitionExecutionLogEntry> entries, CancellationToken cancellationToken = default);

    /// <summary>按时间顺序获取日志。</summary>
    Task<IReadOnlyList<PartitionExecutionLogEntry>> ListAsync(Guid executionTaskId, DateTime? sinceUtc, int take, CancellationToken cancellationToken = default);

    /// <summary>获取最新一条日志。</summary>
    Task<PartitionExecutionLogEntry?> GetLatestAsync(Guid executionTaskId, CancellationToken cancellationToken = default);
}
