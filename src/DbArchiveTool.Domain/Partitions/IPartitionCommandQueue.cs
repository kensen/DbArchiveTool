namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 分区命令队列接口,用于异步入队和出队分区命令标识。
/// </summary>
public interface IPartitionCommandQueue
{
    /// <summary>
    /// 将分区命令标识加入队列。
    /// </summary>
    /// <param name="commandId">分区命令唯一标识。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步任务。</returns>
    ValueTask EnqueueAsync(Guid commandId, CancellationToken cancellationToken = default);

    /// <summary>
    /// 从队列中异步枚举待执行的分区命令标识。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步可枚举的命令标识流。</returns>
    IAsyncEnumerable<Guid> DequeueAsync(CancellationToken cancellationToken = default);
}
