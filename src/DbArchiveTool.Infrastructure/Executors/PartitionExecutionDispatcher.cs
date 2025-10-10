using System;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Application.Partitions;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 默认的分区执行派发实现，基于内存队列。
/// </summary>
internal sealed class PartitionExecutionDispatcher : IPartitionExecutionDispatcher
{
    private readonly PartitionExecutionQueue queue;

    public PartitionExecutionDispatcher(PartitionExecutionQueue queue)
    {
        this.queue = queue;
    }

    public Task DispatchAsync(Guid executionTaskId, Guid dataSourceId, CancellationToken cancellationToken = default)
    {
        var dispatch = new PartitionExecutionDispatch(executionTaskId, dataSourceId);
        return queue.EnqueueAsync(dispatch, cancellationToken).AsTask();
    }
}
