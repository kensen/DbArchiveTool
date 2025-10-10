using System.Threading.Channels;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 分区执行任务的内存队列，用于协调后台 HostedService。
/// </summary>
internal sealed class PartitionExecutionQueue
{
    private readonly Channel<PartitionExecutionDispatch> channel;

    public PartitionExecutionQueue(int capacity = 20)
    {
        var options = new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        };

        channel = Channel.CreateBounded<PartitionExecutionDispatch>(options);
    }

    /// <summary>入队执行任务。</summary>
    public ValueTask EnqueueAsync(PartitionExecutionDispatch dispatch, CancellationToken cancellationToken)
        => channel.Writer.WriteAsync(dispatch, cancellationToken);

    /// <summary>后台消费任务。</summary>
    public IAsyncEnumerable<PartitionExecutionDispatch> DequeueAsync(CancellationToken cancellationToken)
        => channel.Reader.ReadAllAsync(cancellationToken);
}

/// <summary>
/// 队列中的派发消息，包含任务标识与数据源信息。
/// </summary>
internal readonly record struct PartitionExecutionDispatch(Guid ExecutionTaskId, Guid DataSourceId);
