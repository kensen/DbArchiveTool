using System.Threading.Channels;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 分区执行任务的内存队列，用于协调后台 HostedService。
/// </summary>
internal sealed class BackgroundTaskQueue
{
    private readonly Channel<BackgroundTaskDispatch> channel;

    public BackgroundTaskQueue(int capacity = 20)
    {
        var options = new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        };

        channel = Channel.CreateBounded<BackgroundTaskDispatch>(options);
    }

    /// <summary>入队执行任务。</summary>
    public ValueTask EnqueueAsync(BackgroundTaskDispatch dispatch, CancellationToken cancellationToken)
        => channel.Writer.WriteAsync(dispatch, cancellationToken);

    /// <summary>后台消费任务。</summary>
    public IAsyncEnumerable<BackgroundTaskDispatch> DequeueAsync(CancellationToken cancellationToken)
        => channel.Reader.ReadAllAsync(cancellationToken);
}

/// <summary>
/// 队列中的派发消息，包含任务标识与数据源信息。
/// </summary>
internal readonly record struct BackgroundTaskDispatch(Guid ExecutionTaskId, Guid DataSourceId);
