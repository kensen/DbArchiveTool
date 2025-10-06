using System.Threading.Channels;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 简单的内存队列实现，用于缓存待执行的分区命令标识，支持异步读写。
/// </summary>
internal sealed class PartitionCommandQueue
{
    private readonly Channel<Guid> channel;

    public PartitionCommandQueue(int capacity = 100)
    {
        var options = new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        };
        channel = Channel.CreateBounded<Guid>(options);
    }

    /// <summary>入队待执行命令。</summary>
    public ValueTask EnqueueAsync(Guid commandId, CancellationToken cancellationToken)
        => channel.Writer.WriteAsync(commandId, cancellationToken);

    /// <summary>异步枚举出口，供 HostedService 消费。</summary>
    public IAsyncEnumerable<Guid> DequeueAsync(CancellationToken cancellationToken)
        => channel.Reader.ReadAllAsync(cancellationToken);
}
