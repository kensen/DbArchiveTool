using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 后台消费分区执行任务的 HostedService。
/// </summary>
internal sealed class PartitionExecutionHostedService : BackgroundService
{
    private readonly IServiceProvider serviceProvider;
    private readonly PartitionExecutionQueue queue;
    private readonly ILogger<PartitionExecutionHostedService> logger;
    private readonly ConcurrentDictionary<Guid, SemaphoreSlim> locks = new();

    public PartitionExecutionHostedService(
        IServiceProvider serviceProvider,
        PartitionExecutionQueue queue,
        ILogger<PartitionExecutionHostedService> logger)
    {
        this.serviceProvider = serviceProvider;
        this.queue = queue;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var dispatch in queue.DequeueAsync(stoppingToken))
        {
            var semaphore = locks.GetOrAdd(dispatch.DataSourceId, _ => new SemaphoreSlim(1, 1));
            await semaphore.WaitAsync(stoppingToken);

            _ = ProcessDispatchAsync(dispatch, semaphore, stoppingToken);
        }
    }

    private async Task ProcessDispatchAsync(PartitionExecutionDispatch dispatch, SemaphoreSlim semaphore, CancellationToken stoppingToken)
    {
        try
        {
            using var scope = serviceProvider.CreateScope();
            var processor = scope.ServiceProvider.GetRequiredService<PartitionExecutionProcessor>();
            await processor.ExecuteAsync(dispatch.ExecutionTaskId, stoppingToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process execution dispatch {TaskId}", dispatch.ExecutionTaskId);
        }
        finally
        {
            semaphore.Release();
        }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var semaphore in locks.Values)
        {
            semaphore.Dispose();
        }

        locks.Clear();
        return base.StopAsync(cancellationToken);
    }
}
