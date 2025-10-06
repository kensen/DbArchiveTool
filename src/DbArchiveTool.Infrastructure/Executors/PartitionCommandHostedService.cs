using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// HostedService，负责后台拉取并执行分区命令。
/// </summary>
internal sealed class PartitionCommandHostedService : BackgroundService
{
    private readonly IServiceProvider serviceProvider;
    private readonly PartitionCommandQueue queue;
    private readonly ILogger<PartitionCommandHostedService> logger;

    public PartitionCommandHostedService(IServiceProvider serviceProvider, PartitionCommandQueue queue, ILogger<PartitionCommandHostedService> logger)
    {
        this.serviceProvider = serviceProvider;
        this.queue = queue;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var commandId in queue.DequeueAsync(stoppingToken))
        {
            using var scope = serviceProvider.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IPartitionCommandRepository>();
            var command = await repository.GetByIdAsync(commandId, stoppingToken);
            if (command is null)
            {
                logger.LogWarning("Partition command {CommandId} not found", commandId);
                continue;
            }

            logger.LogInformation("Executing partition command {CommandId} ({Type})", commandId, command.CommandType);
            command.MarkExecuting("SYSTEM");
            await repository.UpdateAsync(command, stoppingToken);
        }
    }
}
