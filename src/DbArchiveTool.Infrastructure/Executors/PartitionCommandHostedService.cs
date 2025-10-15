using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 后台服务：监听命令队列并调用对应执行器完成分区操作。
/// </summary>
internal sealed class PartitionCommandHostedService : BackgroundService
{
    private readonly IServiceScopeFactory scopeFactory;
    private readonly IPartitionCommandQueue queue;
    private readonly ILogger<PartitionCommandHostedService> logger;

    public PartitionCommandHostedService(
        IServiceScopeFactory scopeFactory,
        IPartitionCommandQueue queue,
        ILogger<PartitionCommandHostedService> logger)
    {
        this.scopeFactory = scopeFactory;
        this.queue = queue;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await RecoverPendingCommandsAsync(stoppingToken);

        await foreach (var commandId in queue.DequeueAsync(stoppingToken))
        {
            await HandleCommandAsync(commandId, stoppingToken);
        }
    }

    private async Task RecoverPendingCommandsAsync(CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IPartitionCommandRepository>();
        var pendingCommands = await repository.ListPendingAsync(cancellationToken);
        if (pendingCommands.Count == 0)
        {
            return;
        }

        logger.LogInformation("恢复待执行分区命令 {Count} 条。", pendingCommands.Count);

        foreach (var command in pendingCommands)
        {
            if (command.Status == PartitionCommandStatus.Approved)
            {
                command.MarkQueued("SYSTEM");
                await repository.UpdateAsync(command, cancellationToken);
            }

            await queue.EnqueueAsync(command.Id, cancellationToken);
        }
    }

    private async Task HandleCommandAsync(Guid commandId, CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IPartitionCommandRepository>();
        var executors = scope.ServiceProvider.GetServices<IPartitionCommandExecutor>();

        var command = await repository.GetByIdAsync(commandId, cancellationToken);
        if (command is null)
        {
            logger.LogWarning("分区命令 {CommandId} 不存在，跳过。", commandId);
            return;
        }

        var executor = ResolveExecutor(executors, command.CommandType);
        if (executor is null)
        {
            logger.LogError("未找到命令类型 {CommandType} 的执行器，命令 {CommandId} 将被标记失败。", command.CommandType, command.Id);

            try
            {
                command.MarkExecuting("SYSTEM");
                command.MarkFailed("SYSTEM", $"未实现命令类型 {command.CommandType}", $"命令类型 {command.CommandType} 缺少执行器。");
                await repository.UpdateAsync(command, cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "标记命令 {CommandId} 失败。", command.Id);
            }

            return;
        }

        try
        {
            if (command.Status == PartitionCommandStatus.Approved)
            {
                command.MarkQueued("SYSTEM");
            }

            command.MarkExecuting("SYSTEM");
            await repository.UpdateAsync(command, cancellationToken);

            var result = await executor.ExecuteAsync(command, cancellationToken);

            if (result.IsSuccess)
            {
                var log = BuildSuccessLog(result);
                command.MarkSucceeded("SYSTEM", log);
                logger.LogInformation("命令 {CommandId} 执行成功：{Message}", command.Id, result.Message);
            }
            else
            {
                var errorDetail = string.IsNullOrWhiteSpace(result.ErrorDetail) ? result.Message : result.ErrorDetail;
                command.MarkFailed("SYSTEM", result.Message, errorDetail);
                logger.LogWarning("命令 {CommandId} 执行失败：{Message}", command.Id, result.Message);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "执行命令 {CommandId} 发生异常。", command.Id);
            try
            {
                command.MarkFailed("SYSTEM", ex.Message, ex.ToString());
            }
            catch (Exception markEx)
            {
                logger.LogError(markEx, "更新命令 {CommandId} 状态失败。", command.Id);
            }
        }
        finally
        {
            try
            {
                await repository.UpdateAsync(command, cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "保存命令 {CommandId} 状态失败。", command.Id);
            }
        }
    }

    private static IPartitionCommandExecutor? ResolveExecutor(IEnumerable<IPartitionCommandExecutor> executors, PartitionCommandType commandType)
        => executors.FirstOrDefault(x => x.CommandType == commandType);

    private static string BuildSuccessLog(SqlExecutionResult result)
    {
        return $"{result.Message} | 受影响分区/行={result.AffectedCount}, 耗时={result.ElapsedMilliseconds}ms";
    }
}
