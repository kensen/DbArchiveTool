using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 后台消费分区执行任务的 HostedService，支持并发控制、僵尸任务恢复、心跳监控。
/// </summary>
internal sealed class PartitionExecutionHostedService : BackgroundService
{
    private readonly IServiceProvider serviceProvider;
    private readonly PartitionExecutionQueue queue;
    private readonly ILogger<PartitionExecutionHostedService> logger;
    private readonly ConcurrentDictionary<Guid, SemaphoreSlim> dataSourceLocks = new();
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> runningTasks = new();
    private Timer? heartbeatTimer;

    // 配置常量
    private const int HeartbeatIntervalSeconds = 30; // 心跳更新间隔
    private const int ZombieTaskThresholdMinutes = 5; // 僵尸任务阈值（超过5分钟无心跳）
    private const int MaxConcurrentTasksPerDataSource = 1; // 每个数据源的最大并发任务数

    public PartitionExecutionHostedService(
        IServiceProvider serviceProvider,
        PartitionExecutionQueue queue,
        ILogger<PartitionExecutionHostedService> logger)
    {
        this.serviceProvider = serviceProvider;
        this.queue = queue;
        this.logger = logger;
    }

    /// <summary>
    /// 启动后台服务：恢复僵尸任务 + 启动心跳定时器 + 开始消费队列
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("PartitionExecutionHostedService 正在启动...");

        try
        {
            // 1. 启动时恢复僵尸任务
            await RecoverZombieTasksAsync(stoppingToken);

            // 2. 启动心跳定时器
            StartHeartbeatTimer(stoppingToken);

            logger.LogInformation("PartitionExecutionHostedService 已启动，开始消费任务队列。");

            // 3. 持续消费队列
            await foreach (var dispatch in queue.DequeueAsync(stoppingToken))
            {
                if (stoppingToken.IsCancellationRequested)
                {
                    logger.LogWarning("检测到停止信号，停止消费任务队列。");
                    break;
                }

                // 获取或创建数据源级别的信号量（确保同一数据源任务串行执行）
                var semaphore = dataSourceLocks.GetOrAdd(
                    dispatch.DataSourceId,
                    _ => new SemaphoreSlim(MaxConcurrentTasksPerDataSource, MaxConcurrentTasksPerDataSource));

                // 尝试获取锁（非阻塞）
                var acquired = await semaphore.WaitAsync(0, stoppingToken);

                if (!acquired)
                {
                    logger.LogWarning(
                        "数据源 {DataSourceId} 已有任务正在执行，任务 {TaskId} 重新入队。",
                        dispatch.DataSourceId, dispatch.ExecutionTaskId);

                    // 重新入队（延迟5秒后重试）
                    _ = Task.Run(async () =>
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                        await queue.EnqueueAsync(dispatch, stoppingToken);
                    }, stoppingToken);

                    continue;
                }

                // 启动异步处理（Fire-and-Forget模式，通过 runningTasks 跟踪）
                var taskCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                runningTasks.TryAdd(dispatch.ExecutionTaskId, taskCts);

                _ = Task.Run(async () =>
                {
                    await ProcessDispatchAsync(dispatch, semaphore, taskCts.Token);
                    runningTasks.TryRemove(dispatch.ExecutionTaskId, out _);
                    taskCts.Dispose();
                }, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("PartitionExecutionHostedService 收到停止信号，正常退出。");
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "PartitionExecutionHostedService 发生严重错误！");
            throw;
        }
    }

    /// <summary>
    /// 恢复僵尸任务（启动时扫描处于执行中但心跳超时的任务）
    /// </summary>
    private async Task RecoverZombieTasksAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("开始扫描僵尸任务...");

        try
        {
            using var scope = serviceProvider.CreateScope();
            var taskRepository = scope.ServiceProvider.GetRequiredService<IPartitionExecutionTaskRepository>();
            var logRepository = scope.ServiceProvider.GetRequiredService<IPartitionExecutionLogRepository>();

            // 使用 ListStaleAsync 查询心跳超时的任务
            var zombieThreshold = TimeSpan.FromMinutes(ZombieTaskThresholdMinutes);
            var zombies = await taskRepository.ListStaleAsync(zombieThreshold, cancellationToken);

            if (zombies.Count == 0)
            {
                logger.LogInformation("未发现僵尸任务。");
                return;
            }

            logger.LogWarning("发现 {Count} 个僵尸任务，准备恢复...", zombies.Count);

            foreach (var zombie in zombies)
            {
                try
                {
                    // 记录恢复日志
                    var logEntry = PartitionExecutionLogEntry.Create(
                        zombie.Id,
                        "Warning",
                        "僵尸任务恢复",
                        $"检测到任务在 {zombie.LastHeartbeatUtc:yyyy-MM-dd HH:mm:ss} UTC 之后无心跳，标记为失败并重新入队。");

                    await logRepository.AddAsync(logEntry, cancellationToken);

                    // 标记任务失败
                    zombie.MarkFailed(
                        "SYSTEM",
                        $"任务超时无响应（最后心跳：{zombie.LastHeartbeatUtc:yyyy-MM-dd HH:mm:ss} UTC），已自动标记失败。");

                    await taskRepository.UpdateAsync(zombie, cancellationToken);

                    logger.LogWarning(
                        "僵尸任务 {TaskId} 已标记为失败（最后心跳：{LastHeartbeat}）。",
                        zombie.Id, zombie.LastHeartbeatUtc);

                    // 可选：重新入队（根据业务需求决定是否自动重试）
                    // await queue.EnqueueAsync(new PartitionExecutionDispatch(zombie.Id, zombie.DataSourceId), cancellationToken);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "恢复僵尸任务 {TaskId} 失败。", zombie.Id);
                }
            }

            logger.LogInformation("僵尸任务恢复完成，共处理 {Count} 个任务。", zombies.Count);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "扫描僵尸任务失败！");
        }
    }

    /// <summary>
    /// 启动心跳定时器（定期更新运行中任务的心跳时间）
    /// </summary>
    private void StartHeartbeatTimer(CancellationToken stoppingToken)
    {
        heartbeatTimer = new Timer(
            async _ => await UpdateHeartbeatsAsync(stoppingToken),
            null,
            TimeSpan.FromSeconds(HeartbeatIntervalSeconds),
            TimeSpan.FromSeconds(HeartbeatIntervalSeconds));

        logger.LogInformation("心跳定时器已启动，间隔 {Interval} 秒。", HeartbeatIntervalSeconds);
    }

    /// <summary>
    /// 更新所有运行中任务的心跳时间
    /// </summary>
    private async Task UpdateHeartbeatsAsync(CancellationToken cancellationToken)
    {
        if (runningTasks.IsEmpty)
        {
            return;
        }

        try
        {
            using var scope = serviceProvider.CreateScope();
            var taskRepository = scope.ServiceProvider.GetRequiredService<IPartitionExecutionTaskRepository>();

            var taskIds = runningTasks.Keys.ToList();
            var updated = 0;

            foreach (var taskId in taskIds)
            {
                try
                {
                    var task = await taskRepository.GetByIdAsync(taskId, cancellationToken);
                    if (task is not null && !task.IsCompleted)
                    {
                        task.UpdateHeartbeat("SYSTEM");
                        await taskRepository.UpdateAsync(task, cancellationToken);
                        updated++;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "更新任务 {TaskId} 心跳失败。", taskId);
                }
            }

            if (updated > 0)
            {
                logger.LogDebug("心跳更新完成，更新了 {Count} 个任务。", updated);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "批量更新心跳失败！");
        }
    }

    /// <summary>
    /// 处理单个任务派发
    /// </summary>
    private async Task ProcessDispatchAsync(
        PartitionExecutionDispatch dispatch,
        SemaphoreSlim semaphore,
        CancellationToken cancellationToken)
    {
        var taskId = dispatch.ExecutionTaskId;

        try
        {
            logger.LogInformation(
                "开始处理分区执行任务：{TaskId}（数据源：{DataSourceId}）",
                taskId, dispatch.DataSourceId);

            using var scope = serviceProvider.CreateScope();
            var processor = scope.ServiceProvider.GetRequiredService<PartitionExecutionProcessor>();

            await processor.ExecuteAsync(taskId, cancellationToken);

            logger.LogInformation("分区执行任务 {TaskId} 处理完成。", taskId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning("任务 {TaskId} 被取消。", taskId);

            // 尝试标记任务为已取消
            try
            {
                using var scope = serviceProvider.CreateScope();
                var taskRepository = scope.ServiceProvider.GetRequiredService<IPartitionExecutionTaskRepository>();
                var task = await taskRepository.GetByIdAsync(taskId, CancellationToken.None);

                if (task is not null && !task.IsCompleted)
                {
                    task.Cancel("SYSTEM", "服务停止，任务被取消。");
                    await taskRepository.UpdateAsync(task, CancellationToken.None);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "标记任务 {TaskId} 为已取消失败。", taskId);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "处理分区执行任务 {TaskId} 失败！", taskId);

            // 尝试标记任务为失败
            try
            {
                using var scope = serviceProvider.CreateScope();
                var taskRepository = scope.ServiceProvider.GetRequiredService<IPartitionExecutionTaskRepository>();
                var logRepository = scope.ServiceProvider.GetRequiredService<IPartitionExecutionLogRepository>();

                var task = await taskRepository.GetByIdAsync(taskId, CancellationToken.None);

                if (task is not null && !task.IsCompleted)
                {
                    // 记录错误日志
                    var logEntry = PartitionExecutionLogEntry.Create(
                        taskId,
                        "Error",
                        "HostedService 异常",
                        $"后台服务处理任务时发生未捕获异常：{ex.Message}");

                    await logRepository.AddAsync(logEntry, CancellationToken.None);

                    task.MarkFailed("SYSTEM", $"后台服务异常：{ex.Message}");
                    await taskRepository.UpdateAsync(task, CancellationToken.None);
                }
            }
            catch (Exception innerEx)
            {
                logger.LogError(innerEx, "标记任务 {TaskId} 为失败时发生异常。", taskId);
            }
        }
        finally
        {
            // 释放数据源级别的锁
            semaphore.Release();

            logger.LogDebug("数据源 {DataSourceId} 的锁已释放。", dispatch.DataSourceId);
        }
    }

    /// <summary>
    /// 停止服务时的清理工作
    /// </summary>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("PartitionExecutionHostedService 正在停止...");

        // 停止心跳定时器
        heartbeatTimer?.Change(Timeout.Infinite, 0);
        heartbeatTimer?.Dispose();

        // 取消所有运行中的任务
        foreach (var (taskId, cts) in runningTasks.ToList())
        {
            logger.LogWarning("取消运行中的任务：{TaskId}", taskId);
            cts.Cancel();
        }

        // 等待所有任务完成（最多等待30秒）
        var timeout = Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
        var allCompleted = Task.Run(async () =>
        {
            while (!runningTasks.IsEmpty && !cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(100, cancellationToken);
            }
        }, cancellationToken);

        await Task.WhenAny(allCompleted, timeout);

        if (!runningTasks.IsEmpty)
        {
            logger.LogWarning("仍有 {Count} 个任务未完成，强制停止。", runningTasks.Count);
        }

        // 释放所有锁
        foreach (var semaphore in dataSourceLocks.Values)
        {
            semaphore.Dispose();
        }

        dataSourceLocks.Clear();
        runningTasks.Clear();

        logger.LogInformation("PartitionExecutionHostedService 已停止。");

        await base.StopAsync(cancellationToken);
    }
}
