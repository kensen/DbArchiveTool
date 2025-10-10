using System;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 承担分区执行任务校验与脚本生成的占位处理器。
/// </summary>
internal sealed class PartitionExecutionProcessor
{
    private readonly IPartitionExecutionTaskRepository taskRepository;
    private readonly IPartitionExecutionLogRepository logRepository;
    private readonly IPartitionConfigurationRepository configurationRepository;
    private readonly SqlPartitionCommandExecutor commandExecutor;
    private readonly ILogger<PartitionExecutionProcessor> logger;

    public PartitionExecutionProcessor(
        IPartitionExecutionTaskRepository taskRepository,
        IPartitionExecutionLogRepository logRepository,
        IPartitionConfigurationRepository configurationRepository,
        SqlPartitionCommandExecutor commandExecutor,
        ILogger<PartitionExecutionProcessor> logger)
    {
        this.taskRepository = taskRepository;
        this.logRepository = logRepository;
        this.configurationRepository = configurationRepository;
        this.commandExecutor = commandExecutor;
        this.logger = logger;
    }

    public async Task ExecuteAsync(Guid executionTaskId, CancellationToken cancellationToken)
    {
        var task = await taskRepository.GetByIdAsync(executionTaskId, cancellationToken);
        if (task is null)
        {
            logger.LogWarning("Partition execution task {TaskId} not found.", executionTaskId);
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            await AppendLogAsync(task.Id, "Info", "任务入队", $"任务由 {task.RequestedBy} 发起，开始校验。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(PartitionExecutionPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.05, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            await AppendLogAsync(task.Id, "Step", "校验配置", "正在检查配置草稿是否完整。", cancellationToken);

            var configuration = await configurationRepository.GetByIdAsync(task.PartitionConfigurationId, cancellationToken);
            if (configuration is null)
            {
                await HandleValidationFailureAsync(task, "未找到分区配置草稿。", cancellationToken);
                return;
            }

            if (configuration.Boundaries.Count == 0)
            {
                await HandleValidationFailureAsync(task, "分区配置中未提供任何边界。", cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "校验通过", $"检测到 {configuration.Boundaries.Count} 个目标边界，准备生成脚本。", cancellationToken);

            task.UpdateProgress(0.2, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            task.MarkQueued("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            await AppendLogAsync(task.Id, "Step", "进入队列", "校验完成，任务进入执行阶段。", cancellationToken);

            task.MarkRunning("SYSTEM");
            task.UpdateProgress(0.35, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var boundaryValues = configuration.Boundaries
                .OrderBy(b => b.SortKey, StringComparer.Ordinal)
                .Select(b => b.Value)
                .ToList();

            var script = commandExecutor.RenderSplitScript(configuration, boundaryValues);
            var scriptMeta = new { boundaryCount = boundaryValues.Count, scriptLength = script.Length, simulated = true };

            await AppendLogAsync(
                task.Id,
                "Step",
                "生成脚本",
                $"已根据配置生成 {boundaryValues.Count} 条 SPLIT 指令。目前仅记录脚本文本，尚未执行。",
                cancellationToken,
                extraJson: JsonSerializer.Serialize(scriptMeta));

            task.UpdateProgress(0.55, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            await AppendLogAsync(
                task.Id,
                "Info",
                "执行脚本(占位)",
                "当前实现尚未执行实际 SQL，仅记录脚本生成情况。",
                cancellationToken);

            task.UpdateProgress(0.8, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var summary = JsonSerializer.Serialize(new
            {
                boundaryCount = boundaryValues.Count,
                generatedScriptLength = script.Length,
                simulated = true,
                completedAt = DateTime.UtcNow
            });

            task.MarkSucceeded("SYSTEM", summary);
            await taskRepository.UpdateAsync(task, cancellationToken);

            await AppendLogAsync(
                task.Id,
                "Info",
                "任务完成",
                $"处理完成，耗时 {stopwatch.Elapsed:g}。当前阶段仅完成配置校验与脚本生成。",
                cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Partition execution task {TaskId} failed.", task.Id);
            await AppendLogAsync(task.Id, "Error", "执行异常", ex.ToString(), cancellationToken);

            if (task.Status is PartitionExecutionStatus.PendingValidation or PartitionExecutionStatus.Validating or PartitionExecutionStatus.Queued)
            {
                task.Cancel("SYSTEM", ex.Message);
            }
            else
            {
                task.MarkFailed("SYSTEM", ex.Message ?? "执行失败");
            }

            await taskRepository.UpdateAsync(task, cancellationToken);
        }
        finally
        {
            stopwatch.Stop();
        }
    }

    private async Task HandleValidationFailureAsync(PartitionExecutionTask task, string reason, CancellationToken cancellationToken)
    {
        await AppendLogAsync(task.Id, "Warning", "校验失败", reason, cancellationToken);
        task.Cancel("SYSTEM", reason);
        await taskRepository.UpdateAsync(task, cancellationToken);
    }

    private Task AppendLogAsync(
        Guid taskId,
        string category,
        string title,
        string message,
        CancellationToken cancellationToken,
        long? durationMs = null,
        string? extraJson = null)
    {
        var entry = PartitionExecutionLogEntry.Create(taskId, category, title, message, durationMs, extraJson);
        return logRepository.AddAsync(entry, cancellationToken);
    }
}
