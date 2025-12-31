using DbArchiveTool.Domain.Entities;
using DbArchiveTool.Domain.ScheduledArchiveJobs;
using DbArchiveTool.Shared.Archive;
using Cronos;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace DbArchiveTool.Application.Services.ScheduledArchiveJobs;

/// <summary>
/// 定时归档任务执行器实现
/// 负责实际执行归档操作，并更新任务执行状态
/// </summary>
public sealed class ScheduledArchiveJobExecutor : IScheduledArchiveJobExecutor
{
    private readonly IScheduledArchiveJobRepository _jobRepository;
    private readonly Archives.ArchiveOrchestrationService _orchestrationService;
    private readonly ILogger<ScheduledArchiveJobExecutor> _logger;

    public ScheduledArchiveJobExecutor(
        IScheduledArchiveJobRepository jobRepository,
        Archives.ArchiveOrchestrationService orchestrationService,
        ILogger<ScheduledArchiveJobExecutor> logger)
    {
        _jobRepository = jobRepository;
        _orchestrationService = orchestrationService;
        _logger = logger;
    }

    /// <summary>
    /// 执行指定的定时归档任务
    /// </summary>
    public async Task<ArchiveExecutionResult> ExecuteAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        var executionTimeUtc = DateTime.UtcNow;

        try
        {
            _logger.LogInformation("开始执行定时归档任务: JobId={JobId}", jobId);

            // 步骤 1: 加载任务配置
            var job = await _jobRepository.GetByIdAsync(jobId, cancellationToken);
            if (job == null)
            {
                _logger.LogWarning("定时归档任务不存在: JobId={JobId}", jobId);
                return ArchiveExecutionResult.CreateFailure($"任务不存在: {jobId}", stopwatch.Elapsed);
            }

            if (!job.IsEnabled)
            {
                _logger.LogInformation("定时归档任务已禁用,跳过执行: JobId={JobId}, Name={Name}", jobId, job.Name);
                
                // 更新任务状态为跳过
                job.UpdateExecutionResult(
                    JobExecutionStatus.Skipped,
                    archivedRowCount: null,
                    errorMessage: "任务已禁用",
                    executionTimeUtc: executionTimeUtc,
                    updatedBy: "SYSTEM");
                
                await _jobRepository.UpdateAsync(job, cancellationToken);
                
                return ArchiveExecutionResult.CreateSkipped(stopwatch.Elapsed);
            }

            // 步骤 2: 更新任务状态为运行中
            job.UpdateExecutionResult(
                JobExecutionStatus.Running,
                archivedRowCount: null,
                errorMessage: null,
                executionTimeUtc: executionTimeUtc,
                updatedBy: "SYSTEM");

            // 定时归档任务专用于普通表归档：若历史数据写入了 PartitionSwitch/BCP，则在执行时自动纠正为 BulkCopy
            job.EnsureBulkCopyArchiveMethod(updatedBy: "SYSTEM");
            await _jobRepository.UpdateAsync(job, cancellationToken);

            _logger.LogInformation(
                "执行定时归档: Name={Name}, Source={SourceSchema}.{SourceTable}, Target={TargetSchema}.{TargetTable}, BatchSize={BatchSize}, MaxRowsPerExecution={MaxRows}",
                job.Name,
                job.SourceSchemaName,
                job.SourceTableName,
                job.TargetSchemaName,
                job.TargetTableName,
                job.BatchSize,
                job.MaxRowsPerExecution);

            // 步骤 3: 循环执行归档,直到达到 MaxRowsPerExecution 或无更多数据
            long totalArchivedRows = 0;
            string? lastErrorMessage = null;
            var batchCount = 0;
            var maxBatches = (int)Math.Ceiling((double)job.MaxRowsPerExecution / job.BatchSize);

            while (totalArchivedRows < job.MaxRowsPerExecution && batchCount < maxBatches)
            {
                batchCount++;

                _logger.LogDebug(
                    "执行批次 {Batch}/{MaxBatches}: JobId={JobId}, 已归档={TotalRows}/{MaxRows}",
                    batchCount,
                    maxBatches,
                    jobId,
                    totalArchivedRows,
                    job.MaxRowsPerExecution);

                var archiveResult = await ExecuteSingleBatchAsync(job, cancellationToken);

                if (!archiveResult.Success)
                {
                    // 批次失败 - 记录错误并退出循环
                    lastErrorMessage = archiveResult.Message;
                    _logger.LogError(
                        "批次归档失败: Batch={Batch}, JobId={JobId}, Error={Error}",
                        batchCount,
                        jobId,
                        archiveResult.Message);
                    break;
                }

                if (archiveResult.RowsArchived == 0)
                {
                    // 无更多数据可归档 - 正常退出
                    _logger.LogInformation(
                        "批次归档完成(无更多数据): Batch={Batch}, JobId={JobId}, 总归档行数={TotalRows}",
                        batchCount,
                        jobId,
                        totalArchivedRows);
                    break;
                }

                // 累加归档行数
                totalArchivedRows += archiveResult.RowsArchived;

                _logger.LogDebug(
                    "批次归档成功: Batch={Batch}, 本批次行数={BatchRows}, 累计行数={TotalRows}",
                    batchCount,
                    archiveResult.RowsArchived,
                    totalArchivedRows);
            }

            stopwatch.Stop();

            // 步骤 4: 根据归档结果更新任务状态
            if (!string.IsNullOrEmpty(lastErrorMessage))
            {
                // 有批次失败
                job.UpdateExecutionResult(
                    JobExecutionStatus.Failed,
                    archivedRowCount: totalArchivedRows > 0 ? totalArchivedRows : null,
                    errorMessage: lastErrorMessage,
                    executionTimeUtc: executionTimeUtc,
                    updatedBy: "SYSTEM");

                _logger.LogError(
                    "定时归档任务失败: JobId={JobId}, Name={Name}, 已归档行数={Rows}, 错误={Error}, 连续失败次数={FailureCount}/{MaxFailures}, 耗时={Duration}ms",
                    jobId,
                    job.Name,
                    totalArchivedRows,
                    lastErrorMessage,
                    job.ConsecutiveFailureCount,
                    job.MaxConsecutiveFailures,
                    stopwatch.ElapsedMilliseconds);

                await _jobRepository.UpdateAsync(job, cancellationToken);

                // 关键：必须抛异常，才能让 Hangfire 将本次执行标记为 Failed（否则只返回失败结果会被当作 Succeeded）
                throw new InvalidOperationException($"定时归档任务失败: {lastErrorMessage}");
            }
            else if (totalArchivedRows == 0)
            {
                // 无数据可归档,状态为跳过
                job.UpdateExecutionResult(
                    JobExecutionStatus.Skipped,
                    archivedRowCount: 0,
                    errorMessage: null,
                    executionTimeUtc: executionTimeUtc,
                    updatedBy: "SYSTEM");

                _logger.LogInformation(
                    "定时归档任务完成(无数据): JobId={JobId}, Name={Name}, 耗时={Duration}ms",
                    jobId,
                    job.Name,
                    stopwatch.ElapsedMilliseconds);

                await _jobRepository.UpdateAsync(job, cancellationToken);

                return ArchiveExecutionResult.CreateSkipped(stopwatch.Elapsed);
            }
            else
            {
                // 有数据归档成功
                job.UpdateExecutionResult(
                    JobExecutionStatus.Success,
                    archivedRowCount: totalArchivedRows,
                    errorMessage: null,
                    executionTimeUtc: executionTimeUtc,
                    updatedBy: "SYSTEM");

                // 计算并更新下次执行时间（用于界面展示，需与 Hangfire 调度语义一致）
                UpdateNextExecutionTimeForDisplay(job);

                _logger.LogInformation(
                    "定时归档任务成功: JobId={JobId}, Name={Name}, 批次数={Batches}, 归档行数={Rows}, 耗时={Duration}ms, 下次执行={NextTime}",
                    jobId,
                    job.Name,
                    batchCount,
                    totalArchivedRows,
                    stopwatch.ElapsedMilliseconds,
                    job.NextExecutionAtUtc);

                await _jobRepository.UpdateAsync(job, cancellationToken);

                return ArchiveExecutionResult.CreateSuccess(totalArchivedRows, stopwatch.Elapsed);
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();

            _logger.LogError(ex, "定时归档任务执行异常: JobId={JobId}, 耗时={Duration}ms", jobId, stopwatch.ElapsedMilliseconds);

            // 尝试更新任务状态为失败
            try
            {
                var job = await _jobRepository.GetByIdAsync(jobId, cancellationToken);
                if (job != null)
                {
                    job.UpdateExecutionResult(
                        JobExecutionStatus.Failed,
                        archivedRowCount: null,
                        errorMessage: ex.Message,
                        executionTimeUtc: executionTimeUtc,
                        updatedBy: "SYSTEM");

                    await _jobRepository.UpdateAsync(job, cancellationToken);
                }
            }
            catch (Exception updateEx)
            {
                _logger.LogError(updateEx, "更新任务失败状态时发生异常: JobId={JobId}", jobId);
            }

            // 关键：向上抛出，让 Hangfire 标记为 Failed，并在监控页/仪表盘展示异常原因
            throw;
        }
    }

    /// <summary>
    /// 执行单个批次的归档操作
    /// </summary>
    private async Task<Archives.ArchiveExecutionResult> ExecuteSingleBatchAsync(
        ScheduledArchiveJob job,
        CancellationToken cancellationToken)
    {
        // 构建归档参数对象
        var archiveParams = new Archives.ArchiveParameters
        {
            DataSourceId = job.DataSourceId,
            SourceSchemaName = job.SourceSchemaName,
            SourceTableName = job.SourceTableName,
            TargetSchemaName = job.TargetSchemaName,
            TargetTableName = job.TargetTableName,
            ArchiveFilterColumn = job.ArchiveFilterColumn,
            ArchiveFilterCondition = job.ArchiveFilterCondition,
            ArchiveMethod = ArchiveMethod.BulkCopy,
            DeleteSourceDataAfterArchive = job.DeleteSourceDataAfterArchive,
            BatchSize = job.BatchSize
        };

        // 调用 ArchiveOrchestrationService 的参数重载方法
        return await _orchestrationService.ExecuteArchiveAsync(archiveParams, null, cancellationToken);
    }

    private void UpdateNextExecutionTimeForDisplay(ScheduledArchiveJob job)
    {
        // 若任务在执行结果更新后被自动禁用（达到最大连续失败次数），则不再显示下次执行
        if (!job.IsEnabled)
        {
            job.SetNextExecutionTime(null, updatedBy: "SYSTEM");
            return;
        }

        var cronExpression = !string.IsNullOrWhiteSpace(job.CronExpression)
            ? job.CronExpression
            : GenerateCronFromInterval(job.IntervalMinutes);

        try
        {
            var parts = cronExpression.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var format = parts.Length >= 6 ? CronFormat.IncludeSeconds : CronFormat.Standard;

            var expr = CronExpression.Parse(cronExpression, format);
            var next = expr.GetNextOccurrence(DateTime.UtcNow, TimeZoneInfo.Local, inclusive: false);

            job.SetNextExecutionTime(next, updatedBy: "SYSTEM");
        }
        catch (Exception ex)
        {
            // 不阻断执行结果入库，但记录错误
            _logger.LogError(ex, "计算下次执行时间失败: JobId={JobId}, Cron={Cron}", job.Id, cronExpression);
        }
    }

    private static string GenerateCronFromInterval(int intervalMinutes)
    {
        if (intervalMinutes <= 0)
        {
            return "* * * * *";
        }

        if (intervalMinutes == 1)
        {
            return "* * * * *";
        }
        else if (intervalMinutes < 60)
        {
            return $"*/{intervalMinutes} * * * *";
        }
        else if (intervalMinutes == 60)
        {
            return "0 * * * *";
        }
        else if (intervalMinutes < 1440)
        {
            var intervalHours = Math.Max(1, intervalMinutes / 60);
            return $"0 */{intervalHours} * * *";
        }
        else
        {
            return "0 0 * * *";
        }
    }
}
