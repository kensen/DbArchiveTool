using DbArchiveTool.Domain.Entities;
using DbArchiveTool.Domain.ScheduledArchiveJobs;
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

                return ArchiveExecutionResult.CreateFailure(lastErrorMessage, stopwatch.Elapsed);
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

                // 计算下次执行时间(当前时间 + IntervalMinutes)
                var nextExecutionTime = DateTime.UtcNow.AddMinutes(job.IntervalMinutes);
                job.SetNextExecutionTime(nextExecutionTime, updatedBy: "SYSTEM");

                _logger.LogInformation(
                    "定时归档任务成功: JobId={JobId}, Name={Name}, 批次数={Batches}, 归档行数={Rows}, 耗时={Duration}ms, 下次执行={NextTime}",
                    jobId,
                    job.Name,
                    batchCount,
                    totalArchivedRows,
                    stopwatch.ElapsedMilliseconds,
                    nextExecutionTime);

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

            return ArchiveExecutionResult.CreateFailure(ex.Message, stopwatch.Elapsed);
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
            ArchiveMethod = job.ArchiveMethod,
            DeleteSourceDataAfterArchive = job.DeleteSourceDataAfterArchive,
            BatchSize = job.BatchSize
        };

        // 调用 ArchiveOrchestrationService 的参数重载方法
        return await _orchestrationService.ExecuteArchiveAsync(archiveParams, null, cancellationToken);
    }
}
