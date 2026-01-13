using DbArchiveTool.Domain.ScheduledArchiveJobs;
using Cronos;
using Hangfire;
using Hangfire.Common;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Scheduling;

/// <summary>
/// 定时归档任务调度器实现(基于 Hangfire)
/// </summary>
public sealed class ScheduledArchiveJobScheduler : Application.Services.ScheduledArchiveJobs.IScheduledArchiveJobScheduler
{
    private readonly IScheduledArchiveJobRepository _jobRepository;
    private readonly IRecurringJobManager _recurringJobManager;
    private readonly IBackgroundJobClient _backgroundJobClient;
    private readonly ILogger<ScheduledArchiveJobScheduler> _logger;
    private const string RecurringJobIdPrefix = "scheduled-archive-job-";

    public ScheduledArchiveJobScheduler(
        IScheduledArchiveJobRepository jobRepository,
        IRecurringJobManager recurringJobManager,
        IBackgroundJobClient backgroundJobClient,
        ILogger<ScheduledArchiveJobScheduler> logger)
    {
        _jobRepository = jobRepository;
        _recurringJobManager = recurringJobManager;
        _backgroundJobClient = backgroundJobClient;
        _logger = logger;
    }

    /// <summary>
    /// 注册所有启用的定时归档任务到调度器
    /// </summary>
    public async Task<int> RegisterAllJobsAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("开始注册所有启用的定时归档任务到 Hangfire");

        var enabledJobs = await _jobRepository.GetEnabledJobsAsync(cancellationToken);
        
        var registeredCount = 0;
        foreach (var job in enabledJobs)
        {
            try
            {
                await RegisterJobAsync(job.Id, cancellationToken);
                registeredCount++;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "注册定时归档任务失败: JobId={JobId}, Name={Name}", job.Id, job.Name);
            }
        }

        _logger.LogInformation("完成注册定时归档任务: 成功={Success}, 总数={Total}", registeredCount, enabledJobs.Count);
        return registeredCount;
    }

    /// <summary>
    /// 注册单个定时归档任务到调度器
    /// </summary>
    public async Task RegisterJobAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        var job = await _jobRepository.GetByIdAsync(jobId, cancellationToken);
        if (job == null)
        {
            _logger.LogWarning("注册任务失败: 任务不存在 JobId={JobId}", jobId);
            return;
        }

        if (!job.IsEnabled)
        {
            _logger.LogInformation("跳过注册已禁用的任务: JobId={JobId}, Name={Name}", jobId, job.Name);
            return;
        }

        var recurringJobId = GetRecurringJobId(jobId);

        // 构建 Cron 表达式
        string cronExpression;
        if (!string.IsNullOrWhiteSpace(job.CronExpression))
        {
            // 使用用户提供的 Cron 表达式
            cronExpression = job.CronExpression;
            _logger.LogInformation(
                "注册定时归档任务(Cron): JobId={JobId}, Name={Name}, Cron={Cron}",
                jobId,
                job.Name,
                cronExpression);
        }
        else
        {
            // 根据 IntervalMinutes 生成 Cron 表达式
            cronExpression = GenerateCronFromInterval(job.IntervalMinutes);
            _logger.LogInformation(
                "注册定时归档任务(间隔): JobId={JobId}, Name={Name}, IntervalMinutes={Interval}, Cron={Cron}",
                jobId,
                job.Name,
                job.IntervalMinutes,
                cronExpression);
        }

        // 注册到 Hangfire RecurringJob
        // 说明：不要使用静态 RecurringJob 或其扩展方法（可能内部仍依赖 JobStorage.Current）。
        // 这里直接构造 Job 并调用 IRecurringJobManager.AddOrUpdate，确保可在应用启动早期安全执行。
        var hangfireJob = Job.FromExpression<Application.Services.ScheduledArchiveJobs.IScheduledArchiveJobExecutor>(
            executor => executor.ExecuteAsync(jobId, CancellationToken.None));

        _recurringJobManager.AddOrUpdate(
            recurringJobId,
            hangfireJob,
            cronExpression,
            new RecurringJobOptions
            {
                // Cron 表达式按“本地时间语义”解释，与 UI 侧预览一致
                TimeZone = TimeZoneInfo.Local
            });

        // 计算并更新下次执行时间
        var nextExecutionTime = CalculateNextExecutionTime(cronExpression);
        if (nextExecutionTime.HasValue)
        {
            job.SetNextExecutionTime(nextExecutionTime.Value, updatedBy: "SYSTEM");
            await _jobRepository.UpdateAsync(job, cancellationToken);

            _logger.LogInformation(
                "任务注册成功,下次执行时间: {NextTime}",
                nextExecutionTime.Value);
        }

        _logger.LogInformation("已注册定时归档任务到 Hangfire: RecurringJobId={RecurringJobId}", recurringJobId);
    }

    /// <summary>
    /// 从调度器中移除定时归档任务
    /// </summary>
    public Task UnregisterJobAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        var recurringJobId = GetRecurringJobId(jobId);
        
        _logger.LogInformation("从 Hangfire 移除定时归档任务: JobId={JobId}, RecurringJobId={RecurringJobId}", jobId, recurringJobId);
        
        _recurringJobManager.RemoveIfExists(recurringJobId);
        
        return Task.CompletedTask;
    }

    /// <summary>
    /// 更新任务的调度配置
    /// </summary>
    public async Task UpdateJobScheduleAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("更新定时归档任务调度配置: JobId={JobId}", jobId);

        // 先移除旧的调度
        await UnregisterJobAsync(jobId, cancellationToken);

        // 重新注册(如果任务仍然启用)
        var job = await _jobRepository.GetByIdAsync(jobId, cancellationToken);
        if (job != null && job.IsEnabled)
        {
            await RegisterJobAsync(jobId, cancellationToken);
        }
    }

    /// <summary>
    /// 立即触发任务执行(不等待下次调度时间)
    /// </summary>
    public async Task TriggerJobAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        var job = await _jobRepository.GetByIdAsync(jobId, cancellationToken);
        if (job == null)
        {
            _logger.LogWarning("立即执行任务失败: 任务不存在 JobId={JobId}", jobId);
            return;
        }

        _logger.LogInformation("立即触发定时归档任务: JobId={JobId}, Name={Name}", jobId, job.Name);

        // 使用 Hangfire BackgroundJob 立即执行
        // 说明：避免使用静态 BackgroundJob（依赖 JobStorage.Current），以免在某些启动阶段或测试环境下异常。
        var backgroundJobId = _backgroundJobClient.Enqueue<Application.Services.ScheduledArchiveJobs.IScheduledArchiveJobExecutor>(
            executor => executor.ExecuteAsync(jobId, CancellationToken.None));

        _logger.LogInformation(
            "已触发任务立即执行: JobId={JobId}, BackgroundJobId={BackgroundJobId}",
            jobId,
            backgroundJobId);
    }

    /// <summary>
    /// 获取 Hangfire RecurringJob ID
    /// </summary>
    private static string GetRecurringJobId(Guid jobId)
    {
        return $"{RecurringJobIdPrefix}{jobId}";
    }

    /// <summary>
    /// 根据间隔分钟数生成 Cron 表达式
    /// </summary>
    private string GenerateCronFromInterval(int intervalMinutes)
    {
        // Cron 表达式的最小粒度是1分钟
        
        if (intervalMinutes <= 0)
        {
            _logger.LogWarning("间隔分钟数无效: {Minutes},使用默认值1分钟", intervalMinutes);
            return Cron.Minutely(); // "* * * * *"
        }

        if (intervalMinutes == 1)
        {
            return Cron.Minutely(); // 每分钟: "* * * * *"
        }
        else if (intervalMinutes < 60)
        {
            // 每 N 分钟执行一次: "*/N * * * *"
            return $"*/{intervalMinutes} * * * *";
        }
        else if (intervalMinutes == 60)
        {
            return Cron.Hourly(); // 每小时: "0 * * * *"
        }
        else if (intervalMinutes < 1440) // < 24小时
        {
            var intervalHours = intervalMinutes / 60;
            return $"0 */{intervalHours} * * *"; // 每 N 小时: "0 */N * * *"
        }
        else
        {
            return Cron.Daily(); // 每天: "0 0 * * *"
        }
    }

    /// <summary>
    /// 计算 Cron 表达式的下次执行时间
    /// </summary>
    private DateTime? CalculateNextExecutionTime(string cronExpression)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(cronExpression))
            {
                return null;
            }

            // Cron 表达式按“本地时间语义”解释，但 Cronos 要求 fromUtc 必须为 UTC
            var nowUtc = DateTime.UtcNow;

            var parts = cronExpression.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var format = parts.Length >= 6 ? CronFormat.IncludeSeconds : CronFormat.Standard;

            var expr = CronExpression.Parse(cronExpression, format);
            var next = expr.GetNextOccurrence(nowUtc, TimeZoneInfo.Local, inclusive: false);
            return next;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "计算下次执行时间失败: CronExpression={Cron}", cronExpression);
            return null;
        }
    }
}
