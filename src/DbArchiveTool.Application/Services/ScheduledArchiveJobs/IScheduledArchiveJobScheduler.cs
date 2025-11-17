namespace DbArchiveTool.Application.Services.ScheduledArchiveJobs;

/// <summary>
/// 定时归档任务调度器服务接口
/// 负责将任务注册到 Hangfire RecurringJob，并管理调度生命周期
/// </summary>
public interface IScheduledArchiveJobScheduler
{
    /// <summary>
    /// 注册所有启用的定时归档任务到调度器
    /// 通常在应用启动时调用
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>注册的任务数量</returns>
    Task<int> RegisterAllJobsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// 注册单个定时归档任务到调度器
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    Task RegisterJobAsync(Guid jobId, CancellationToken cancellationToken = default);

    /// <summary>
    /// 从调度器中移除定时归档任务
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    Task UnregisterJobAsync(Guid jobId, CancellationToken cancellationToken = default);

    /// <summary>
    /// 更新任务的调度配置
    /// 当任务的 IntervalSeconds 或 CronExpression 变更时调用
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    Task UpdateJobScheduleAsync(Guid jobId, CancellationToken cancellationToken = default);

    /// <summary>
    /// 立即触发任务执行(不等待下次调度时间)
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    Task TriggerJobAsync(Guid jobId, CancellationToken cancellationToken = default);
}
