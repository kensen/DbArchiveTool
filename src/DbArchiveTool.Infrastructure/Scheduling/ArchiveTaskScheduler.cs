using DbArchiveTool.Application.Archives;
using DbArchiveTool.Domain.ArchiveConfigurations;
using Hangfire;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Scheduling;

/// <summary>
/// 使用 Hangfire 实现的归档任务调度器。
/// </summary>
internal sealed class ArchiveTaskScheduler : IArchiveTaskScheduler
{
    private readonly IRecurringJobManager _recurringJobManager;
    private readonly ILogger<ArchiveTaskScheduler> _logger;

    public ArchiveTaskScheduler(
        IRecurringJobManager recurringJobManager,
        ILogger<ArchiveTaskScheduler> logger)
    {
        _recurringJobManager = recurringJobManager;
        _logger = logger;
    }

    /// <inheritdoc />
    public Task SyncRecurringJobAsync(ArchiveConfiguration configuration, CancellationToken cancellationToken = default)
    {
        if (configuration == null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        var jobId = BuildJobId(configuration.Id);

        try
        {
            if (!configuration.IsEnabled || !configuration.EnableScheduledArchive || string.IsNullOrWhiteSpace(configuration.CronExpression))
            {
                _recurringJobManager.RemoveIfExists(jobId);
                _logger.LogDebug("归档配置定时任务已移除: ConfigId={ConfigId}", configuration.Id);
                return Task.CompletedTask;
            }

            _recurringJobManager.AddOrUpdate<IArchiveJobService>(
                jobId,
                service => service.ExecuteArchiveJobAsync(configuration.Id),
                configuration.CronExpression!,
                new RecurringJobOptions
                {
                    TimeZone = TimeZoneInfo.Utc
                });

            _logger.LogInformation(
                "归档配置定时任务已同步: ConfigId={ConfigId}, Cron={Cron}",
                configuration.Id,
                configuration.CronExpression);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "同步归档配置定时任务失败: ConfigId={ConfigId}", configuration.Id);
            throw;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task RemoveRecurringJobAsync(Guid configurationId, CancellationToken cancellationToken = default)
    {
        var jobId = BuildJobId(configurationId);

        try
        {
            _recurringJobManager.RemoveIfExists(jobId);
            _logger.LogDebug("归档配置定时任务已移除: ConfigId={ConfigId}", configurationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "移除归档配置定时任务失败: ConfigId={ConfigId}", configurationId);
            throw;
        }

        return Task.CompletedTask;
    }

    private static string BuildJobId(Guid configurationId) => $"archive-config-{configurationId:N}";
}
