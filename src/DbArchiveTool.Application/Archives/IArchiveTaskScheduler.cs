using DbArchiveTool.Domain.ArchiveConfigurations;

namespace DbArchiveTool.Application.Archives;

/// <summary>
/// 归档任务调度器接口,用于与 Hangfire 等后台调度框架同步归档配置。
/// </summary>
public interface IArchiveTaskScheduler
{
    /// <summary>
    /// 同步定时归档任务。
    /// </summary>
    /// <param name="configuration">归档配置实体</param>
    /// <param name="cancellationToken">取消令牌</param>
    Task SyncRecurringJobAsync(ArchiveConfiguration configuration, CancellationToken cancellationToken = default);

    /// <summary>
    /// 移除指定归档配置对应的定时任务。
    /// </summary>
    /// <param name="configurationId">归档配置标识</param>
    /// <param name="cancellationToken">取消令牌</param>
    Task RemoveRecurringJobAsync(Guid configurationId, CancellationToken cancellationToken = default);
}
