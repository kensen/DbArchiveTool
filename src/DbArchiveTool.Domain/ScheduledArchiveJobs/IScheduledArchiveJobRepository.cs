namespace DbArchiveTool.Domain.ScheduledArchiveJobs;

/// <summary>
/// 定时归档任务仓储接口
/// </summary>
public interface IScheduledArchiveJobRepository
{
    /// <summary>根据ID获取任务</summary>
    Task<ScheduledArchiveJob?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);

    /// <summary>获取所有任务</summary>
    Task<List<ScheduledArchiveJob>> GetAllAsync(CancellationToken cancellationToken = default);

    /// <summary>根据数据源ID查询任务列表</summary>
    Task<List<ScheduledArchiveJob>> GetByDataSourceIdAsync(Guid dataSourceId, CancellationToken cancellationToken = default);

    /// <summary>查询所有启用的任务</summary>
    Task<List<ScheduledArchiveJob>> GetEnabledJobsAsync(CancellationToken cancellationToken = default);

    /// <summary>查询指定数据源下启用的任务</summary>
    Task<List<ScheduledArchiveJob>> GetEnabledJobsByDataSourceAsync(Guid dataSourceId, CancellationToken cancellationToken = default);

    /// <summary>查询需要执行的任务(NextExecutionAtUtc &lt;= 当前时间)</summary>
    Task<List<ScheduledArchiveJob>> GetDueJobsAsync(DateTime currentTimeUtc, CancellationToken cancellationToken = default);

    /// <summary>根据名称查询任务(用于唯一性检查)</summary>
    Task<ScheduledArchiveJob?> GetByNameAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>检查任务名称是否已存在</summary>
    Task<bool> ExistsByNameAsync(string name, Guid? excludeId = null, CancellationToken cancellationToken = default);

    /// <summary>添加任务</summary>
    Task AddAsync(ScheduledArchiveJob job, CancellationToken cancellationToken = default);

    /// <summary>更新任务</summary>
    Task UpdateAsync(ScheduledArchiveJob job, CancellationToken cancellationToken = default);

    /// <summary>删除任务</summary>
    Task DeleteAsync(Guid id, CancellationToken cancellationToken = default);
}
