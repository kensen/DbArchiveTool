namespace DbArchiveTool.Application.Archives;

/// <summary>
/// 归档任务服务接口(用于Hangfire调度)
/// </summary>
public interface IArchiveJobService
{
    /// <summary>
    /// 执行单个归档任务
    /// </summary>
    /// <param name="configurationId">归档配置ID</param>
    /// <returns>任务结果</returns>
    Task<ArchiveExecutionResult> ExecuteArchiveJobAsync(Guid configurationId);

    /// <summary>
    /// 批量执行归档任务
    /// </summary>
    /// <param name="configurationIds">归档配置ID列表</param>
    /// <returns>批量任务结果</returns>
    Task<BatchArchiveExecutionResult> ExecuteBatchArchiveJobAsync(List<Guid> configurationIds);

    /// <summary>
    /// 执行所有启用的归档任务
    /// </summary>
    /// <returns>批量任务结果</returns>
    Task<BatchArchiveExecutionResult> ExecuteAllEnabledArchiveJobsAsync();
}
