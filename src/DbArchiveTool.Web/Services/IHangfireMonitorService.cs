using DbArchiveTool.Web.Models;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// Hangfire 监控服务接口
/// </summary>
public interface IHangfireMonitorService
{
    /// <summary>
    /// 获取所有归档任务的执行状态
    /// </summary>
    /// <param name="status">状态筛选(可选): Enqueued, Scheduled, Processing, Succeeded, Failed, Deleted</param>
    /// <param name="pageIndex">页码(从0开始)</param>
    /// <param name="pageSize">每页数量</param>
    /// <returns>任务列表</returns>
    Task<PagedResult<HangfireJobModel>> GetJobsAsync(string? status = null, int pageIndex = 0, int pageSize = 20);

    /// <summary>
    /// 获取任务详情
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <returns>任务详情</returns>
    Task<HangfireJobDetailModel?> GetJobDetailAsync(string jobId);

    /// <summary>
    /// 获取定时任务列表
    /// </summary>
    /// <returns>定时任务列表</returns>
    Task<List<HangfireRecurringJobModel>> GetRecurringJobsAsync();

    /// <summary>
    /// 删除任务
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <returns>是否成功</returns>
    Task<bool> DeleteJobAsync(string jobId);

    /// <summary>
    /// 重新入队失败的任务
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <returns>是否成功</returns>
    Task<bool> RequeueJobAsync(string jobId);

    /// <summary>
    /// 触发定时任务立即执行
    /// </summary>
    /// <param name="recurringJobId">定时任务ID</param>
    /// <returns>是否成功</returns>
    Task<bool> TriggerRecurringJobAsync(string recurringJobId);

    /// <summary>
    /// 移除定时任务
    /// </summary>
    /// <param name="recurringJobId">定时任务ID</param>
    /// <returns>是否成功</returns>
    Task<bool> RemoveRecurringJobAsync(string recurringJobId);

    /// <summary>
    /// 获取统计信息
    /// </summary>
    /// <returns>统计信息</returns>
    Task<HangfireStatisticsModel> GetStatisticsAsync();
}
