using Hangfire;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using DbArchiveTool.Web.Models;
using System.Text.Json;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// Hangfire 监控服务实现
/// 提供对 Hangfire 任务的查询和管理功能
/// </summary>
public class HangfireMonitorService : IHangfireMonitorService
{
    private readonly ILogger<HangfireMonitorService> _logger;

    public HangfireMonitorService(ILogger<HangfireMonitorService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// 获取监控 API
    /// </summary>
    private IMonitoringApi GetMonitoringApi()
    {
        return JobStorage.Current.GetMonitoringApi();
    }

    public async Task<PagedResult<HangfireJobModel>> GetJobsAsync(string? status = null, int pageIndex = 0, int pageSize = 20)
    {
        return await Task.Run(() =>
        {
            var api = GetMonitoringApi();
            var result = new PagedResult<HangfireJobModel>
            {
                PageIndex = pageIndex,
                PageSize = pageSize
            };

            JobList<EnqueuedJobDto>? enqueuedJobs = null;
            JobList<ScheduledJobDto>? scheduledJobs = null;
            JobList<ProcessingJobDto>? processingJobs = null;
            JobList<SucceededJobDto>? succeededJobs = null;
            JobList<FailedJobDto>? failedJobs = null;
            JobList<DeletedJobDto>? deletedJobs = null;

            var from = pageIndex * pageSize;
            var count = pageSize;

            // 根据状态筛选获取任务
            switch (status?.ToLower())
            {
                case "enqueued":
                    enqueuedJobs = api.EnqueuedJobs("archive", from, count);
                    result.TotalCount = api.EnqueuedCount("archive");
                    break;
                case "scheduled":
                    scheduledJobs = api.ScheduledJobs(from, count);
                    result.TotalCount = api.ScheduledCount();
                    break;
                case "processing":
                    processingJobs = api.ProcessingJobs(from, count);
                    result.TotalCount = api.ProcessingCount();
                    break;
                case "succeeded":
                    succeededJobs = api.SucceededJobs(from, count);
                    result.TotalCount = api.SucceededListCount();
                    break;
                case "failed":
                    failedJobs = api.FailedJobs(from, count);
                    result.TotalCount = api.FailedCount();
                    break;
                case "deleted":
                    deletedJobs = api.DeletedJobs(from, count);
                    result.TotalCount = api.DeletedListCount();
                    break;
                default:
                    // 获取所有状态的任务
                    enqueuedJobs = api.EnqueuedJobs("archive", 0, 10);
                    scheduledJobs = api.ScheduledJobs(0, 10);
                    processingJobs = api.ProcessingJobs(0, 10);
                    succeededJobs = api.SucceededJobs(0, 10);
                    failedJobs = api.FailedJobs(0, 10);
                    result.TotalCount = api.EnqueuedCount("archive") + api.ScheduledCount() + 
                                       api.ProcessingCount() + api.SucceededListCount() + api.FailedCount();
                    break;
            }

            // 转换为统一的模型
            if (enqueuedJobs != null)
            {
                result.Items.AddRange(enqueuedJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Enqueued",
                    CreatedAtUtc = j.Value.EnqueuedAt ?? DateTime.UtcNow,
                    MethodName = GetMethodName(j.Value.Job),
                    QueueName = j.Value.InEnqueuedState ? "archive" : null
                }));
            }

            if (scheduledJobs != null)
            {
                result.Items.AddRange(scheduledJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Scheduled",
                    CreatedAtUtc = DateTime.UtcNow,
                    ScheduledAtUtc = j.Value.ScheduledAt,
                    MethodName = GetMethodName(j.Value.Job),
                    QueueName = j.Value.InScheduledState ? "archive" : null
                }));
            }

            if (processingJobs != null)
            {
                result.Items.AddRange(processingJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Processing",
                    CreatedAtUtc = DateTime.UtcNow,
                    StartedAtUtc = j.Value.StartedAt,
                    MethodName = GetMethodName(j.Value.Job),
                    QueueName = j.Value.InProcessingState ? "archive" : null
                }));
            }

            if (succeededJobs != null)
            {
                result.Items.AddRange(succeededJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Succeeded",
                    CreatedAtUtc = DateTime.UtcNow,
                    FinishedAtUtc = j.Value.SucceededAt,
                    DurationSeconds = j.Value.TotalDuration,
                    MethodName = GetMethodName(j.Value.Job)
                }));
            }

            if (failedJobs != null)
            {
                result.Items.AddRange(failedJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Failed",
                    CreatedAtUtc = DateTime.UtcNow,
                    FinishedAtUtc = j.Value.FailedAt,
                    FailureReason = j.Value.ExceptionMessage,
                    MethodName = GetMethodName(j.Value.Job)
                }));
            }

            if (deletedJobs != null)
            {
                result.Items.AddRange(deletedJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Deleted",
                    CreatedAtUtc = DateTime.UtcNow,
                    FinishedAtUtc = j.Value.DeletedAt,
                    MethodName = GetMethodName(j.Value.Job)
                }));
            }

            // 按创建时间倒序排列
            result.Items = result.Items.OrderByDescending(j => j.CreatedAtUtc).ToList();

            return result;
        });
    }

    public async Task<HangfireJobDetailModel?> GetJobDetailAsync(string jobId)
    {
        return await Task.Run(() =>
        {
            var api = GetMonitoringApi();
            var jobDetails = api.JobDetails(jobId);
            
            if (jobDetails == null)
                return null;

            var model = new HangfireJobDetailModel
            {
                JobId = jobId,
                CreatedAtUtc = jobDetails.CreatedAt ?? DateTime.UtcNow,
                MethodName = GetMethodName(jobDetails.Job)
            };

            // 获取状态历史
            if (jobDetails.History != null)
            {
                model.StateHistory = jobDetails.History.Select(h => new HangfireJobStateHistoryModel
                {
                    StateName = h.StateName,
                    Reason = h.Reason,
                    CreatedAtUtc = h.CreatedAt,
                    Data = new Dictionary<string, string>(h.Data ?? new Dictionary<string, string>())
                }).OrderByDescending(h => h.CreatedAtUtc).ToList();

                // 从最新状态获取任务状态
                var latestState = model.StateHistory.FirstOrDefault();
                if (latestState != null)
                {
                    model.Status = latestState.StateName;
                    
                    // 提取特定状态的信息
                    if (latestState.Data.TryGetValue("FailedAt", out var failedAt))
                        model.FinishedAtUtc = DateTime.Parse(failedAt);
                    if (latestState.Data.TryGetValue("SucceededAt", out var succeededAt))
                        model.FinishedAtUtc = DateTime.Parse(succeededAt);
                    if (latestState.Data.TryGetValue("StartedAt", out var startedAt))
                        model.StartedAtUtc = DateTime.Parse(startedAt);
                    if (latestState.Data.TryGetValue("ScheduledAt", out var scheduledAt))
                        model.ScheduledAtUtc = DateTime.Parse(scheduledAt);
                    if (latestState.Data.TryGetValue("ExceptionMessage", out var exception))
                        model.FailureReason = exception;
                }
            }

            // 解析参数
            if (jobDetails.Job?.Args != null)
            {
                for (int i = 0; i < jobDetails.Job.Args.Count; i++)
                {
                    var arg = jobDetails.Job.Args[i];
                    var key = $"Arg{i}";
                    var value = arg?.ToString() ?? "null";
                    
                    model.ParsedArguments[key] = value;
                    
                    // 如果是 GUID,尝试识别为配置ID
                    if (Guid.TryParse(value, out var guid))
                    {
                        model.ParsedArguments[$"{key}_Type"] = "ConfigurationId";
                    }
                }
            }

            return model;
        });
    }

    public async Task<List<HangfireRecurringJobModel>> GetRecurringJobsAsync()
    {
        return await Task.Run(() =>
        {
            using var connection = JobStorage.Current.GetConnection();
            var recurringJobs = connection.GetRecurringJobs();

            return recurringJobs.Select(j => new HangfireRecurringJobModel
            {
                Id = j.Id,
                Cron = j.Cron,
                MethodName = GetMethodName(j.Job),
                QueueName = j.Queue,
                NextExecutionUtc = j.NextExecution,
                LastJobId = j.LastJobId,
                LastExecutionUtc = j.LastExecution,
                CreatedAtUtc = j.CreatedAt ?? DateTime.UtcNow,
                Error = j.Error
            }).ToList();
        });
    }

    public async Task<bool> DeleteJobAsync(string jobId)
    {
        return await Task.Run(() =>
        {
            try
            {
                BackgroundJob.Delete(jobId);
                _logger.LogInformation("已删除 Hangfire 任务: {JobId}", jobId);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "删除 Hangfire 任务失败: {JobId}", jobId);
                return false;
            }
        });
    }

    public async Task<bool> RequeueJobAsync(string jobId)
    {
        return await Task.Run(() =>
        {
            try
            {
                BackgroundJob.Requeue(jobId);
                _logger.LogInformation("已重新入队 Hangfire 任务: {JobId}", jobId);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "重新入队 Hangfire 任务失败: {JobId}", jobId);
                return false;
            }
        });
    }

    public async Task<bool> TriggerRecurringJobAsync(string recurringJobId)
    {
        return await Task.Run(() =>
        {
            try
            {
                RecurringJob.TriggerJob(recurringJobId);
                _logger.LogInformation("已触发定时任务: {RecurringJobId}", recurringJobId);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "触发定时任务失败: {RecurringJobId}", recurringJobId);
                return false;
            }
        });
    }

    public async Task<bool> RemoveRecurringJobAsync(string recurringJobId)
    {
        return await Task.Run(() =>
        {
            try
            {
                RecurringJob.RemoveIfExists(recurringJobId);
                _logger.LogInformation("已移除定时任务: {RecurringJobId}", recurringJobId);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "移除定时任务失败: {RecurringJobId}", recurringJobId);
                return false;
            }
        });
    }

    public async Task<HangfireStatisticsModel> GetStatisticsAsync()
    {
        return await Task.Run(() =>
        {
            var api = GetMonitoringApi();
            var stats = api.GetStatistics();

            return new HangfireStatisticsModel
            {
                EnqueuedCount = (int)stats.Enqueued,
                ScheduledCount = (int)stats.Scheduled,
                ProcessingCount = (int)stats.Processing,
                SucceededCount = (int)stats.Succeeded,
                FailedCount = (int)stats.Failed,
                DeletedCount = (int)stats.Deleted,
                RecurringJobCount = (int)stats.Recurring,
                ServerCount = (int)stats.Servers
            };
        });
    }

    /// <summary>
    /// 获取方法名称
    /// </summary>
    private string GetMethodName(Hangfire.Common.Job? job)
    {
        if (job == null)
            return "Unknown";

        var typeName = job.Type.Name;
        var methodName = job.Method.Name;
        return $"{typeName}.{methodName}";
    }
}
