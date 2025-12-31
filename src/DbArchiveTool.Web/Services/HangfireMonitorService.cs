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
    private static readonly string[] SupportedQueues = new[] { "default", "archive" };

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

            // 用于处理“入队任务按多个队列聚合”的额外列表
            var extraItems = new List<HangfireJobModel>();

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
                    // 归档任务可能在 default 或 archive 队列，为了监控一致性这里同时聚合两者
                    long total = 0;
                    foreach (var q in SupportedQueues)
                    {
                        var list = api.EnqueuedJobs(q, from, count);
                        if (list != null)
                        {
                            extraItems.AddRange(list.Select(j => new HangfireJobModel
                            {
                                JobId = j.Key,
                                Status = "Enqueued",
                                CreatedAtUtc = j.Value.EnqueuedAt ?? DateTime.UtcNow,
                                MethodName = GetMethodName(j.Value.Job),
                                Arguments = SerializeJobArguments(j.Value.Job),
                                QueueName = q
                            }));
                        }

                        total += api.EnqueuedCount(q);
                    }

                    enqueuedJobs = null;
                    result.TotalCount = total;
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
                    // 默认也聚合 default+archive，避免监控页统计与列表不一致
                    foreach (var q in SupportedQueues)
                    {
                        var list = api.EnqueuedJobs(q, 0, 10);
                        if (list != null)
                        {
                            extraItems.AddRange(list.Select(j => new HangfireJobModel
                            {
                                JobId = j.Key,
                                Status = "Enqueued",
                                CreatedAtUtc = j.Value.EnqueuedAt ?? DateTime.UtcNow,
                                MethodName = GetMethodName(j.Value.Job),
                                Arguments = SerializeJobArguments(j.Value.Job),
                                QueueName = q
                            }));
                        }
                    }
                    enqueuedJobs = null;
                    scheduledJobs = api.ScheduledJobs(0, 10);
                    processingJobs = api.ProcessingJobs(0, 10);
                    succeededJobs = api.SucceededJobs(0, 10);
                    failedJobs = api.FailedJobs(0, 10);
                    result.TotalCount = SupportedQueues.Sum(q => api.EnqueuedCount(q)) + api.ScheduledCount() + 
                                       api.ProcessingCount() + api.SucceededListCount() + api.FailedCount();
                    break;
            }

            if (extraItems.Count > 0)
            {
                result.Items.AddRange(extraItems);
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
                    Arguments = SerializeJobArguments(j.Value.Job),
                    QueueName = null
                }));
            }

            if (scheduledJobs != null)
            {
                result.Items.AddRange(scheduledJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Scheduled",
                    // Hangfire 的 ScheduledJobDto 不一定有 CreatedAt，这里用 ScheduledAt 作为列表排序/显示基准
                    CreatedAtUtc = j.Value.ScheduledAt ?? DateTime.UtcNow,
                    ScheduledAtUtc = j.Value.ScheduledAt,
                    MethodName = GetMethodName(j.Value.Job),
                    Arguments = SerializeJobArguments(j.Value.Job),
                    QueueName = null
                }));
            }

            if (processingJobs != null)
            {
                result.Items.AddRange(processingJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Processing",
                    // Processing 列表同样缺少 CreatedAt，优先使用 StartedAt
                    CreatedAtUtc = j.Value.StartedAt ?? DateTime.UtcNow,
                    StartedAtUtc = j.Value.StartedAt,
                    DurationSeconds = j.Value.StartedAt.HasValue
                        ? Math.Max(0, (DateTime.UtcNow - DateTime.SpecifyKind(j.Value.StartedAt.Value, DateTimeKind.Utc)).TotalSeconds)
                        : null,
                    MethodName = GetMethodName(j.Value.Job),
                    Arguments = SerializeJobArguments(j.Value.Job),
                    QueueName = null
                }));
            }

            if (succeededJobs != null)
            {
                result.Items.AddRange(succeededJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Succeeded",
                    // Succeeded 列表同样缺少 CreatedAt，优先使用完成时间
                    CreatedAtUtc = j.Value.SucceededAt ?? DateTime.UtcNow,
                    FinishedAtUtc = j.Value.SucceededAt,
                    // Hangfire 的 TotalDuration 单位为毫秒，这里换算为秒用于展示
                    DurationSeconds = j.Value.TotalDuration.HasValue ? j.Value.TotalDuration.Value / 1000d : null,
                    MethodName = GetMethodName(j.Value.Job),
                    Arguments = SerializeJobArguments(j.Value.Job)
                }));
            }

            if (failedJobs != null)
            {
                result.Items.AddRange(failedJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Failed",
                    // Failed 列表同样缺少 CreatedAt，优先使用失败时间
                    CreatedAtUtc = j.Value.FailedAt ?? DateTime.UtcNow,
                    FinishedAtUtc = j.Value.FailedAt,
                    FailureReason = j.Value.ExceptionMessage,
                    MethodName = GetMethodName(j.Value.Job),
                    Arguments = SerializeJobArguments(j.Value.Job)
                }));
            }

            if (deletedJobs != null)
            {
                result.Items.AddRange(deletedJobs.Select(j => new HangfireJobModel
                {
                    JobId = j.Key,
                    Status = "Deleted",
                    CreatedAtUtc = j.Value.DeletedAt ?? DateTime.UtcNow,
                    FinishedAtUtc = j.Value.DeletedAt,
                    MethodName = GetMethodName(j.Value.Job),
                    Arguments = SerializeJobArguments(j.Value.Job)
                }));
            }

            // 按创建时间倒序排列
            result.Items = result.Items.OrderByDescending(j => j.CreatedAtUtc).ToList();

            return result;
        });
    }

    private string? SerializeJobArguments(Hangfire.Common.Job? job)
    {
        try
        {
            if (job?.Args == null || job.Args.Count == 0)
            {
                return null;
            }

            var args = job.Args.Select(a => a?.ToString() ?? "null").ToList();
            return JsonSerializer.Serialize(args);
        }
        catch
        {
            return null;
        }
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
                MethodName = GetMethodName(jobDetails.Job),
                Arguments = SerializeJobArguments(jobDetails.Job)
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
                    if (latestState.Data.TryGetValue("FailedAt", out var failedAt) && DateTime.TryParse(failedAt, out var failedAtUtc))
                        model.FinishedAtUtc = failedAtUtc;
                    if (latestState.Data.TryGetValue("SucceededAt", out var succeededAt) && DateTime.TryParse(succeededAt, out var succeededAtUtc))
                        model.FinishedAtUtc = succeededAtUtc;
                    if (latestState.Data.TryGetValue("StartedAt", out var startedAt) && DateTime.TryParse(startedAt, out var startedAtUtc))
                        model.StartedAtUtc = startedAtUtc;
                    if (latestState.Data.TryGetValue("ScheduledAt", out var scheduledAt) && DateTime.TryParse(scheduledAt, out var scheduledAtUtc))
                        model.ScheduledAtUtc = scheduledAtUtc;
                    if (latestState.Data.TryGetValue("ExceptionMessage", out var exception))
                        model.FailureReason = exception;

                    // 尝试从状态数据解析队列
                    if (latestState.Data.TryGetValue("Queue", out var queue))
                        model.QueueName = queue;
                }

                // 解析重试次数：取状态历史中 RetryCount 的最大值
                var retryCounts = model.StateHistory
                    .SelectMany(h => h.Data)
                    .Where(kv => string.Equals(kv.Key, "RetryCount", StringComparison.OrdinalIgnoreCase))
                    .Select(kv => int.TryParse(kv.Value, out var v) ? v : (int?)null)
                    .Where(v => v.HasValue)
                    .Select(v => v!.Value)
                    .ToList();

                if (retryCounts.Count > 0)
                {
                    model.RetryCount = retryCounts.Max();
                }
            }

            // 若缺少耗时但有开始/结束时间，则用时间差补齐
            if (!model.DurationSeconds.HasValue && model.StartedAtUtc.HasValue && model.FinishedAtUtc.HasValue)
            {
                model.DurationSeconds = Math.Max(0, (model.FinishedAtUtc.Value - model.StartedAtUtc.Value).TotalSeconds);
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
