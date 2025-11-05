using DbArchiveTool.Application.Archives;
using Hangfire;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers.V1;

/// <summary>
/// Hangfire 归档任务调度 API 控制器
/// </summary>
[ApiController]
[Route("api/v1/archive-jobs")]
[Produces("application/json")]
public sealed class ArchiveJobsController : ControllerBase
{
    private readonly IBackgroundJobClient _backgroundJobClient;
    private readonly IRecurringJobManager _recurringJobManager;
    private readonly ILogger<ArchiveJobsController> _logger;

    public ArchiveJobsController(
        IBackgroundJobClient backgroundJobClient,
        IRecurringJobManager recurringJobManager,
        ILogger<ArchiveJobsController> logger)
    {
        _backgroundJobClient = backgroundJobClient;
        _recurringJobManager = recurringJobManager;
        _logger = logger;
    }

    /// <summary>
    /// 立即执行单个归档任务(后台异步)
    /// </summary>
    /// <param name="configurationId">归档配置ID</param>
    /// <returns>Hangfire 任务ID</returns>
    [HttpPost("execute/{configurationId}")]
    [ProducesResponseType(typeof(EnqueueJobResponse), 200)]
    public IActionResult EnqueueJob(Guid configurationId)
    {
        try
        {
            var jobId = _backgroundJobClient.Enqueue<IArchiveJobService>(
                service => service.ExecuteArchiveJobAsync(configurationId));

            _logger.LogInformation(
                "已将归档任务加入队列: ConfigId={ConfigId}, JobId={JobId}",
                configurationId,
                jobId);

            return Ok(new EnqueueJobResponse
            {
                JobId = jobId,
                ConfigurationId = configurationId,
                Message = "归档任务已加入后台队列"
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "加入归档任务失败: ConfigId={ConfigId}", configurationId);
            return StatusCode(500, new { message = "加入归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 批量执行归档任务(后台异步)
    /// </summary>
    /// <param name="request">批量任务请求</param>
    /// <returns>Hangfire 任务ID</returns>
    [HttpPost("execute-batch")]
    [ProducesResponseType(typeof(EnqueueJobResponse), 200)]
    public IActionResult EnqueueBatchJob([FromBody] BatchJobRequest request)
    {
        try
        {
            if (request.ConfigurationIds == null || request.ConfigurationIds.Count == 0)
            {
                return BadRequest(new { message = "配置ID列表不能为空" });
            }

            var jobId = _backgroundJobClient.Enqueue<IArchiveJobService>(
                service => service.ExecuteBatchArchiveJobAsync(request.ConfigurationIds));

            _logger.LogInformation(
                "已将批量归档任务加入队列: Count={Count}, JobId={JobId}",
                request.ConfigurationIds.Count,
                jobId);

            return Ok(new EnqueueJobResponse
            {
                JobId = jobId,
                Message = $"已将 {request.ConfigurationIds.Count} 个归档任务加入后台队列"
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "加入批量归档任务失败");
            return StatusCode(500, new { message = "加入批量归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 延迟执行归档任务
    /// </summary>
    /// <param name="configurationId">归档配置ID</param>
    /// <param name="delayMinutes">延迟分钟数</param>
    /// <returns>Hangfire 任务ID</returns>
    [HttpPost("schedule/{configurationId}")]
    [ProducesResponseType(typeof(EnqueueJobResponse), 200)]
    public IActionResult ScheduleJob(Guid configurationId, [FromQuery] int delayMinutes = 5)
    {
        try
        {
            var jobId = _backgroundJobClient.Schedule<IArchiveJobService>(
                service => service.ExecuteArchiveJobAsync(configurationId),
                TimeSpan.FromMinutes(delayMinutes));

            _logger.LogInformation(
                "已调度归档任务: ConfigId={ConfigId}, Delay={Delay}分钟, JobId={JobId}",
                configurationId,
                delayMinutes,
                jobId);

            return Ok(new EnqueueJobResponse
            {
                JobId = jobId,
                ConfigurationId = configurationId,
                Message = $"归档任务已调度,将在 {delayMinutes} 分钟后执行"
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "调度归档任务失败: ConfigId={ConfigId}", configurationId);
            return StatusCode(500, new { message = "调度归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 创建或更新定时归档任务
    /// </summary>
    /// <param name="request">定时任务请求</param>
    /// <returns>操作结果</returns>
    [HttpPost("recurring")]
    [ProducesResponseType(200)]
    public IActionResult CreateOrUpdateRecurringJob([FromBody] RecurringJobRequest request)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(request.JobId))
            {
                return BadRequest(new { message = "任务ID不能为空" });
            }

            if (string.IsNullOrWhiteSpace(request.CronExpression))
            {
                return BadRequest(new { message = "Cron表达式不能为空" });
            }

            if (request.ConfigurationId == Guid.Empty)
            {
                return BadRequest(new { message = "配置ID不能为空" });
            }

            _recurringJobManager.AddOrUpdate<IArchiveJobService>(
                request.JobId,
                "archive", // 队列名称
                service => service.ExecuteArchiveJobAsync(request.ConfigurationId),
                request.CronExpression,
                new RecurringJobOptions
                {
                    TimeZone = TimeZoneInfo.Local
                });

            _logger.LogInformation(
                "已创建/更新定时归档任务: JobId={JobId}, ConfigId={ConfigId}, Cron={Cron}",
                request.JobId,
                request.ConfigurationId,
                request.CronExpression);

            return Ok(new
            {
                message = "定时归档任务已创建/更新",
                jobId = request.JobId,
                cronExpression = request.CronExpression
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建/更新定时归档任务失败: JobId={JobId}", request.JobId);
            return StatusCode(500, new { message = "创建/更新定时归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 删除定时归档任务
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <returns>操作结果</returns>
    [HttpDelete("recurring/{jobId}")]
    [ProducesResponseType(200)]
    public IActionResult RemoveRecurringJob(string jobId)
    {
        try
        {
            _recurringJobManager.RemoveIfExists(jobId);

            _logger.LogInformation("已删除定时归档任务: JobId={JobId}", jobId);

            return Ok(new { message = "定时归档任务已删除", jobId });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "删除定时归档任务失败: JobId={JobId}", jobId);
            return StatusCode(500, new { message = "删除定时归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 立即触发定时任务执行
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <returns>操作结果</returns>
    [HttpPost("recurring/{jobId}/trigger")]
    [ProducesResponseType(200)]
    public IActionResult TriggerRecurringJob(string jobId)
    {
        try
        {
            _recurringJobManager.Trigger(jobId);

            _logger.LogInformation("已触发定时归档任务: JobId={JobId}", jobId);

            return Ok(new { message = "定时归档任务已触发", jobId });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "触发定时归档任务失败: JobId={JobId}", jobId);
            return StatusCode(500, new { message = "触发定时归档任务失败", error = ex.Message });
        }
    }
}

/// <summary>
/// 任务入队响应
/// </summary>
public sealed class EnqueueJobResponse
{
    /// <summary>Hangfire 任务ID</summary>
    public string JobId { get; set; } = string.Empty;

    /// <summary>归档配置ID(可选)</summary>
    public Guid? ConfigurationId { get; set; }

    /// <summary>响应消息</summary>
    public string Message { get; set; } = string.Empty;
}

/// <summary>
/// 批量任务请求
/// </summary>
public sealed class BatchJobRequest
{
    /// <summary>归档配置ID列表</summary>
    public List<Guid> ConfigurationIds { get; set; } = new();
}

/// <summary>
/// 定时任务请求
/// </summary>
public sealed class RecurringJobRequest
{
    /// <summary>任务ID(唯一标识)</summary>
    public string JobId { get; set; } = string.Empty;

    /// <summary>归档配置ID</summary>
    public Guid ConfigurationId { get; set; }

    /// <summary>Cron表达式</summary>
    public string CronExpression { get; set; } = string.Empty;
}
