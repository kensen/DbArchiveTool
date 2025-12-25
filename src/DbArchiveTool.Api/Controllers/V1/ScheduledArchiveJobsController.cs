using DbArchiveTool.Api.DTOs.Archives;
using DbArchiveTool.Application.Services.ScheduledArchiveJobs;
using DbArchiveTool.Domain.Entities;
using DbArchiveTool.Domain.ScheduledArchiveJobs;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers.V1;

/// <summary>
/// 定时归档任务 API 控制器
/// </summary>
[ApiController]
[Route("api/v1/scheduled-archive-jobs")]
[Produces("application/json")]
public sealed class ScheduledArchiveJobsController : ControllerBase
{
    private readonly IScheduledArchiveJobRepository _repository;
    private readonly IScheduledArchiveJobScheduler _scheduler;
    private readonly ILogger<ScheduledArchiveJobsController> _logger;

    public ScheduledArchiveJobsController(
        IScheduledArchiveJobRepository repository,
        IScheduledArchiveJobScheduler scheduler,
        ILogger<ScheduledArchiveJobsController> logger)
    {
        _repository = repository;
        _scheduler = scheduler;
        _logger = logger;
    }

    /// <summary>
    /// 获取所有定时归档任务列表
    /// </summary>
    /// <param name="dataSourceId">数据源ID(可选)</param>
    /// <param name="isEnabled">是否启用(可选)</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>定时归档任务列表</returns>
    [HttpGet]
    [ProducesResponseType(typeof(IEnumerable<ScheduledArchiveJobListItemDto>), 200)]
    public async Task<IActionResult> GetAll(
        [FromQuery] Guid? dataSourceId = null,
        [FromQuery] bool? isEnabled = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            List<ScheduledArchiveJob> jobs;

            if (dataSourceId.HasValue)
            {
                jobs = await _repository.GetByDataSourceIdAsync(dataSourceId.Value, cancellationToken);
            }
            else
            {
                jobs = await _repository.GetAllAsync(cancellationToken);
            }

            // 按启用状态过滤
            if (isEnabled.HasValue)
            {
                jobs = jobs.Where(j => j.IsEnabled == isEnabled.Value).ToList();
            }

            var dtos = jobs.Select(j => new ScheduledArchiveJobListItemDto
            {
                Id = j.Id,
                Name = j.Name,
                Description = j.Description,
                DataSourceId = j.DataSourceId,
                SourceSchemaName = j.SourceSchemaName,
                SourceTableName = j.SourceTableName,
                TargetSchemaName = j.TargetSchemaName,
                TargetTableName = j.TargetTableName,
                ArchiveMethod = j.ArchiveMethod,
                BatchSize = j.BatchSize,
                MaxRowsPerExecution = j.MaxRowsPerExecution,
                IntervalMinutes = j.IntervalMinutes,
                IsEnabled = j.IsEnabled,
                NextExecutionAtUtc = j.NextExecutionAtUtc,
                LastExecutionAtUtc = j.LastExecutionAtUtc,
                LastExecutionStatus = j.LastExecutionStatus,
                LastArchivedRowCount = j.LastArchivedRowCount,
                TotalExecutionCount = j.TotalExecutionCount,
                TotalArchivedRowCount = j.TotalArchivedRowCount,
                ConsecutiveFailureCount = j.ConsecutiveFailureCount,
                CreatedAtUtc = j.CreatedAtUtc,
                UpdatedAtUtc = j.UpdatedAtUtc
            });

            return Ok(dtos);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取定时归档任务列表失败");
            return StatusCode(500, new { message = "获取定时归档任务列表失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 根据ID获取定时归档任务详情
    /// </summary>
    /// <param name="id">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>定时归档任务详情</returns>
    [HttpGet("{id}")]
    [ProducesResponseType(typeof(ScheduledArchiveJobDetailDto), 200)]
    [ProducesResponseType(404)]
    public async Task<IActionResult> GetById(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            var job = await _repository.GetByIdAsync(id, cancellationToken);
            if (job == null)
            {
                return NotFound(new { message = $"定时归档任务不存在: {id}" });
            }

            var dto = new ScheduledArchiveJobDetailDto
            {
                Id = job.Id,
                Name = job.Name,
                Description = job.Description,
                DataSourceId = job.DataSourceId,
                SourceSchemaName = job.SourceSchemaName,
                SourceTableName = job.SourceTableName,
                TargetSchemaName = job.TargetSchemaName,
                TargetTableName = job.TargetTableName,
                ArchiveFilterColumn = job.ArchiveFilterColumn,
                ArchiveFilterCondition = job.ArchiveFilterCondition,
                ArchiveFilterDefinition = job.ArchiveFilterDefinition,
                ArchiveMethod = job.ArchiveMethod,
                DeleteSourceDataAfterArchive = job.DeleteSourceDataAfterArchive,
                BatchSize = job.BatchSize,
                MaxRowsPerExecution = job.MaxRowsPerExecution,
                IntervalMinutes = job.IntervalMinutes,
                CronExpression = job.CronExpression,
                IsEnabled = job.IsEnabled,
                NextExecutionAtUtc = job.NextExecutionAtUtc,
                LastExecutionAtUtc = job.LastExecutionAtUtc,
                LastExecutionStatus = job.LastExecutionStatus,
                LastExecutionError = job.LastExecutionError,
                LastArchivedRowCount = job.LastArchivedRowCount,
                TotalExecutionCount = job.TotalExecutionCount,
                TotalArchivedRowCount = job.TotalArchivedRowCount,
                ConsecutiveFailureCount = job.ConsecutiveFailureCount,
                MaxConsecutiveFailures = job.MaxConsecutiveFailures,
                CreatedAtUtc = job.CreatedAtUtc,
                CreatedBy = job.CreatedBy,
                UpdatedAtUtc = job.UpdatedAtUtc,
                UpdatedBy = job.UpdatedBy
            };

            return Ok(dto);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取定时归档任务详情失败: {Id}", id);
            return StatusCode(500, new { message = "获取定时归档任务详情失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 创建定时归档任务
    /// </summary>
    /// <param name="request">创建请求</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>创建的任务详情</returns>
    [HttpPost]
    [ProducesResponseType(typeof(ScheduledArchiveJobDetailDto), 201)]
    [ProducesResponseType(400)]
    public async Task<IActionResult> Create(
        [FromBody] CreateScheduledArchiveJobRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // 验证名称唯一性
            if (await _repository.ExistsByNameAsync(request.Name, null, cancellationToken))
            {
                return BadRequest(new { message = $"任务名称已存在: {request.Name}" });
            }

            // 创建任务
            var job = new ScheduledArchiveJob(
                name: request.Name,
                description: request.Description,
                dataSourceId: request.DataSourceId,
                sourceSchemaName: request.SourceSchemaName,
                sourceTableName: request.SourceTableName,
                targetSchemaName: request.TargetSchemaName,
                targetTableName: request.TargetTableName,
                archiveFilterColumn: request.ArchiveFilterColumn,
                archiveFilterCondition: request.ArchiveFilterCondition,
                archiveFilterDefinition: request.ArchiveFilterDefinition,
                archiveMethod: request.ArchiveMethod,
                deleteSourceDataAfterArchive: request.DeleteSourceDataAfterArchive,
                batchSize: request.BatchSize,
                maxRowsPerExecution: request.MaxRowsPerExecution,
                intervalMinutes: request.IntervalMinutes,
                cronExpression: request.CronExpression,
                maxConsecutiveFailures: request.MaxConsecutiveFailures,
                createdBy: "SYSTEM" // TODO: 从当前用户获取
            );

            await _repository.AddAsync(job, cancellationToken);

            _logger.LogInformation("成功创建定时归档任务: {Id} - {Name}", job.Id, job.Name);

            // 返回创建的任务详情
            var dto = new ScheduledArchiveJobDetailDto
            {
                Id = job.Id,
                Name = job.Name,
                Description = job.Description,
                DataSourceId = job.DataSourceId,
                SourceSchemaName = job.SourceSchemaName,
                SourceTableName = job.SourceTableName,
                TargetSchemaName = job.TargetSchemaName,
                TargetTableName = job.TargetTableName,
                ArchiveFilterColumn = job.ArchiveFilterColumn,
                ArchiveFilterCondition = job.ArchiveFilterCondition,
                ArchiveFilterDefinition = job.ArchiveFilterDefinition,
                ArchiveMethod = job.ArchiveMethod,
                DeleteSourceDataAfterArchive = job.DeleteSourceDataAfterArchive,
                BatchSize = job.BatchSize,
                MaxRowsPerExecution = job.MaxRowsPerExecution,
                IntervalMinutes = job.IntervalMinutes,
                CronExpression = job.CronExpression,
                IsEnabled = job.IsEnabled,
                NextExecutionAtUtc = job.NextExecutionAtUtc,
                LastExecutionAtUtc = job.LastExecutionAtUtc,
                LastExecutionStatus = job.LastExecutionStatus,
                LastExecutionError = job.LastExecutionError,
                LastArchivedRowCount = job.LastArchivedRowCount,
                TotalExecutionCount = job.TotalExecutionCount,
                TotalArchivedRowCount = job.TotalArchivedRowCount,
                ConsecutiveFailureCount = job.ConsecutiveFailureCount,
                MaxConsecutiveFailures = job.MaxConsecutiveFailures,
                CreatedAtUtc = job.CreatedAtUtc,
                CreatedBy = job.CreatedBy,
                UpdatedAtUtc = job.UpdatedAtUtc,
                UpdatedBy = job.UpdatedBy
            };

            return CreatedAtAction(nameof(GetById), new { id = job.Id }, dto);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(ex, "创建定时归档任务参数验证失败");
            return BadRequest(new { message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建定时归档任务失败");
            return StatusCode(500, new { message = "创建定时归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 更新定时归档任务
    /// </summary>
    /// <param name="id">任务ID</param>
    /// <param name="request">更新请求</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>更新后的任务详情</returns>
    [HttpPut("{id}")]
    [ProducesResponseType(typeof(ScheduledArchiveJobDetailDto), 200)]
    [ProducesResponseType(404)]
    [ProducesResponseType(400)]
    public async Task<IActionResult> Update(
        Guid id,
        [FromBody] UpdateScheduledArchiveJobRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var job = await _repository.GetByIdAsync(id, cancellationToken);
            if (job == null)
            {
                return NotFound(new { message = $"定时归档任务不存在: {id}" });
            }

            // 验证名称唯一性(排除当前任务)
            if (await _repository.ExistsByNameAsync(request.Name, id, cancellationToken))
            {
                return BadRequest(new { message = $"任务名称已存在: {request.Name}" });
            }

            // 更新任务
            job.Update(
                name: request.Name,
                description: request.Description,
                sourceSchemaName: request.SourceSchemaName,
                sourceTableName: request.SourceTableName,
                targetSchemaName: request.TargetSchemaName,
                targetTableName: request.TargetTableName,
                archiveFilterColumn: request.ArchiveFilterColumn,
                archiveFilterCondition: request.ArchiveFilterCondition,
                archiveFilterDefinition: request.ArchiveFilterDefinition,
                archiveMethod: request.ArchiveMethod,
                deleteSourceDataAfterArchive: request.DeleteSourceDataAfterArchive,
                batchSize: request.BatchSize,
                maxRowsPerExecution: request.MaxRowsPerExecution,
                intervalMinutes: request.IntervalMinutes,
                cronExpression: request.CronExpression,
                maxConsecutiveFailures: request.MaxConsecutiveFailures,
                updatedBy: "SYSTEM" // TODO: 从当前用户获取
            );

            await _repository.UpdateAsync(job, cancellationToken);

            _logger.LogInformation("成功更新定时归档任务: {Id} - {Name}", job.Id, job.Name);

            // 返回更新后的任务详情
            var dto = new ScheduledArchiveJobDetailDto
            {
                Id = job.Id,
                Name = job.Name,
                Description = job.Description,
                DataSourceId = job.DataSourceId,
                SourceSchemaName = job.SourceSchemaName,
                SourceTableName = job.SourceTableName,
                TargetSchemaName = job.TargetSchemaName,
                TargetTableName = job.TargetTableName,
                ArchiveFilterColumn = job.ArchiveFilterColumn,
                ArchiveFilterCondition = job.ArchiveFilterCondition,
                ArchiveFilterDefinition = job.ArchiveFilterDefinition,
                ArchiveMethod = job.ArchiveMethod,
                DeleteSourceDataAfterArchive = job.DeleteSourceDataAfterArchive,
                BatchSize = job.BatchSize,
                MaxRowsPerExecution = job.MaxRowsPerExecution,
                IntervalMinutes = job.IntervalMinutes,
                CronExpression = job.CronExpression,
                IsEnabled = job.IsEnabled,
                NextExecutionAtUtc = job.NextExecutionAtUtc,
                LastExecutionAtUtc = job.LastExecutionAtUtc,
                LastExecutionStatus = job.LastExecutionStatus,
                LastExecutionError = job.LastExecutionError,
                LastArchivedRowCount = job.LastArchivedRowCount,
                TotalExecutionCount = job.TotalExecutionCount,
                TotalArchivedRowCount = job.TotalArchivedRowCount,
                ConsecutiveFailureCount = job.ConsecutiveFailureCount,
                MaxConsecutiveFailures = job.MaxConsecutiveFailures,
                CreatedAtUtc = job.CreatedAtUtc,
                CreatedBy = job.CreatedBy,
                UpdatedAtUtc = job.UpdatedAtUtc,
                UpdatedBy = job.UpdatedBy
            };

            return Ok(dto);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(ex, "更新定时归档任务参数验证失败: {Id}", id);
            return BadRequest(new { message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "更新定时归档任务失败: {Id}", id);
            return StatusCode(500, new { message = "更新定时归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 删除定时归档任务(软删除)
    /// </summary>
    /// <param name="id">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>无内容</returns>
    [HttpDelete("{id}")]
    [ProducesResponseType(204)]
    [ProducesResponseType(404)]
    public async Task<IActionResult> Delete(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            var job = await _repository.GetByIdAsync(id, cancellationToken);
            if (job == null)
            {
                return NotFound(new { message = $"定时归档任务不存在: {id}" });
            }

            await _repository.DeleteAsync(id, cancellationToken);

            _logger.LogInformation("成功删除定时归档任务: {Id} - {Name}", id, job.Name);

            return NoContent();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "删除定时归档任务失败: {Id}", id);
            return StatusCode(500, new { message = "删除定时归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 启用定时归档任务
    /// </summary>
    /// <param name="id">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>无内容</returns>
    [HttpPost("{id}/enable")]
    [ProducesResponseType(204)]
    [ProducesResponseType(404)]
    public async Task<IActionResult> Enable(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            var job = await _repository.GetByIdAsync(id, cancellationToken);
            if (job == null)
            {
                return NotFound(new { message = $"定时归档任务不存在: {id}" });
            }

            job.Enable("SYSTEM"); // TODO: 从当前用户获取
            await _repository.UpdateAsync(job, cancellationToken);

            // 同步到 Hangfire 调度器
            await _scheduler.RegisterJobAsync(id, cancellationToken);

            _logger.LogInformation("成功启用定时归档任务: {Id} - {Name}", id, job.Name);

            return NoContent();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "启用定时归档任务失败: {Id}", id);
            return StatusCode(500, new { message = "启用定时归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 禁用定时归档任务
    /// </summary>
    /// <param name="id">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>无内容</returns>
    [HttpPost("{id}/disable")]
    [ProducesResponseType(204)]
    [ProducesResponseType(404)]
    public async Task<IActionResult> Disable(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            var job = await _repository.GetByIdAsync(id, cancellationToken);
            if (job == null)
            {
                return NotFound(new { message = $"定时归档任务不存在: {id}" });
            }

            job.Disable("SYSTEM"); // TODO: 从当前用户获取
            await _repository.UpdateAsync(job, cancellationToken);

            // 从 Hangfire 调度器中移除
            await _scheduler.UnregisterJobAsync(id, cancellationToken);

            _logger.LogInformation("成功禁用定时归档任务: {Id} - {Name}", id, job.Name);

            return NoContent();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "禁用定时归档任务失败: {Id}", id);
            return StatusCode(500, new { message = "禁用定时归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 获取任务统计信息
    /// </summary>
    /// <param name="id">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>统计信息</returns>
    [HttpGet("{id}/statistics")]
    [ProducesResponseType(typeof(ScheduledArchiveJobStatisticsDto), 200)]
    [ProducesResponseType(404)]
    public async Task<IActionResult> GetStatistics(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            var job = await _repository.GetByIdAsync(id, cancellationToken);
            if (job == null)
            {
                return NotFound(new { message = $"定时归档任务不存在: {id}" });
            }

            // 计算成功率和平均归档行数
            var successRate = job.TotalExecutionCount > 0
                ? (double)(job.TotalExecutionCount - job.ConsecutiveFailureCount) / job.TotalExecutionCount * 100
                : 0;

            var avgArchived = job.TotalExecutionCount > 0
                ? job.TotalArchivedRowCount / job.TotalExecutionCount
                : 0;

            var dto = new ScheduledArchiveJobStatisticsDto
            {
                Id = job.Id,
                Name = job.Name,
                TotalExecutionCount = job.TotalExecutionCount,
                TotalArchivedRowCount = job.TotalArchivedRowCount,
                SuccessCount = job.TotalExecutionCount - job.ConsecutiveFailureCount, // 简化计算
                FailureCount = job.ConsecutiveFailureCount, // 简化计算
                SkippedCount = 0, // TODO: 需要从执行历史中统计
                SuccessRate = successRate,
                AverageArchivedRowCount = avgArchived,
                LastExecutionAtUtc = job.LastExecutionAtUtc,
                LastExecutionStatus = job.LastExecutionStatus,
                ConsecutiveFailureCount = job.ConsecutiveFailureCount
            };

            return Ok(dto);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取任务统计信息失败: {Id}", id);
            return StatusCode(500, new { message = "获取任务统计信息失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 立即执行定时归档任务
    /// </summary>
    /// <param name="id">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>执行状态消息</returns>
    [HttpPost("{id}/execute")]
    [ProducesResponseType(typeof(object), 202)]
    [ProducesResponseType(404)]
    public async Task<IActionResult> ExecuteNow(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            var job = await _repository.GetByIdAsync(id, cancellationToken);
            if (job == null)
            {
                return NotFound(new { message = $"定时归档任务不存在: {id}" });
            }

            // 触发 Hangfire 立即执行任务
            await _scheduler.TriggerJobAsync(id, cancellationToken);

            _logger.LogInformation("成功触发定时归档任务立即执行: {Id} - {Name}", id, job.Name);

            return Accepted(new
            {
                message = "任务已提交到后台执行队列",
                jobId = id,
                jobName = job.Name
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "触发任务执行失败: {Id}", id);
            return StatusCode(500, new { message = "触发任务执行失败", error = ex.Message });
        }
    }
}


