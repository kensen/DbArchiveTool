using DbArchiveTool.Api.DTOs.Archives;
using DbArchiveTool.Application.Abstractions;
using DbArchiveTool.Application.Archives;
using DbArchiveTool.Application.Services.ScheduledArchiveJobs;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Entities;
using DbArchiveTool.Domain.ScheduledArchiveJobs;
using DbArchiveTool.Shared.Archive;
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
    private readonly ITableManagementService _tableManagementService;
    private readonly IDataSourceRepository _dataSourceRepository;
    private readonly IPasswordEncryptionService _passwordEncryptionService;
    private readonly ILogger<ScheduledArchiveJobsController> _logger;

    public ScheduledArchiveJobsController(
        IScheduledArchiveJobRepository repository,
        IScheduledArchiveJobScheduler scheduler,
        ITableManagementService tableManagementService,
        IDataSourceRepository dataSourceRepository,
        IPasswordEncryptionService passwordEncryptionService,
        ILogger<ScheduledArchiveJobsController> logger)
    {
        _repository = repository;
        _scheduler = scheduler;
        _tableManagementService = tableManagementService;
        _dataSourceRepository = dataSourceRepository;
        _passwordEncryptionService = passwordEncryptionService;
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
                CronExpression = j.CronExpression,
                IsEnabled = j.IsEnabled,
                NextExecutionAtUtc = j.NextExecutionAtUtc,
                LastExecutionAtUtc = j.LastExecutionAtUtc,
                LastExecutionStatus = j.LastExecutionStatus,
                LastExecutionError = j.LastExecutionError,
                LastArchivedRowCount = j.LastArchivedRowCount,
                TotalExecutionCount = j.TotalExecutionCount,
                TotalArchivedRowCount = j.TotalArchivedRowCount,
                ConsecutiveFailureCount = j.ConsecutiveFailureCount,
                MaxConsecutiveFailures = j.MaxConsecutiveFailures,
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
            // 定时归档任务专用于“普通表小批量归档”，不允许使用分区切换/BCP 等方式，避免与分区管理功能混淆
            if (request.ArchiveMethod != ArchiveMethod.BulkCopy)
            {
                return BadRequest(new { message = "定时归档任务仅支持普通表归档，归档方法必须为 BulkCopy(不支持 PartitionSwitch/BCP)。" });
            }

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
                archiveMethod: ArchiveMethod.BulkCopy,
                deleteSourceDataAfterArchive: request.DeleteSourceDataAfterArchive,
                batchSize: request.BatchSize,
                maxRowsPerExecution: request.MaxRowsPerExecution,
                intervalMinutes: request.IntervalMinutes,
                cronExpression: request.CronExpression,
                maxConsecutiveFailures: request.MaxConsecutiveFailures,
                createdBy: "SYSTEM" // TODO: 从当前用户获取
            );

            await _repository.AddAsync(job, cancellationToken);

            // 创建后若任务处于启用状态，需要立即注册到 Hangfire
            // 否则会出现“列表显示启用，但 Hangfire 未产生 RecurringJob，需要手工点一次启用才生效”的体验问题
            if (job.IsEnabled)
            {
                try
                {
                    await _scheduler.RegisterJobAsync(job.Id, cancellationToken);

                    // RegisterJobAsync 内部会更新 NextExecutionAtUtc，需要重新加载以返回最新值
                    job = await _repository.GetByIdAsync(job.Id, cancellationToken) ?? job;
                }
                catch (Exception ex)
                {
                    // 不阻断创建接口返回，但记录错误，方便排查“创建成功但未调度”
                    _logger.LogError(ex, "创建后注册 Hangfire 调度失败: {Id}", job.Id);
                }
            }

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
    /// 同步任务调度（重新注册/重算下次执行时间）
    /// </summary>
    /// <param name="id">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>无内容</returns>
    [HttpPost("{id}/reschedule")]
    [ProducesResponseType(204)]
    [ProducesResponseType(404)]
    public async Task<IActionResult> Reschedule(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            var job = await _repository.GetByIdAsync(id, cancellationToken);
            if (job == null)
            {
                return NotFound(new { message = $"定时归档任务不存在: {id}" });
            }

            await _scheduler.UpdateJobScheduleAsync(id, cancellationToken);

            _logger.LogInformation("已同步定时归档任务调度: {Id} - {Name}", id, job.Name);
            return NoContent();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "同步定时归档任务调度失败: {Id}", id);
            return StatusCode(500, new { message = "同步定时归档任务调度失败", error = ex.Message });
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

            // 定时归档任务专用于“普通表小批量归档”，不允许使用分区切换/BCP 等方式，避免与分区管理功能混淆
            if (request.ArchiveMethod != ArchiveMethod.BulkCopy)
            {
                return BadRequest(new { message = "定时归档任务仅支持普通表归档，归档方法必须为 BulkCopy(不支持 PartitionSwitch/BCP)。" });
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
                archiveMethod: ArchiveMethod.BulkCopy,
                deleteSourceDataAfterArchive: request.DeleteSourceDataAfterArchive,
                batchSize: request.BatchSize,
                maxRowsPerExecution: request.MaxRowsPerExecution,
                intervalMinutes: request.IntervalMinutes,
                cronExpression: request.CronExpression,
                maxConsecutiveFailures: request.MaxConsecutiveFailures,
                updatedBy: "SYSTEM" // TODO: 从当前用户获取
            );

            await _repository.UpdateAsync(job, cancellationToken);

            // 若任务处于启用状态，更新后同步调度配置到 Hangfire
            if (job.IsEnabled)
            {
                try
                {
                    await _scheduler.UpdateJobScheduleAsync(id, cancellationToken);
                }
                catch (Exception ex)
                {
                    // 不阻断更新接口返回，但记录错误，避免“配置已保存但调度未更新”不可见
                    _logger.LogError(ex, "同步更新 Hangfire 调度配置失败: {Id}", id);
                }
            }

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

            // 删除后从 Hangfire 调度器中移除，避免留下孤儿 RecurringJob
            try
            {
                await _scheduler.UnregisterJobAsync(id, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "删除后移除 Hangfire 调度失败: {Id}", id);
            }

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

    /// <summary>
    /// 检查目标表是否存在
    /// </summary>
    /// <param name="request">检查请求</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>检查结果</returns>
    [HttpPost("check-target-table")]
    [ProducesResponseType(typeof(TargetTableCheckResult), 200)]
    [ProducesResponseType(400)]
    public async Task<IActionResult> CheckTargetTable(
        [FromBody] CheckTargetTableRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // 获取数据源
            var dataSource = await _dataSourceRepository.GetAsync(request.DataSourceId, cancellationToken);
            if (dataSource == null)
            {
                return BadRequest(new { message = $"数据源不存在: {request.DataSourceId}" });
            }

            // 获取目标数据库连接字符串
            var targetConnectionString = GetTargetConnectionString(dataSource);

            // 检查目标表是否存在
            var exists = await _tableManagementService.CheckTableExistsAsync(
                targetConnectionString,
                request.TargetSchemaName,
                request.TargetTableName,
                cancellationToken);

            var result = new TargetTableCheckResult
            {
                Exists = exists,
                TargetSchemaName = request.TargetSchemaName,
                TargetTableName = request.TargetTableName,
                Message = exists
                    ? $"目标表 [{request.TargetSchemaName}].[{request.TargetTableName}] 已存在"
                    : $"目标表 [{request.TargetSchemaName}].[{request.TargetTableName}] 不存在"
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "检查目标表失败");
            return StatusCode(500, new { message = "检查目标表失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 创建目标表
    /// </summary>
    /// <param name="request">创建请求</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>创建结果</returns>
    [HttpPost("create-target-table")]
    [ProducesResponseType(typeof(TargetTableCreationResult), 200)]
    [ProducesResponseType(400)]
    public async Task<IActionResult> CreateTargetTable(
        [FromBody] CreateTargetTableRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // 获取数据源
            var dataSource = await _dataSourceRepository.GetAsync(request.DataSourceId, cancellationToken);
            if (dataSource == null)
            {
                return BadRequest(new { message = $"数据源不存在: {request.DataSourceId}" });
            }

            // 获取源数据库和目标数据库连接字符串
            var sourceConnectionString = GetSourceConnectionString(dataSource);
            var targetConnectionString = GetTargetConnectionString(dataSource);

            // 创建目标表
            var creationResult = await _tableManagementService.CreateTargetTableAsync(
                sourceConnectionString,
                targetConnectionString,
                request.SourceSchemaName,
                request.SourceTableName,
                request.TargetSchemaName,
                request.TargetTableName,
                cancellationToken);

            if (!creationResult.Success)
            {
                return BadRequest(new { message = creationResult.ErrorMessage });
            }

            var result = new TargetTableCreationResult
            {
                Success = true,
                Message = $"目标表 [{request.TargetSchemaName}].[{request.TargetTableName}] 创建成功",
                TargetSchemaName = request.TargetSchemaName,
                TargetTableName = request.TargetTableName,
                ColumnCount = creationResult.ColumnCount,
                Script = creationResult.Script
            };

            _logger.LogInformation(
                "成功创建目标表: {Schema}.{Table}, 列数={ColumnCount}",
                request.TargetSchemaName, request.TargetTableName, creationResult.ColumnCount);

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建目标表失败");
            return StatusCode(500, new { message = "创建目标表失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 验证目标表结构
    /// </summary>
    /// <param name="request">验证请求</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>验证结果</returns>
    [HttpPost("validate-target-table")]
    [ProducesResponseType(typeof(TargetTableValidationResult), 200)]
    [ProducesResponseType(400)]
    public async Task<IActionResult> ValidateTargetTable(
        [FromBody] ValidateTargetTableRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // 获取数据源
            var dataSource = await _dataSourceRepository.GetAsync(request.DataSourceId, cancellationToken);
            if (dataSource == null)
            {
                return BadRequest(new { message = $"数据源不存在: {request.DataSourceId}" });
            }

            // 获取源数据库和目标数据库连接字符串
            var sourceConnectionString = GetSourceConnectionString(dataSource);
            var targetConnectionString = GetTargetConnectionString(dataSource);

            // 验证目标表结构
            var comparisonResult = await _tableManagementService.CompareTableSchemasAsync(
                sourceConnectionString,
                request.SourceSchemaName,
                request.SourceTableName,
                targetConnectionString,
                null, // 目标数据库名称从连接字符串中获取
                request.TargetSchemaName,
                request.TargetTableName,
                cancellationToken);

            var result = new TargetTableValidationResult
            {
                TargetTableExists = comparisonResult.TargetTableExists,
                IsCompatible = comparisonResult.IsCompatible,
                Message = comparisonResult.IsCompatible
                    ? $"目标表 [{request.TargetSchemaName}].[{request.TargetTableName}] 结构与源表一致，共 {comparisonResult.SourceColumnCount} 列"
                    : comparisonResult.DifferenceDescription ?? "目标表结构验证失败",
                SourceColumnCount = comparisonResult.SourceColumnCount,
                TargetColumnCount = comparisonResult.TargetColumnCount,
                MissingColumns = comparisonResult.MissingColumns,
                TypeMismatchColumns = comparisonResult.TypeMismatchColumns,
                LengthInsufficientColumns = comparisonResult.LengthInsufficientColumns,
                PrecisionInsufficientColumns = comparisonResult.PrecisionInsufficientColumns
            };

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "验证目标表结构失败");
            return StatusCode(500, new { message = "验证目标表结构失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 获取源数据库连接字符串
    /// </summary>
    private string GetSourceConnectionString(ArchiveDataSource dataSource)
    {
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder
        {
            DataSource = $"{dataSource.ServerAddress},{dataSource.ServerPort}",
            InitialCatalog = dataSource.DatabaseName,
            IntegratedSecurity = dataSource.UseIntegratedSecurity,
            TrustServerCertificate = true
        };

        if (!dataSource.UseIntegratedSecurity)
        {
            builder.UserID = dataSource.UserName;
            // 解密密码
            builder.Password = DecryptPassword(dataSource.Password);
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// 获取目标数据库连接字符串
    /// </summary>
    private string GetTargetConnectionString(ArchiveDataSource dataSource)
    {
        if (dataSource.UseSourceAsTarget)
        {
            // 目标数据库与源数据库相同
            return GetSourceConnectionString(dataSource);
        }

        // 使用独立的目标数据库配置
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder
        {
            DataSource = $"{dataSource.TargetServerAddress},{dataSource.TargetServerPort}",
            InitialCatalog = dataSource.TargetDatabaseName,
            IntegratedSecurity = dataSource.TargetUseIntegratedSecurity,
            TrustServerCertificate = true
        };

        if (!dataSource.TargetUseIntegratedSecurity)
        {
            builder.UserID = dataSource.TargetUserName;
            // 解密密码
            builder.Password = DecryptPassword(dataSource.TargetPassword);
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// 解密密码
    /// </summary>
    private string? DecryptPassword(string? encrypted)
    {
        if (string.IsNullOrWhiteSpace(encrypted))
        {
            return encrypted;
        }

        // 检查是否已加密
        return _passwordEncryptionService.IsEncrypted(encrypted)
            ? _passwordEncryptionService.Decrypt(encrypted)
            : encrypted;
    }
}

/// <summary>
/// 检查目标表请求
/// </summary>
public sealed record CheckTargetTableRequest(
    Guid DataSourceId,
    string TargetSchemaName,
    string TargetTableName);

/// <summary>
/// 检查目标表结果
/// </summary>
public sealed record TargetTableCheckResult
{
    public bool Exists { get; init; }
    public string TargetSchemaName { get; init; } = string.Empty;
    public string TargetTableName { get; init; } = string.Empty;
    public string Message { get; init; } = string.Empty;
}

/// <summary>
/// 创建目标表请求
/// </summary>
public sealed record CreateTargetTableRequest(
    Guid DataSourceId,
    string SourceSchemaName,
    string SourceTableName,
    string TargetSchemaName,
    string TargetTableName);

/// <summary>
/// 创建目标表结果
/// </summary>
public sealed record TargetTableCreationResult
{
    public bool Success { get; init; }
    public string Message { get; init; } = string.Empty;
    public string TargetSchemaName { get; init; } = string.Empty;
    public string TargetTableName { get; init; } = string.Empty;
    public int ColumnCount { get; init; }
    public string? Script { get; init; }
}

/// <summary>
/// 验证目标表结构请求
/// </summary>
public sealed record ValidateTargetTableRequest(
    Guid DataSourceId,
    string SourceSchemaName,
    string SourceTableName,
    string TargetSchemaName,
    string TargetTableName);

/// <summary>
/// 验证目标表结构结果
/// </summary>
public sealed record TargetTableValidationResult
{
    public bool TargetTableExists { get; init; }
    public bool IsCompatible { get; init; }
    public string Message { get; init; } = string.Empty;
    public int SourceColumnCount { get; init; }
    public int? TargetColumnCount { get; init; }
    public List<string> MissingColumns { get; init; } = new();
    public List<string> TypeMismatchColumns { get; init; } = new();
    public List<string> LengthInsufficientColumns { get; init; } = new();
    public List<string> PrecisionInsufficientColumns { get; init; } = new();
}


