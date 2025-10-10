using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Api.Models;
using DbArchiveTool.Application.Partitions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Api.Controllers.V1;

/// <summary>
/// 分区执行任务相关接口。
/// </summary>
[ApiController]
[Route("api/v1/partition-executions")]
public sealed class PartitionExecutionsController : ControllerBase
{
    private readonly IPartitionExecutionAppService appService;
    private readonly ILogger<PartitionExecutionsController> logger;

    public PartitionExecutionsController(
        IPartitionExecutionAppService appService,
        ILogger<PartitionExecutionsController> logger)
    {
        this.appService = appService;
        this.logger = logger;
    }

    /// <summary>
    /// 发起分区执行任务。
    /// </summary>
    [HttpPost]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Start(
        [FromBody] StartPartitionExecutionDto dto,
        CancellationToken cancellationToken = default)
    {
        var request = new StartPartitionExecutionRequest(
            dto.PartitionConfigurationId,
            dto.DataSourceId,
            dto.RequestedBy,
            dto.BackupConfirmed,
            dto.BackupReference,
            dto.Notes,
            dto.ForceWhenWarnings,
            dto.Priority);

        var result = await appService.StartAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("Start partition execution failed: {Error}", result.Error);
            return BadRequest(new { error = result.Error });
        }

        return Ok(new { taskId = result.Value });
    }

    /// <summary>
    /// 获取任务详情。
    /// </summary>
    [HttpGet("{taskId:guid}")]
    [ProducesResponseType(typeof(PartitionExecutionTaskDetailDto), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Get(
        Guid taskId,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.GetAsync(taskId, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("Get partition execution {TaskId} failed: {Error}", taskId, result.Error);
            return NotFound(new { error = result.Error });
        }

        return Ok(result.Value);
    }

    /// <summary>
    /// 列出近期任务。
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(List<PartitionExecutionTaskSummaryDto>), StatusCodes.Status200OK)]
    public async Task<IActionResult> List(
        [FromQuery] Guid? dataSourceId,
        [FromQuery] int maxCount = 20,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.ListAsync(dataSourceId, maxCount, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("List partition executions failed: {Error}", result.Error);
            return BadRequest(new { error = result.Error });
        }

        return Ok(result.Value ?? new List<PartitionExecutionTaskSummaryDto>());
    }

    /// <summary>
    /// 获取任务日志。
    /// </summary>
    [HttpGet("{taskId:guid}/logs")]
    [ProducesResponseType(typeof(List<PartitionExecutionLogDto>), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetLogs(
        Guid taskId,
        [FromQuery] DateTime? sinceUtc,
        [FromQuery] int take = 200,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.GetLogsAsync(taskId, sinceUtc, take, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("Get logs for task {TaskId} failed: {Error}", taskId, result.Error);
            return BadRequest(new { error = result.Error });
        }

        return Ok(result.Value ?? new List<PartitionExecutionLogDto>());
    }
}
