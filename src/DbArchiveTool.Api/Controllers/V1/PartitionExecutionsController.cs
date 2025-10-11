using System;
using System.Collections.Generic;
using System.Linq;
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
    /// <param name="dto">执行请求体，包含配置标识、数据源、执行人、备份确认等信息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>成功时返回任务标识（taskId），失败时返回错误信息。</returns>
    /// <response code="200">任务创建成功，返回任务标识。</response>
    /// <response code="400">请求参数无效或业务校验失败。</response>
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
    /// <param name="taskId">任务标识（GUID）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>返回任务的详细信息，包括状态、进度、执行阶段、备份参考等。</returns>
    /// <response code="200">成功获取任务详情。</response>
    /// <response code="404">任务不存在。</response>
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
    /// <param name="dataSourceId">可选的数据源标识，用于过滤特定数据源的任务。</param>
    /// <param name="maxCount">最多返回的任务数量，默认 20。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>返回任务摘要列表，按创建时间倒序排列。</returns>
    /// <response code="200">成功获取任务列表。</response>
    /// <response code="400">请求参数无效。</response>
    [HttpGet]
    [ProducesResponseType(typeof(List<PartitionExecutionTaskSummaryDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
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
    /// 获取任务日志（分页）。
    /// </summary>
    /// <param name="taskId">任务标识（GUID）。</param>
    /// <param name="pageIndex">页码（从 1 开始），默认 1。</param>
    /// <param name="pageSize">每页记录数，默认 20，最大 200。</param>
    /// <param name="category">可选的日志分类过滤（Info、Warning、Error、Step、Cancel 等）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>返回分页后的日志列表。</returns>
    /// <response code="200">成功获取日志列表。</response>
    /// <response code="400">请求参数无效。</response>
    [HttpGet("{taskId:guid}/logs")]
    [ProducesResponseType(typeof(List<PartitionExecutionLogDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetLogs(
        Guid taskId,
        [FromQuery] int pageIndex = 1,
        [FromQuery] int pageSize = 20,
        [FromQuery] string? category = null,
        CancellationToken cancellationToken = default)
    {
        // 参数校验
        if (pageIndex < 1)
        {
            return BadRequest(new { error = "页码必须大于等于 1。" });
        }

        if (pageSize < 1 || pageSize > 200)
        {
            return BadRequest(new { error = "每页记录数必须在 1 到 200 之间。" });
        }

        // 计算 skip/take
        var skip = (pageIndex - 1) * pageSize;
        var take = pageSize;

        // 由于现有 GetLogsAsync 不支持分页和分类过滤，这里先调用原方法并在内存中过滤
        // TODO: 未来优化为在仓储层直接支持分页和过滤
        var result = await appService.GetLogsAsync(taskId, null, 1000, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("获取任务 {TaskId} 日志失败: {Error}", taskId, result.Error);
            return BadRequest(new { error = result.Error });
        }

        var allLogs = result.Value ?? new List<PartitionExecutionLogDto>();

        // 分类过滤
        if (!string.IsNullOrWhiteSpace(category))
        {
            allLogs = allLogs.Where(log => log.Category.Equals(category, StringComparison.OrdinalIgnoreCase)).ToList();
        }

        // 分页
        var pagedLogs = allLogs
            .OrderByDescending(log => log.LogTimeUtc)
            .Skip(skip)
            .Take(take)
            .ToList();

        return Ok(new
        {
            pageIndex,
            pageSize,
            totalCount = allLogs.Count,
            items = pagedLogs
        });
    }

    /// <summary>
    /// 取消分区执行任务。
    /// </summary>
    /// <param name="taskId">任务标识（GUID）。</param>
    /// <param name="dto">取消请求体，包含取消人和取消原因。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>成功时返回空结果，失败时返回错误信息。</returns>
    /// <response code="200">任务取消成功。</response>
    /// <response code="400">请求参数无效或业务校验失败（例如任务已结束）。</response>
    /// <response code="404">任务不存在。</response>
    [HttpPost("{taskId:guid}/cancel")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Cancel(
        Guid taskId,
        [FromBody] CancelPartitionExecutionDto dto,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.CancelAsync(taskId, dto.CancelledBy, dto.Reason, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("取消任务 {TaskId} 失败: {Error}", taskId, result.Error);

            // 判断错误类型（任务不存在 vs 业务校验失败）
            if (result.Error!.Contains("任务不存在") || result.Error.Contains("不存在"))
            {
                return NotFound(new { error = result.Error });
            }

            return BadRequest(new { error = result.Error });
        }

        return Ok(new { message = "任务已成功取消。" });
    }
}
