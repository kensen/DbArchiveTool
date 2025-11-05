using DbArchiveTool.Api.DTOs.Archives;
using DbArchiveTool.Application.Archives;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers.V1;

/// <summary>
/// 归档执行 API 控制器
/// </summary>
[ApiController]
[Route("api/v1/archive-executions")]
[Produces("application/json")]
public sealed class ArchiveExecutionsController : ControllerBase
{
    private readonly ArchiveOrchestrationService _orchestrationService;
    private readonly ILogger<ArchiveExecutionsController> _logger;

    public ArchiveExecutionsController(
        ArchiveOrchestrationService orchestrationService,
        ILogger<ArchiveExecutionsController> logger)
    {
        _orchestrationService = orchestrationService;
        _logger = logger;
    }

    /// <summary>
    /// 执行单个归档任务
    /// </summary>
    /// <param name="configId">归档配置ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>执行结果</returns>
    [HttpPost("single/{configId}")]
    [ProducesResponseType(typeof(ArchiveExecutionResult), 200)]
    [ProducesResponseType(400)]
    public async Task<IActionResult> ExecuteSingle(Guid configId, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("开始执行归档任务: {ConfigId}", configId);

            var result = await _orchestrationService.ExecuteArchiveAsync(
                configId,
                partitionNumber: null,
                progressCallback: null,
                cancellationToken);

            if (result.Success)
            {
                _logger.LogInformation("归档任务执行成功: {ConfigId}, 归档 {Rows} 行",
                    configId, result.RowsArchived);
            }
            else
            {
                _logger.LogWarning("归档任务执行失败: {ConfigId}, 原因: {Message}",
                    configId, result.Message);
            }

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "执行归档任务失败: {ConfigId}", configId);
            return StatusCode(500, new { message = "执行归档任务失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 批量执行归档任务
    /// </summary>
    /// <param name="request">执行请求</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>批量执行结果</returns>
    [HttpPost("batch")]
    [ProducesResponseType(typeof(BatchArchiveExecutionResult), 200)]
    [ProducesResponseType(400)]
    public async Task<IActionResult> ExecuteBatch(
        [FromBody] ExecuteArchiveRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (request.ConfigurationIds == null || request.ConfigurationIds.Count == 0)
            {
                return BadRequest(new { message = "归档配置ID列表不能为空" });
            }

            _logger.LogInformation("开始批量执行归档任务: {Count} 个配置", request.ConfigurationIds.Count);

            var result = await _orchestrationService.ExecuteBatchArchiveAsync(
                request.ConfigurationIds,
                cancellationToken);

            _logger.LogInformation(
                "批量归档任务执行完成: 成功 {Success}/{Total}, 总归档 {TotalRows} 行",
                result.SuccessCount,
                result.TotalTasks,
                result.TotalRowsArchived);

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "批量执行归档任务失败");
            return StatusCode(500, new { message = "批量执行归档任务失败", error = ex.Message });
        }
    }

}
