using DbArchiveTool.Application.Partitions;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers.V1;

/// <summary>
/// 分区命令管理接口,提供分区拆分、合并、切换等操作的预览、执行和状态查询。
/// </summary>
[ApiController]
[Route("api/v1/partition-commands")]
public class PartitionCommandsController : ControllerBase
{
    private readonly IPartitionCommandAppService appService;
    private readonly ILogger<PartitionCommandsController> logger;

    public PartitionCommandsController(
        IPartitionCommandAppService appService,
        ILogger<PartitionCommandsController> logger)
    {
        this.appService = appService;
        this.logger = logger;
    }

    /// <summary>
    /// 预览分区拆分操作,生成DDL脚本和风险提示。
    /// </summary>
    /// <param name="request">拆分请求参数</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>包含DDL脚本和风险警告的预览结果</returns>
    [HttpPost("split/preview")]
    [ProducesResponseType(typeof(PartitionCommandPreviewDto), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> PreviewSplit(
        [FromBody] SplitPartitionRequest request,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.PreviewSplitAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("Preview split failed: {Error}", result.Error);
            return BadRequest(new { error = result.Error });
        }

        return Ok(result.Value);
    }

    /// <summary>
    /// 执行分区拆分操作,创建命令并等待审批。
    /// </summary>
    /// <param name="request">拆分请求参数</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>创建的命令ID</returns>
    [HttpPost("split/execute")]
    [ProducesResponseType(typeof(Guid), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteSplit(
        [FromBody] SplitPartitionRequest request,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.ExecuteSplitAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("Execute split failed: {Error}", result.Error);
            return BadRequest(new { error = result.Error });
        }

        return Ok(new { commandId = result.Value });
    }

    /// <summary>
    /// 审批分区命令,审批通过后将自动加入执行队列。
    /// </summary>
    /// <param name="commandId">命令ID</param>
    /// <param name="approver">审批人</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>操作结果</returns>
    [HttpPost("{commandId:guid}/approve")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Approve(
        Guid commandId,
        [FromBody] string approver,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.ApproveAsync(commandId, approver, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("Approve command {CommandId} failed: {Error}", commandId, result.Error);
            return BadRequest(new { error = result.Error });
        }

        return Ok(new { message = "命令已审批并加入执行队列" });
    }

    /// <summary>
    /// 拒绝分区命令。
    /// </summary>
    /// <param name="commandId">命令ID</param>
    /// <param name="request">拒绝请求,包含审批人和拒绝原因</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>操作结果</returns>
    [HttpPost("{commandId:guid}/reject")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Reject(
        Guid commandId,
        [FromBody] RejectRequest request,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.RejectAsync(commandId, request.Approver, request.Reason, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("Reject command {CommandId} failed: {Error}", commandId, result.Error);
            return BadRequest(new { error = result.Error });
        }

        return Ok(new { message = "命令已拒绝" });
    }

    /// <summary>
    /// 查询分区命令状态。
    /// </summary>
    /// <param name="commandId">命令ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>命令状态详情</returns>
    [HttpGet("{commandId:guid}/status")]
    [ProducesResponseType(typeof(PartitionCommandStatusDto), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetStatus(
        Guid commandId,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.GetStatusAsync(commandId, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("Get status for command {CommandId} failed: {Error}", commandId, result.Error);
            return NotFound(new { error = result.Error });
        }

        return Ok(result.Value);
    }

    /// <summary>
    /// 拒绝请求参数
    /// </summary>
    public record RejectRequest(string Approver, string Reason);
}
