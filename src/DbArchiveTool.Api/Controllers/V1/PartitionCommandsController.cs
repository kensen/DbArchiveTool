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
    /// 预览分区合并操作,生成DDL脚本和风险提示。
    /// </summary>
    /// <param name="request">合并请求参数</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>包含DDL脚本和风险警告的预览结果</returns>
    [HttpPost("merge/preview")]
    [ProducesResponseType(typeof(PartitionCommandPreviewDto), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> PreviewMerge(
        [FromBody] MergePartitionRequest request,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.PreviewMergeAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("Preview merge failed: {Error}", result.Error);
            return BadRequest(new { error = result.Error });
        }

        return Ok(result.Value);
    }

    /// <summary>
    /// 执行分区合并操作,创建命令并自动加入执行队列。
    /// </summary>
    /// <param name="request">合并请求参数</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>创建的命令ID</returns>
    [HttpPost("merge/execute")]
    [ProducesResponseType(typeof(Guid), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteMerge(
        [FromBody] MergePartitionRequest request,
        CancellationToken cancellationToken = default)
    {
        var result = await appService.ExecuteMergeAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            logger.LogWarning("Execute merge failed: {Error}", result.Error);
            return BadRequest(new { error = result.Error });
        }

        return Ok(new { commandId = result.Value });
    }
}
