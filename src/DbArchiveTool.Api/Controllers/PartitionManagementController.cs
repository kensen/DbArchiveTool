using DbArchiveTool.Application.Partitions;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers;

/// <summary>
/// 分区管理相关接口，提供只读概览与安全信息。
/// </summary>
[ApiController]
[Route("api/v1/archive-data-sources/{dataSourceId:guid}/partitions")]
public sealed class PartitionManagementController : ControllerBase
{
    private readonly IPartitionManagementAppService appService;

    public PartitionManagementController(IPartitionManagementAppService appService)
    {
        this.appService = appService;
    }

    /// <summary>
    /// 获取指定数据源表的分区概览。
    /// </summary>
    [HttpGet("overview")]
    [ProducesResponseType(typeof(PartitionOverviewDto), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetOverviewAsync(Guid dataSourceId, [FromQuery] string schema, [FromQuery] string table, CancellationToken cancellationToken)
    {
        var result = await appService.GetOverviewAsync(new PartitionOverviewRequest(dataSourceId, schema, table), cancellationToken);
        if (!result.IsSuccess)
        {
            return Problem(title: "获取分区概览失败", detail: result.Error, statusCode: StatusCodes.Status404NotFound);
        }

        return Ok(result.Value);
    }

    /// <summary>
    /// 获取指定分区边界的安全状态。
    /// </summary>
    [HttpGet("{boundaryKey}/safety")]
    [ProducesResponseType(typeof(PartitionBoundarySafetyDto), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetSafetyAsync(Guid dataSourceId, string boundaryKey, [FromQuery] string schema, [FromQuery] string table, CancellationToken cancellationToken)
    {
        var result = await appService.GetBoundarySafetyAsync(new PartitionBoundarySafetyRequest(dataSourceId, schema, table, boundaryKey), cancellationToken);
        if (!result.IsSuccess)
        {
            return Problem(title: "获取分区安全信息失败", detail: result.Error, statusCode: StatusCodes.Status404NotFound);
        }

        return Ok(result.Value);
    }
}
