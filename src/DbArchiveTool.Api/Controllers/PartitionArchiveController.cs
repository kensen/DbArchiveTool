using DbArchiveTool.Api.Models;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers;

/// <summary>
/// 分区数据归档相关接口（规划中方案）。
/// </summary>
[ApiController]
[Route("api/v1/partition-archives")]
public sealed class PartitionArchiveController : ControllerBase
{
    private readonly IPartitionArchiveAppService archiveAppService;

    public PartitionArchiveController(IPartitionArchiveAppService archiveAppService)
    {
        this.archiveAppService = archiveAppService;
    }

    /// <summary>
    /// 规划基于 BCP 的跨实例归档方案（占位实现）。
    /// </summary>
    [HttpPost("bcp/plan")]
    [ProducesResponseType(typeof(Result<ArchivePlanDto>), StatusCodes.Status200OK)]
    public async Task<IActionResult> PlanBcpAsync([FromBody] BcpArchivePlanDto request, CancellationToken cancellationToken)
    {
        var result = await archiveAppService.PlanArchiveWithBcpAsync(request.ToApplicationRequest(), cancellationToken);
        return Ok(result);
    }

    /// <summary>
    /// 规划基于 BulkCopy 的跨实例归档方案（占位实现）。
    /// </summary>
    [HttpPost("bulkcopy/plan")]
    [ProducesResponseType(typeof(Result<ArchivePlanDto>), StatusCodes.Status200OK)]
    public async Task<IActionResult> PlanBulkCopyAsync([FromBody] BulkCopyArchivePlanDto request, CancellationToken cancellationToken)
    {
        var result = await archiveAppService.PlanArchiveWithBulkCopyAsync(request.ToApplicationRequest(), cancellationToken);
        return Ok(result);
    }
}
