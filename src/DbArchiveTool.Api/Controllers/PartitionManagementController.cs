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

    /// <summary>
    /// 获取指定已分区表的元数据信息(分区列、类型、边界值、文件组映射等)。
    /// </summary>
    [HttpGet("metadata")]
    [ProducesResponseType(typeof(PartitionMetadataDto), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetMetadataAsync(Guid dataSourceId, [FromQuery] string schema, [FromQuery] string table, CancellationToken cancellationToken)
    {
        var result = await appService.GetPartitionMetadataAsync(new PartitionMetadataRequest(dataSourceId, schema, table), cancellationToken);
        if (!result.IsSuccess)
        {
            return Problem(title: "获取分区元数据失败", detail: result.Error, statusCode: StatusCodes.Status404NotFound);
        }

        return Ok(result.Value);
    }

    /// <summary>
    /// 为已分区表添加新的分区边界值。
    /// </summary>
    [HttpPost("boundaries")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<IActionResult> AddBoundaryAsync(Guid dataSourceId, [FromBody] AddBoundaryRequestDto dto, CancellationToken cancellationToken)
    {
        var request = new AddBoundaryToPartitionedTableRequest(
            dataSourceId,
            dto.SchemaName,
            dto.TableName,
            dto.BoundaryValue,
            dto.FilegroupName,
            dto.RequestedBy ?? "Anonymous",
            dto.Notes);

        var result = await appService.AddBoundaryToPartitionedTableAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            return Problem(title: "添加分区边界值失败", detail: result.Error, statusCode: StatusCodes.Status400BadRequest);
        }

        return Ok();
    }
}

/// <summary>
/// 添加边界值请求DTO(用于API Body绑定)。
/// </summary>
public sealed record AddBoundaryRequestDto(
    string SchemaName,
    string TableName,
    string BoundaryValue,
    string? FilegroupName,
    string? RequestedBy,
    string? Notes);

