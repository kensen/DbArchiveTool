using DbArchiveTool.Api.Models;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers;

/// <summary>
/// 分区配置向导相关的写入接口。
/// </summary>
[ApiController]
[Route("api/v1/partition-configurations")]
public sealed class PartitionConfigurationsController : ControllerBase
{
    private readonly IPartitionConfigurationAppService appService;

    public PartitionConfigurationsController(IPartitionConfigurationAppService appService)
    {
        this.appService = appService;
    }

    /// <summary>
    /// 创建新的分区配置。
    /// </summary>
    [HttpPost]
    [ProducesResponseType(typeof(Result<Guid>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<Guid>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> CreateAsync([FromBody] CreatePartitionConfigurationDto request, CancellationToken cancellationToken)
    {
        var result = await appService.CreateAsync(request.ToApplicationRequest(), cancellationToken);
        return result.IsSuccess
            ? Ok(Result<Guid>.Success(result.Value))
            : BadRequest(Result<Guid>.Failure(result.Error!));
    }

    /// <summary>
    /// 替换指定配置的分区边界值。
    /// </summary>
    [HttpPost("{id:guid}/values")]
    [ProducesResponseType(typeof(Result), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ReplaceValuesAsync(Guid id, [FromBody] ReplacePartitionValuesDto request, CancellationToken cancellationToken)
    {
        var result = await appService.ReplaceValuesAsync(id, request.ToApplicationRequest(), cancellationToken);
        return result.IsSuccess
            ? Ok(Result.Success())
            : BadRequest(Result.Failure(result.Error!));
    }

    /// <summary>
    /// 获取指定数据源的所有分区配置（包括草稿）。
    /// </summary>
    [HttpGet("by-datasource/{dataSourceId:guid}")]
    [ProducesResponseType(typeof(Result<List<PartitionConfigurationSummaryDto>>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<List<PartitionConfigurationSummaryDto>>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetByDataSourceAsync(Guid dataSourceId, CancellationToken cancellationToken)
    {
        var result = await appService.GetByDataSourceAsync(dataSourceId, cancellationToken);
        return result.IsSuccess
            ? Ok(result)
            : BadRequest(result);
    }

    /// <summary>
    /// 删除指定的分区配置草稿。
    /// </summary>
    [HttpDelete("{id:guid}")]
    [ProducesResponseType(typeof(Result), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> DeleteAsync(Guid id, CancellationToken cancellationToken)
    {
        var result = await appService.DeleteAsync(id, cancellationToken);
        return result.IsSuccess
            ? Ok(Result.Success())
            : BadRequest(Result.Failure(result.Error!));
    }
}
