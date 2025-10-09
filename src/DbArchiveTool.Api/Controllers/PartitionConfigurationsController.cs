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
}

