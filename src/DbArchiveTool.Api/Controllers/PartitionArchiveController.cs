using DbArchiveTool.Api.Models;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers;

/// <summary>
/// 分区归档相关接口。
/// </summary>
[ApiController]
[Route("api/v1/partition-archive")]
public sealed class PartitionArchiveController : ControllerBase
{
    private readonly IPartitionSwitchAppService switchAppService;
    private readonly IPartitionArchiveAppService archiveAppService;

    public PartitionArchiveController(
        IPartitionSwitchAppService switchAppService,
        IPartitionArchiveAppService archiveAppService)
    {
        this.switchAppService = switchAppService;
        this.archiveAppService = archiveAppService;
    }

    /// <summary>
    /// 检查分区切换是否满足执行条件。
    /// </summary>
    [HttpPost("switch/inspect")]
    [ProducesResponseType(typeof(Result<PartitionSwitchInspectionResultDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<PartitionSwitchInspectionResultDto>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> InspectSwitchAsync([FromBody] SwitchArchiveInspectDto request, CancellationToken cancellationToken)
    {
        var result = await switchAppService.InspectAsync(request.ToApplicationRequest(), cancellationToken);
        return result.IsSuccess ? Ok(result) : BadRequest(result);
    }

    /// <summary>
    /// 提交分区切换归档任务。
    /// </summary>
    [HttpPost("switch")]
    [ProducesResponseType(typeof(Result<Guid>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<Guid>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ArchiveBySwitchAsync([FromBody] SwitchArchiveExecuteDto request, CancellationToken cancellationToken)
    {
        var result = await switchAppService.ArchiveBySwitchAsync(request.ToApplicationRequest(), cancellationToken);
        return result.IsSuccess
            ? Ok(Result<Guid>.Success(result.Value))
            : BadRequest(Result<Guid>.Failure(result.Error!));
    }

    /// <summary>
    /// 执行分区切换自动补齐步骤。
    /// </summary>
    [HttpPost("switch/autofix")]
    [ProducesResponseType(typeof(Result<PartitionSwitchAutoFixResultDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<PartitionSwitchAutoFixResultDto>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> AutoFixSwitchAsync([FromBody] SwitchArchiveAutoFixDto request, CancellationToken cancellationToken)
    {
        var result = await switchAppService.AutoFixAsync(request.ToApplicationRequest(), cancellationToken);
        return result.IsSuccess ? Ok(result) : BadRequest(result);
    }

    /// <summary>
    /// 规划基于 BCP 的归档方案（占位）。
    /// </summary>
    [HttpPost("bcp/plan")]
    [ProducesResponseType(typeof(Result<ArchivePlanDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<ArchivePlanDto>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> PlanBcpAsync([FromBody] BcpArchivePlanRequest request, CancellationToken cancellationToken)
    {
        var result = await archiveAppService.PlanArchiveWithBcpAsync(request, cancellationToken);
        return result.IsSuccess ? Ok(result) : BadRequest(result);
    }

    /// <summary>
    /// 规划基于 BulkCopy 的归档方案（占位）。
    /// </summary>
    [HttpPost("bulkcopy/plan")]
    [ProducesResponseType(typeof(Result<ArchivePlanDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<ArchivePlanDto>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> PlanBulkCopyAsync([FromBody] BulkCopyArchivePlanRequest request, CancellationToken cancellationToken)
    {
        var result = await archiveAppService.PlanArchiveWithBulkCopyAsync(request, cancellationToken);
        return result.IsSuccess ? Ok(result) : BadRequest(result);
    }

    /// <summary>
    /// 预检 BCP 归档条件。
    /// </summary>
    [HttpPost("bcp/inspect")]
    [ProducesResponseType(typeof(Result<ArchiveInspectionResultDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<ArchiveInspectionResultDto>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> InspectBcpAsync([FromBody] BcpArchiveInspectDto request, CancellationToken cancellationToken)
    {
        var result = await archiveAppService.InspectBcpAsync(request.ToApplicationRequest(), cancellationToken);
        return result.IsSuccess ? Ok(result) : BadRequest(result);
    }

    /// <summary>
    /// 提交 BCP 归档任务。
    /// </summary>
    [HttpPost("bcp/execute")]
    [ProducesResponseType(typeof(Result<Guid>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<Guid>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteBcpAsync([FromBody] BcpArchiveExecuteDto request, CancellationToken cancellationToken)
    {
        var result = await archiveAppService.ExecuteWithBcpAsync(request.ToApplicationRequest(), cancellationToken);
        return result.IsSuccess
            ? Ok(Result<Guid>.Success(result.Value))
            : BadRequest(Result<Guid>.Failure(result.Error!));
    }

    /// <summary>
    /// 执行 BCP/BulkCopy 归档自动修复步骤。
    /// </summary>
    [HttpPost("autofix")]
    [ProducesResponseType(typeof(Result<string>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<string>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteAutoFixAsync([FromBody] ArchiveAutoFixDto request, CancellationToken cancellationToken)
    {
        var result = await archiveAppService.ExecuteAutoFixAsync(request.ToApplicationRequest(), cancellationToken);
        return result.IsSuccess ? Ok(result) : BadRequest(result);
    }

    /// <summary>
    /// 预检 BulkCopy 归档条件。
    /// </summary>
    [HttpPost("bulkcopy/inspect")]
    [ProducesResponseType(typeof(Result<ArchiveInspectionResultDto>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<ArchiveInspectionResultDto>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> InspectBulkCopyAsync([FromBody] BulkCopyArchiveInspectDto request, CancellationToken cancellationToken)
    {
        var result = await archiveAppService.InspectBulkCopyAsync(request.ToApplicationRequest(), cancellationToken);
        return result.IsSuccess ? Ok(result) : BadRequest(result);
    }

    /// <summary>
    /// 提交 BulkCopy 归档任务。
    /// </summary>
    [HttpPost("bulkcopy/execute")]
    [ProducesResponseType(typeof(Result<Guid>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result<Guid>), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteBulkCopyAsync([FromBody] BulkCopyArchiveExecuteDto request, CancellationToken cancellationToken)
    {
        var result = await archiveAppService.ExecuteWithBulkCopyAsync(request.ToApplicationRequest(), cancellationToken);
        return result.IsSuccess
            ? Ok(Result<Guid>.Success(result.Value))
            : BadRequest(Result<Guid>.Failure(result.Error!));
    }
}
