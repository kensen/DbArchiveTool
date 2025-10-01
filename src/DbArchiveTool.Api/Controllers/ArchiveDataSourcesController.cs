using DbArchiveTool.Application.DataSources;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers;

/// <summary>归档数据源配置接口。</summary>
[ApiController]
[Route("api/v1/archive-data-sources")]
public sealed class ArchiveDataSourcesController : ControllerBase
{
    private readonly IArchiveDataSourceAppService _appService;

    public ArchiveDataSourcesController(IArchiveDataSourceAppService appService)
    {
        _appService = appService;
    }

    /// <summary>获取已配置的数据源列表。</summary>
    [HttpGet]
    [ProducesResponseType(typeof(IReadOnlyList<ArchiveDataSourceDto>), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetAsync(CancellationToken cancellationToken)
    {
        var result = await _appService.GetAsync(cancellationToken);
        if (!result.IsSuccess)
        {
            return Problem(title: "获取数据源列表失败", detail: result.Error, statusCode: StatusCodes.Status503ServiceUnavailable);
        }

        return Ok(result.Value);
    }

    /// <summary>新增数据源。</summary>
    [HttpPost]
    [ProducesResponseType(typeof(Guid), StatusCodes.Status201Created)]
    public async Task<IActionResult> CreateAsync([FromBody] CreateArchiveDataSourceRequest request, CancellationToken cancellationToken)
    {
        var result = await _appService.CreateAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            return Problem(title: "新增数据源失败", detail: result.Error, statusCode: StatusCodes.Status400BadRequest);
        }

        return Created($"/api/v1/archive-data-sources/{result.Value}", result.Value);
    }

    /// <summary>测试数据库连接。</summary>
    [HttpPost("test-connection")]
    [ProducesResponseType(typeof(bool), StatusCodes.Status200OK)]
    public async Task<IActionResult> TestConnectionAsync([FromBody] TestArchiveDataSourceRequest request, CancellationToken cancellationToken)
    {
        var result = await _appService.TestConnectionAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            return Problem(title: "测试连接失败", detail: result.Error, statusCode: StatusCodes.Status400BadRequest);
        }

        return Ok(result.Value);
    }
}
