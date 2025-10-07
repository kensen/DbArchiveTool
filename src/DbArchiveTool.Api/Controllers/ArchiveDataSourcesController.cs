using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Application.DataSources;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers;

/// <summary>归档数据源管理接口。</summary>
[ApiController]
[Route("api/v1/archive-data-sources")]
public sealed class ArchiveDataSourcesController : ControllerBase
{
    private readonly IArchiveDataSourceAppService _appService;

    public ArchiveDataSourcesController(IArchiveDataSourceAppService appService)
    {
        _appService = appService;
    }

    /// <summary>获取当前可用的数据源列表。</summary>
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

    /// <summary>根据ID获取单个数据源。</summary>
    [HttpGet("{id:guid}")]
    [ProducesResponseType(typeof(ArchiveDataSourceDto), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        var result = await _appService.GetByIdAsync(id, cancellationToken);
        if (!result.IsSuccess)
        {
            return NotFound(new { error = result.Error });
        }

        return Ok(result.Value);
    }

    /// <summary>创建归档数据源。</summary>
    [HttpPost]
    [ProducesResponseType(typeof(Guid), StatusCodes.Status201Created)]
    public async Task<IActionResult> CreateAsync([FromBody] CreateArchiveDataSourceRequest request, CancellationToken cancellationToken)
    {
        var result = await _appService.CreateAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            return Problem(title: "创建数据源失败", detail: result.Error, statusCode: StatusCodes.Status400BadRequest);
        }

        return Created($"/api/v1/archive-data-sources/{result.Value}", result.Value);
    }

    /// <summary>更新归档数据源。</summary>
    [HttpPut("{id:guid}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public async Task<IActionResult> UpdateAsync(Guid id, [FromBody] UpdateArchiveDataSourceRequest request, CancellationToken cancellationToken)
    {
        request.Id = id;
        var result = await _appService.UpdateAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            return Problem(title: "更新数据源失败", detail: result.Error, statusCode: StatusCodes.Status400BadRequest);
        }

        return NoContent();
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
