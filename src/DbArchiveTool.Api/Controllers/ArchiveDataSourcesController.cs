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

    /// <summary>更新数据源目标服务器配置。</summary>
    [HttpPut("{id:guid}/target-server")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> UpdateTargetServerAsync(Guid id, [FromBody] UpdateTargetServerConfigDto dto, CancellationToken cancellationToken)
    {
        // 先获取数据源以便保留其他字段
        var getResult = await _appService.GetByIdAsync(id, cancellationToken);
        if (!getResult.IsSuccess)
        {
            return NotFound(new { error = getResult.Error });
        }

        var dataSource = getResult.Value!; // 此处已经在上面验证了非空

        // 构造完整的更新请求
        var request = new UpdateArchiveDataSourceRequest
        {
            Id = id,
            Name = dataSource.Name,
            Description = dataSource.Description,
            ServerAddress = dataSource.ServerAddress,
            ServerPort = dataSource.ServerPort,
            DatabaseName = dataSource.DatabaseName,
            UseIntegratedSecurity = dataSource.UseIntegratedSecurity,
            UserName = dataSource.UserName,
            Password = null, // 保持原密码
            UseSourceAsTarget = dto.UseSourceAsTarget,
            TargetServerAddress = dto.TargetServerAddress,
            TargetServerPort = dto.TargetServerPort,
            TargetDatabaseName = dto.TargetDatabaseName,
            TargetUseIntegratedSecurity = dto.TargetUseIntegratedSecurity,
            TargetUserName = dto.TargetUserName,
            TargetPassword = dto.TargetPassword,
            OperatorName = "WebUser"
        };

        var result = await _appService.UpdateAsync(request, cancellationToken);
        if (!result.IsSuccess)
        {
            return Problem(title: "更新目标服务器配置失败", detail: result.Error, statusCode: StatusCodes.Status400BadRequest);
        }

        return NoContent();
    }
}

/// <summary>更新目标服务器配置DTO。</summary>
public sealed record UpdateTargetServerConfigDto
{
    /// <summary>是否使用源服务器作为目标服务器。</summary>
    public bool UseSourceAsTarget { get; init; } = true;
    /// <summary>目标服务器地址。</summary>
    public string? TargetServerAddress { get; init; }
    /// <summary>目标服务器端口。</summary>
    public int TargetServerPort { get; init; } = 1433;
    /// <summary>目标数据库名称。</summary>
    public string? TargetDatabaseName { get; init; }
    /// <summary>目标服务器是否使用集成身份验证。</summary>
    public bool TargetUseIntegratedSecurity { get; init; } = true;
    /// <summary>目标服务器用户名。</summary>
    public string? TargetUserName { get; init; }
    /// <summary>目标服务器密码。</summary>
    public string? TargetPassword { get; init; }
}
