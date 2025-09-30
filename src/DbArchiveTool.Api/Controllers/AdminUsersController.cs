using DbArchiveTool.Application.AdminUsers;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers;

/// <summary>管理员账户接口。</summary>
[ApiController]
[Route("api/v1/admin-users")]
public sealed class AdminUsersController : ControllerBase
{
    private readonly IAdminUserAppService _adminUserAppService;

    public AdminUsersController(IAdminUserAppService adminUserAppService)
    {
        _adminUserAppService = adminUserAppService;
    }

    /// <summary>判断系统是否已经存在管理员。</summary>
    [HttpGet("exists")]
    [ProducesResponseType(typeof(bool), StatusCodes.Status200OK)]
    public async Task<IActionResult> HasAdminAsync(CancellationToken cancellationToken)
    {
        var result = await _adminUserAppService.HasAdminAsync(cancellationToken);

        if (!result.IsSuccess)
        {
            return Problem(title: "查询管理员状态失败", detail: result.Error, statusCode: StatusCodes.Status503ServiceUnavailable);
        }

        return Ok(result.Value);
    }

    /// <summary>注册管理员账户。</summary>
    [HttpPost("register")]
    [ProducesResponseType(typeof(Guid), StatusCodes.Status201Created)]
    public async Task<IActionResult> RegisterAsync([FromBody] RegisterAdminUserRequest request, CancellationToken cancellationToken)
    {
        var result = await _adminUserAppService.RegisterAsync(request, cancellationToken);

        if (!result.IsSuccess)
        {
            return Problem(title: "注册管理员失败", detail: result.Error, statusCode: StatusCodes.Status400BadRequest);
        }

        return Created($"/api/v1/admin-users/{result.Value}", result.Value);
    }

    /// <summary>验证管理员登录。</summary>
    [HttpPost("login")]
    [ProducesResponseType(typeof(Guid), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status401Unauthorized)]
    public async Task<IActionResult> LoginAsync([FromBody] AdminLoginRequest request, CancellationToken cancellationToken)
    {
        var result = await _adminUserAppService.LoginAsync(request, cancellationToken);

        if (!result.IsSuccess)
        {
            return Problem(title: "管理员登录失败", detail: result.Error, statusCode: StatusCodes.Status401Unauthorized);
        }

        return Ok(result.Value);
    }
}
