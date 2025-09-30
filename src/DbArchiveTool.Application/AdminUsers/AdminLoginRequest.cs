namespace DbArchiveTool.Application.AdminUsers;

/// <summary>管理员登录请求。</summary>
public sealed class AdminLoginRequest
{
    /// <summary>登录用户名。</summary>
    public string UserName { get; set; } = string.Empty;

    /// <summary>登录密码。</summary>
    public string Password { get; set; } = string.Empty;
}
