namespace DbArchiveTool.Application.AdminUsers;

/// <summary>管理员注册请求。</summary>
public sealed class RegisterAdminUserRequest
{
    /// <summary>输入的用户名。</summary>
    public string UserName { get; set; } = string.Empty;

    /// <summary>输入的密码。</summary>
    public string Password { get; set; } = string.Empty;

    /// <summary>确认密码。</summary>
    public string ConfirmPassword { get; set; } = string.Empty;
}
