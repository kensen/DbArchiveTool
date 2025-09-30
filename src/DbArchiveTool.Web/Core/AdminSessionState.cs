using System;

namespace DbArchiveTool.Web.Core;

/// <summary>管理员会话状态管理服务。</summary>
public sealed class AdminSessionState
{
    /// <summary>当前会话是否已通过管理员身份验证。</summary>
    public bool IsAuthenticated { get; private set; }

    /// <summary>当前登录管理员的标识。</summary>
    public Guid? AdminId { get; private set; }

    /// <summary>当前登录管理员的用户名。</summary>
    public string? UserName { get; private set; }

    /// <summary>会话状态变化事件。</summary>
    public event Action? StateChanged;

    /// <summary>标记管理员为已登录。</summary>
    /// <param name="adminId">管理员标识。</param>
    /// <param name="userName">管理员名称。</param>
    public void SignIn(Guid adminId, string userName)
    {
        IsAuthenticated = true;
        AdminId = adminId;
        UserName = userName;
        StateChanged?.Invoke();
    }

    /// <summary>注销管理员会话。</summary>
    public void SignOut()
    {
        IsAuthenticated = false;
        AdminId = null;
        UserName = null;
        StateChanged?.Invoke();
    }
}
