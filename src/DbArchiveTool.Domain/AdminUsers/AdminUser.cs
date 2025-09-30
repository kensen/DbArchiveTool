using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.AdminUsers;

/// <summary>管理员用户实体,用于维护系统登录凭据。</summary>
public sealed class AdminUser : AggregateRoot
{
    /// <summary>管理员登录名。</summary>
    public string UserName { get; private set; } = string.Empty;

    /// <summary>密码哈希值,使用安全算法存储。</summary>
    public string PasswordHash { get; private set; } = string.Empty;

    private AdminUser()
    {
    }

    /// <summary>创建管理员账户。</summary>
    /// <param name="userName">登录用户名。</param>
    /// <param name="passwordHash">已计算的密码哈希值。</param>
    public AdminUser(string userName, string passwordHash)
    {
        UserName = userName;
        PasswordHash = passwordHash;
    }

    /// <summary>更新密码哈希。</summary>
    /// <param name="passwordHash">新的密码哈希。</param>
    /// <param name="operatorName">执行更新的操作人。</param>
    public void UpdatePassword(string passwordHash, string operatorName)
    {
        PasswordHash = passwordHash;
        Touch(operatorName);
    }
}
