namespace DbArchiveTool.Domain.AdminUsers;

/// <summary>管理员用户仓储接口,用于访问和维护管理员数据。</summary>
public interface IAdminUserRepository
{
    /// <summary>判断系统中是否存在管理员账户。</summary>
    /// <returns>若存在返回 true。</returns>
    Task<bool> AnyAsync(CancellationToken cancellationToken = default);

    /// <summary>根据用户名获取管理员账户。</summary>
    /// <param name="userName">登录用户名。</param>
    /// <returns>找到的管理员实体或 null。</returns>
    Task<AdminUser?> GetByUserNameAsync(string userName, CancellationToken cancellationToken = default);

    /// <summary>新增管理员账户。</summary>
    /// <param name="adminUser">管理员实体。</param>
    Task AddAsync(AdminUser adminUser, CancellationToken cancellationToken = default);
}
