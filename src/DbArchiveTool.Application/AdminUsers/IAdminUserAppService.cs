using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.AdminUsers;

/// <summary>管理员账户应用服务。</summary>
public interface IAdminUserAppService
{
    /// <summary>判断系统中是否已经存在管理员账户。</summary>
    Task<Result<bool>> HasAdminAsync(CancellationToken cancellationToken = default);

    /// <summary>注册管理员账户。</summary>
    Task<Result<Guid>> RegisterAsync(RegisterAdminUserRequest request, CancellationToken cancellationToken = default);

    /// <summary>验证管理员登录。</summary>
    Task<Result<Guid>> LoginAsync(AdminLoginRequest request, CancellationToken cancellationToken = default);
}
