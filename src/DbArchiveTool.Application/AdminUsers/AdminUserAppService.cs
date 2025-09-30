using DbArchiveTool.Domain.AdminUsers;
using DbArchiveTool.Shared.Results;
using Microsoft.AspNetCore.Identity;

namespace DbArchiveTool.Application.AdminUsers;

/// <summary>管理员账户应用服务实现。</summary>
internal sealed class AdminUserAppService : IAdminUserAppService
{
    private readonly IAdminUserRepository _repository;
    private readonly IPasswordHasher<AdminUser> _passwordHasher;

    public AdminUserAppService(IAdminUserRepository repository, IPasswordHasher<AdminUser> passwordHasher)
    {
        _repository = repository;
        _passwordHasher = passwordHasher;
    }

    public async Task<Result<bool>> HasAdminAsync(CancellationToken cancellationToken = default)
    {
        var exists = await _repository.AnyAsync(cancellationToken);
        return Result<bool>.Success(exists);
    }

    public async Task<Result<Guid>> RegisterAsync(RegisterAdminUserRequest request, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(request.UserName))
        {
            return Result<Guid>.Failure("用户名不能为空");
        }

        if (request.UserName.Length < 4)
        {
            return Result<Guid>.Failure("用户名长度至少为 4 个字符");
        }

        if (string.IsNullOrWhiteSpace(request.Password))
        {
            return Result<Guid>.Failure("密码不能为空");
        }

        if (request.Password.Length < 6)
        {
            return Result<Guid>.Failure("密码长度至少为 6 位");
        }

        if (!string.Equals(request.Password, request.ConfirmPassword, StringComparison.Ordinal))
        {
            return Result<Guid>.Failure("两次输入的密码不一致");
        }

        if (await _repository.AnyAsync(cancellationToken))
        {
            return Result<Guid>.Failure("系统已存在管理员账户,不能重复注册");
        }

        var admin = new AdminUser(request.UserName.Trim(), string.Empty);
        var hashed = _passwordHasher.HashPassword(admin, request.Password);
        admin.UpdatePassword(hashed, request.UserName);

        await _repository.AddAsync(admin, cancellationToken);

        return Result<Guid>.Success(admin.Id);
    }

    public async Task<Result<Guid>> LoginAsync(AdminLoginRequest request, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(request.UserName) || string.IsNullOrWhiteSpace(request.Password))
        {
            return Result<Guid>.Failure("用户名或密码不能为空");
        }

        var admin = await _repository.GetByUserNameAsync(request.UserName.Trim(), cancellationToken);
        if (admin is null)
        {
            return Result<Guid>.Failure("用户名或密码不正确");
        }

        var verifyResult = _passwordHasher.VerifyHashedPassword(admin, admin.PasswordHash, request.Password);
        if (verifyResult == PasswordVerificationResult.Failed)
        {
            return Result<Guid>.Failure("用户名或密码不正确");
        }

        return Result<Guid>.Success(admin.Id);
    }
}
