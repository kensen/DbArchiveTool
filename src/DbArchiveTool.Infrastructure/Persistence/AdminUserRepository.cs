using DbArchiveTool.Domain.AdminUsers;
using Microsoft.EntityFrameworkCore;

namespace DbArchiveTool.Infrastructure.Persistence;

/// <summary>管理员用户仓储实现。</summary>
internal sealed class AdminUserRepository : IAdminUserRepository
{
    private readonly ArchiveDbContext _context;

    public AdminUserRepository(ArchiveDbContext context)
    {
        _context = context;
    }

    public async Task<bool> AnyAsync(CancellationToken cancellationToken = default)
    {
        return await _context.AdminUsers.AnyAsync(cancellationToken);
    }

    public async Task<AdminUser?> GetByUserNameAsync(string userName, CancellationToken cancellationToken = default)
    {
        return await _context.AdminUsers.FirstOrDefaultAsync(x => x.UserName == userName, cancellationToken);
    }

    public async Task AddAsync(AdminUser adminUser, CancellationToken cancellationToken = default)
    {
        await _context.AdminUsers.AddAsync(adminUser, cancellationToken);
        await _context.SaveChangesAsync(cancellationToken);
    }
}
