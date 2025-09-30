using DbArchiveTool.Domain.ArchiveTasks;
using Microsoft.EntityFrameworkCore;
using System.Linq;

namespace DbArchiveTool.Infrastructure.Persistence;

internal sealed class ArchiveTaskRepository : IArchiveTaskRepository
{
    private readonly ArchiveDbContext _context;

    public ArchiveTaskRepository(ArchiveDbContext context)
    {
        _context = context;
    }

    public async Task AddAsync(ArchiveTask task, CancellationToken cancellationToken = default)
    {
        await _context.ArchiveTasks.AddAsync(task, cancellationToken);
        await _context.SaveChangesAsync(cancellationToken);
    }

    public async Task<ArchiveTask?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        return await _context.ArchiveTasks.FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
    }

    public async Task<IReadOnlyList<ArchiveTask>> ListPendingAsync(CancellationToken cancellationToken = default)
    {
        return await _context.ArchiveTasks
            .Where(x => x.Status == ArchiveTaskStatus.Pending)
            .OrderBy(x => x.CreatedAtUtc)
            .ToListAsync(cancellationToken);
    }

    public async Task UpdateAsync(ArchiveTask task, CancellationToken cancellationToken = default)
    {
        _context.ArchiveTasks.Update(task);
        await _context.SaveChangesAsync(cancellationToken);
    }
}
