using DbArchiveTool.Domain.ScheduledArchiveJobs;
using Microsoft.EntityFrameworkCore;

namespace DbArchiveTool.Infrastructure.Persistence;

/// <summary>
/// 定时归档任务仓储实现
/// </summary>
internal sealed class ScheduledArchiveJobRepository : IScheduledArchiveJobRepository
{
    private readonly ArchiveDbContext _context;

    public ScheduledArchiveJobRepository(ArchiveDbContext context)
    {
        _context = context;
    }

    public async Task<ScheduledArchiveJob?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        return await _context.ScheduledArchiveJobs
            .FirstOrDefaultAsync(x => x.Id == id && !x.IsDeleted, cancellationToken);
    }

    public async Task<List<ScheduledArchiveJob>> GetAllAsync(CancellationToken cancellationToken = default)
    {
        return await _context.ScheduledArchiveJobs
            .Where(x => !x.IsDeleted)
            .OrderBy(x => x.Name)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<ScheduledArchiveJob>> GetByDataSourceIdAsync(
        Guid dataSourceId,
        CancellationToken cancellationToken = default)
    {
        return await _context.ScheduledArchiveJobs
            .Where(x => x.DataSourceId == dataSourceId && !x.IsDeleted)
            .OrderBy(x => x.Name)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<ScheduledArchiveJob>> GetEnabledJobsAsync(CancellationToken cancellationToken = default)
    {
        return await _context.ScheduledArchiveJobs
            .Where(x => x.IsEnabled && !x.IsDeleted)
            .OrderBy(x => x.NextExecutionAtUtc)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<ScheduledArchiveJob>> GetEnabledJobsByDataSourceAsync(
        Guid dataSourceId,
        CancellationToken cancellationToken = default)
    {
        return await _context.ScheduledArchiveJobs
            .Where(x => x.DataSourceId == dataSourceId && x.IsEnabled && !x.IsDeleted)
            .OrderBy(x => x.NextExecutionAtUtc)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<ScheduledArchiveJob>> GetDueJobsAsync(
        DateTime currentTimeUtc,
        CancellationToken cancellationToken = default)
    {
        return await _context.ScheduledArchiveJobs
            .Where(x => x.IsEnabled
                && !x.IsDeleted
                && x.NextExecutionAtUtc.HasValue
                && x.NextExecutionAtUtc.Value <= currentTimeUtc)
            .OrderBy(x => x.NextExecutionAtUtc)
            .ToListAsync(cancellationToken);
    }

    public async Task<ScheduledArchiveJob?> GetByNameAsync(string name, CancellationToken cancellationToken = default)
    {
        return await _context.ScheduledArchiveJobs
            .FirstOrDefaultAsync(x => x.Name == name && !x.IsDeleted, cancellationToken);
    }

    public async Task<bool> ExistsByNameAsync(
        string name,
        Guid? excludeId = null,
        CancellationToken cancellationToken = default)
    {
        var query = _context.ScheduledArchiveJobs
            .Where(x => x.Name == name && !x.IsDeleted);

        if (excludeId.HasValue)
        {
            query = query.Where(x => x.Id != excludeId.Value);
        }

        return await query.AnyAsync(cancellationToken);
    }

    public async Task AddAsync(ScheduledArchiveJob job, CancellationToken cancellationToken = default)
    {
        await _context.ScheduledArchiveJobs.AddAsync(job, cancellationToken);
        await _context.SaveChangesAsync(cancellationToken);
    }

    public async Task UpdateAsync(ScheduledArchiveJob job, CancellationToken cancellationToken = default)
    {
        _context.ScheduledArchiveJobs.Update(job);
        await _context.SaveChangesAsync(cancellationToken);
    }

    public async Task DeleteAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var job = await GetByIdAsync(id, cancellationToken);
        if (job != null)
        {
            job.MarkDeleted("SYSTEM");
            await _context.SaveChangesAsync(cancellationToken);
        }
    }
}
