using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using Microsoft.EntityFrameworkCore;

namespace DbArchiveTool.Infrastructure.Persistence;

/// <summary>
/// EF Core 实现的执行日志仓储。
/// </summary>
internal sealed class BackgroundTaskLogRepository : IBackgroundTaskLogRepository
{
    private readonly ArchiveDbContext context;

    public BackgroundTaskLogRepository(ArchiveDbContext context)
    {
        this.context = context;
    }

    public async Task AddAsync(BackgroundTaskLogEntry entry, CancellationToken cancellationToken = default)
    {
        await context.BackgroundTaskLogs.AddAsync(entry, cancellationToken);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async Task AddRangeAsync(IReadOnlyCollection<BackgroundTaskLogEntry> entries, CancellationToken cancellationToken = default)
    {
        if (entries is null || entries.Count == 0)
        {
            return;
        }

        await context.BackgroundTaskLogs.AddRangeAsync(entries, cancellationToken);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<BackgroundTaskLogEntry>> ListAsync(Guid executionTaskId, DateTime? sinceUtc, int take, CancellationToken cancellationToken = default)
    {
        if (take <= 0)
        {
            take = 200;
        }

        var query = context.BackgroundTaskLogs
            .Where(x => x.ExecutionTaskId == executionTaskId && !x.IsDeleted);

        if (sinceUtc.HasValue)
        {
            query = query.Where(x => x.LogTimeUtc >= sinceUtc.Value);
        }

        return await query
            .OrderBy(x => x.LogTimeUtc)
            .ThenBy(x => x.Id)
            .Take(take)
            .ToListAsync(cancellationToken);
    }

    public async Task<BackgroundTaskLogEntry?> GetLatestAsync(Guid executionTaskId, CancellationToken cancellationToken = default)
    {
        return await context.BackgroundTaskLogs
            .Where(x => x.ExecutionTaskId == executionTaskId && !x.IsDeleted)
            .OrderByDescending(x => x.LogTimeUtc)
            .ThenByDescending(x => x.Id)
            .FirstOrDefaultAsync(cancellationToken);
    }
}
