using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using Microsoft.EntityFrameworkCore;

namespace DbArchiveTool.Infrastructure.Persistence;

/// <summary>
/// EF Core 实现的分区执行任务仓储。
/// </summary>
internal sealed class BackgroundTaskRepository : IBackgroundTaskRepository
{
    private static readonly BackgroundTaskStatus[] ActiveStatuses =
    {
        BackgroundTaskStatus.PendingValidation,
        BackgroundTaskStatus.Validating,
        BackgroundTaskStatus.Queued,
        BackgroundTaskStatus.Running
    };

    private static readonly BackgroundTaskStatus[] MonitorStatuses =
    {
        BackgroundTaskStatus.Validating,
        BackgroundTaskStatus.Queued,
        BackgroundTaskStatus.Running
    };

    private readonly ArchiveDbContext context;

    public BackgroundTaskRepository(ArchiveDbContext context)
    {
        this.context = context;
    }

    public async Task AddAsync(BackgroundTask task, CancellationToken cancellationToken = default)
    {
        await context.BackgroundTasks.AddAsync(task, cancellationToken);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async Task UpdateAsync(BackgroundTask task, CancellationToken cancellationToken = default)
    {
        context.BackgroundTasks.Update(task);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async Task<BackgroundTask?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        // 关键修复: 使用 AsNoTracking() 避免实体被跟踪,防止多个 scope 同时更新同一实体时的并发冲突
        return await context.BackgroundTasks
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.Id == id && !x.IsDeleted, cancellationToken);
    }

    public async Task<bool> HasActiveTaskAsync(Guid dataSourceId, CancellationToken cancellationToken = default)
    {
        return await context.BackgroundTasks
            .AnyAsync(
                x => x.DataSourceId == dataSourceId &&
                     !x.IsDeleted &&
                     ActiveStatuses.Contains(x.Status),
                cancellationToken);
    }

    public async Task<IReadOnlyList<BackgroundTask>> ListRecentAsync(Guid? dataSourceId, int maxCount, CancellationToken cancellationToken = default)
    {
        if (maxCount <= 0)
        {
            maxCount = 20;
        }

        var query = context.BackgroundTasks
            .Where(x => !x.IsDeleted);

        if (dataSourceId.HasValue && dataSourceId.Value != Guid.Empty)
        {
            query = query.Where(x => x.DataSourceId == dataSourceId.Value);
        }

        return await query
            .OrderByDescending(x => x.CreatedAtUtc)
            .Take(maxCount)
            .ToListAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<BackgroundTask>> ListStaleAsync(TimeSpan heartbeatTimeout, CancellationToken cancellationToken = default)
    {
        if (heartbeatTimeout <= TimeSpan.Zero)
        {
            heartbeatTimeout = TimeSpan.FromMinutes(5);
        }

        var threshold = DateTime.UtcNow - heartbeatTimeout;
        // 关键修复: 使用 AsNoTracking() 避免实体被跟踪
        return await context.BackgroundTasks
            .AsNoTracking()
            .Where(x => !x.IsDeleted &&
                        MonitorStatuses.Contains(x.Status) &&
                        x.LastHeartbeatUtc < threshold)
            .OrderBy(x => x.LastHeartbeatUtc)
            .ToListAsync(cancellationToken);
    }
}
