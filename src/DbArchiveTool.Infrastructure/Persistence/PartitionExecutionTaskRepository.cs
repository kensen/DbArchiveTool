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
internal sealed class PartitionExecutionTaskRepository : IPartitionExecutionTaskRepository
{
    private static readonly PartitionExecutionStatus[] ActiveStatuses =
    {
        PartitionExecutionStatus.PendingValidation,
        PartitionExecutionStatus.Validating,
        PartitionExecutionStatus.Queued,
        PartitionExecutionStatus.Running
    };

    private static readonly PartitionExecutionStatus[] MonitorStatuses =
    {
        PartitionExecutionStatus.Validating,
        PartitionExecutionStatus.Queued,
        PartitionExecutionStatus.Running
    };

    private readonly ArchiveDbContext context;

    public PartitionExecutionTaskRepository(ArchiveDbContext context)
    {
        this.context = context;
    }

    public async Task AddAsync(PartitionExecutionTask task, CancellationToken cancellationToken = default)
    {
        await context.PartitionExecutionTasks.AddAsync(task, cancellationToken);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async Task UpdateAsync(PartitionExecutionTask task, CancellationToken cancellationToken = default)
    {
        context.PartitionExecutionTasks.Update(task);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async Task<PartitionExecutionTask?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        return await context.PartitionExecutionTasks
            .FirstOrDefaultAsync(x => x.Id == id && !x.IsDeleted, cancellationToken);
    }

    public async Task<bool> HasActiveTaskAsync(Guid dataSourceId, CancellationToken cancellationToken = default)
    {
        return await context.PartitionExecutionTasks
            .AnyAsync(
                x => x.DataSourceId == dataSourceId &&
                     !x.IsDeleted &&
                     ActiveStatuses.Contains(x.Status),
                cancellationToken);
    }

    public async Task<IReadOnlyList<PartitionExecutionTask>> ListRecentAsync(Guid? dataSourceId, int maxCount, CancellationToken cancellationToken = default)
    {
        if (maxCount <= 0)
        {
            maxCount = 20;
        }

        var query = context.PartitionExecutionTasks
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

    public async Task<IReadOnlyList<PartitionExecutionTask>> ListStaleAsync(TimeSpan heartbeatTimeout, CancellationToken cancellationToken = default)
    {
        if (heartbeatTimeout <= TimeSpan.Zero)
        {
            heartbeatTimeout = TimeSpan.FromMinutes(5);
        }

        var threshold = DateTime.UtcNow - heartbeatTimeout;
        return await context.PartitionExecutionTasks
            .Where(x => !x.IsDeleted &&
                        MonitorStatuses.Contains(x.Status) &&
                        x.LastHeartbeatUtc < threshold)
            .OrderBy(x => x.LastHeartbeatUtc)
            .ToListAsync(cancellationToken);
    }
}
