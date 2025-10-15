using DbArchiveTool.Domain.Partitions;

namespace DbArchiveTool.Infrastructure.Persistence;

/// <summary>
/// 基于 EF Core 的分区审计日志仓储实现。
/// </summary>
internal sealed class PartitionAuditLogRepository : IPartitionAuditLogRepository
{
    private readonly ArchiveDbContext context;

    public PartitionAuditLogRepository(ArchiveDbContext context)
    {
        this.context = context;
    }

    public async Task AddAsync(PartitionAuditLog log, CancellationToken cancellationToken = default)
    {
        await context.PartitionAuditLogs.AddAsync(log, cancellationToken);
        await context.SaveChangesAsync(cancellationToken);
    }
}
