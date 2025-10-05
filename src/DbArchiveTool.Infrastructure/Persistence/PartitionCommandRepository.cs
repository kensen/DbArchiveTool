using DbArchiveTool.Domain.Partitions;
using Microsoft.EntityFrameworkCore;

namespace DbArchiveTool.Infrastructure.Persistence;

/// <summary>
/// Entity Framework 的分区命令仓储实现，负责读写 PartitionCommand 聚合根。
/// </summary>
internal sealed class PartitionCommandRepository : IPartitionCommandRepository
{
    private readonly ArchiveDbContext context;

    public PartitionCommandRepository(ArchiveDbContext context)
    {
        this.context = context;
    }

    public async Task AddAsync(PartitionCommand command, CancellationToken cancellationToken = default)
    {
        await context.PartitionCommands.AddAsync(command, cancellationToken);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async Task<PartitionCommand?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        return await context.PartitionCommands.FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
    }

    public async Task UpdateAsync(PartitionCommand command, CancellationToken cancellationToken = default)
    {
        context.PartitionCommands.Update(command);
        await context.SaveChangesAsync(cancellationToken);
    }
}
