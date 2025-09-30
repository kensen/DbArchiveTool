using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.ArchiveTasks;

public interface IArchiveTaskRepository
{
    Task<ArchiveTask?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<ArchiveTask>> ListPendingAsync(CancellationToken cancellationToken = default);
    Task AddAsync(ArchiveTask task, CancellationToken cancellationToken = default);
    Task UpdateAsync(ArchiveTask task, CancellationToken cancellationToken = default);
}
