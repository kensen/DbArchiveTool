using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.ArchiveTasks;

public interface IArchiveTaskCommandService
{
    Task<Result<Guid>> EnqueueArchiveTaskAsync(CreateArchiveTaskRequest request, CancellationToken cancellationToken);
}
