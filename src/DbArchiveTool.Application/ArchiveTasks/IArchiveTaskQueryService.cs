using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.ArchiveTasks;

public interface IArchiveTaskQueryService
{
    Task<PagedResult<ArchiveTaskDto>> GetTasksAsync(int page, int pageSize, CancellationToken cancellationToken);
}
