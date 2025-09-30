using DbArchiveTool.Domain.ArchiveTasks;
using DbArchiveTool.Shared.Results;
using System.Linq;

namespace DbArchiveTool.Application.ArchiveTasks;

internal sealed class ArchiveTaskQueryService : IArchiveTaskQueryService
{
    private readonly IArchiveTaskRepository _repository;

    public ArchiveTaskQueryService(IArchiveTaskRepository repository)
    {
        _repository = repository;
    }

    public async Task<PagedResult<ArchiveTaskDto>> GetTasksAsync(int page, int pageSize, CancellationToken cancellationToken)
    {
        // 骨架阶段先返回待处理任务列表，后续可扩展分页查询
        var items = await _repository.ListPendingAsync(cancellationToken);

        var dtos = items
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Select(task => new ArchiveTaskDto(
                task.Id,
                task.DataSourceId,
                task.SourceTableName,
                task.TargetTableName,
                (int)task.Status,
                task.IsAutoArchive,
                task.StartedAtUtc,
                task.CompletedAtUtc,
                task.SourceRowCount,
                task.TargetRowCount,
                task.LegacyOperationRecordId
            ))
            .ToList();

        return new PagedResult<ArchiveTaskDto>(dtos, items.Count, page, pageSize);
    }
}
