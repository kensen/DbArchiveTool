using DbArchiveTool.Domain.ArchiveTasks;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.ArchiveTasks;

internal sealed class ArchiveTaskCommandService : IArchiveTaskCommandService
{
    private readonly IArchiveTaskRepository _repository;
    private readonly ILogger<ArchiveTaskCommandService> _logger;

    public ArchiveTaskCommandService(IArchiveTaskRepository repository, ILogger<ArchiveTaskCommandService> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    public async Task<Result<Guid>> EnqueueArchiveTaskAsync(CreateArchiveTaskRequest request, CancellationToken cancellationToken)
    {
        try
        {
            var entity = new ArchiveTask(request.DataSourceId, request.SourceTableName, request.TargetTableName, request.IsAutoArchive);

            if (!string.IsNullOrWhiteSpace(request.LegacyOperationRecordId))
            {
                entity.UseLegacyId(request.LegacyOperationRecordId!);
            }

            await _repository.AddAsync(entity, cancellationToken);

            _logger.LogInformation("Archive task {ArchiveTaskId} enqueued for source table {SourceTable}", entity.Id, entity.SourceTableName);

            return Result<Guid>.Success(entity.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to enqueue archive task for table {SourceTable}", request.SourceTableName);
            return Result<Guid>.Failure("ARCHIVE_TASK_CREATE_FAILED");
        }
    }
}
