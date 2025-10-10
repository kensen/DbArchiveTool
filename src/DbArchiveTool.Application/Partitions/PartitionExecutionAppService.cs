using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区执行任务的应用服务实现。
/// </summary>
internal sealed class PartitionExecutionAppService : IPartitionExecutionAppService
{
    private readonly IPartitionConfigurationRepository configurationRepository;
    private readonly IPartitionExecutionTaskRepository taskRepository;
    private readonly IPartitionExecutionLogRepository logRepository;
    private readonly IPartitionExecutionDispatcher dispatcher;
    private readonly ILogger<PartitionExecutionAppService> logger;

    public PartitionExecutionAppService(
        IPartitionConfigurationRepository configurationRepository,
        IPartitionExecutionTaskRepository taskRepository,
        IPartitionExecutionLogRepository logRepository,
        IPartitionExecutionDispatcher dispatcher,
        ILogger<PartitionExecutionAppService> logger)
    {
        this.configurationRepository = configurationRepository;
        this.taskRepository = taskRepository;
        this.logRepository = logRepository;
        this.dispatcher = dispatcher;
        this.logger = logger;
    }

    public async Task<Result<Guid>> StartAsync(StartPartitionExecutionRequest request, CancellationToken cancellationToken = default)
    {
        var validation = ValidateStartRequest(request);
        if (!validation.IsSuccess)
        {
            return Result<Guid>.Failure(validation.Error!);
        }

        var configuration = await configurationRepository.GetByIdAsync(request.PartitionConfigurationId, cancellationToken);
        if (configuration is null)
        {
            return Result<Guid>.Failure("未找到分区配置草稿，请刷新后重试。");
        }

        if (configuration.IsCommitted)
        {
            return Result<Guid>.Failure("该分区配置已执行，无需重复提交。");
        }

        if (configuration.ArchiveDataSourceId != request.DataSourceId)
        {
            return Result<Guid>.Failure("请求中的数据源与配置草稿不一致。");
        }

        if (await taskRepository.HasActiveTaskAsync(request.DataSourceId, cancellationToken))
        {
            return Result<Guid>.Failure("当前数据源已有正在执行的分区任务，请稍后再试。");
        }

        var task = PartitionExecutionTask.Create(
            configuration.Id,
            configuration.ArchiveDataSourceId,
            request.RequestedBy,
            request.RequestedBy,
            request.BackupReference,
            request.Notes,
            request.Priority);

        try
        {
            await taskRepository.AddAsync(task, cancellationToken);
            await dispatcher.DispatchAsync(task.Id, task.DataSourceId, cancellationToken);
            logger.LogInformation("Partition execution task {TaskId} created for configuration {ConfigurationId}", task.Id, configuration.Id);
            return Result<Guid>.Success(task.Id);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to enqueue partition execution for configuration {ConfigurationId}", configuration.Id);
            return Result<Guid>.Failure("PARTITION_EXECUTION_ENQUEUE_FAILED");
        }
    }

    public async Task<Result<PartitionExecutionTaskDetailDto>> GetAsync(Guid executionTaskId, CancellationToken cancellationToken = default)
    {
        if (executionTaskId == Guid.Empty)
        {
            return Result<PartitionExecutionTaskDetailDto>.Failure("任务标识不能为空。");
        }

        var task = await taskRepository.GetByIdAsync(executionTaskId, cancellationToken);
        if (task is null)
        {
            return Result<PartitionExecutionTaskDetailDto>.Failure("未找到分区执行任务。");
        }

        var dto = MapToDetail(task);
        return Result<PartitionExecutionTaskDetailDto>.Success(dto);
    }

    public async Task<Result<List<PartitionExecutionTaskSummaryDto>>> ListAsync(Guid? dataSourceId, int maxCount, CancellationToken cancellationToken = default)
    {
        var tasks = await taskRepository.ListRecentAsync(dataSourceId, maxCount, cancellationToken);
        var items = tasks.Select(MapToSummary).ToList();
        return Result<List<PartitionExecutionTaskSummaryDto>>.Success(items);
    }

    public async Task<Result<List<PartitionExecutionLogDto>>> GetLogsAsync(Guid executionTaskId, DateTime? sinceUtc, int take, CancellationToken cancellationToken = default)
    {
        if (executionTaskId == Guid.Empty)
        {
            return Result<List<PartitionExecutionLogDto>>.Failure("任务标识不能为空。");
        }

        if (sinceUtc.HasValue && sinceUtc.Value.Kind != DateTimeKind.Utc)
        {
            sinceUtc = DateTime.SpecifyKind(sinceUtc.Value, DateTimeKind.Utc);
        }

        var logs = await logRepository.ListAsync(executionTaskId, sinceUtc, take, cancellationToken);
        var dtos = logs.Select(x => new PartitionExecutionLogDto
        {
            Id = x.Id,
            ExecutionTaskId = x.ExecutionTaskId,
            LogTimeUtc = x.LogTimeUtc,
            Category = x.Category,
            Title = x.Title,
            Message = x.Message,
            DurationMs = x.DurationMs,
            ExtraJson = x.ExtraJson
        }).ToList();

        return Result<List<PartitionExecutionLogDto>>.Success(dtos);
    }

    private static Result ValidateStartRequest(StartPartitionExecutionRequest request)
    {
        if (request is null)
        {
            return Result.Failure("请求不能为空。");
        }

        if (request.PartitionConfigurationId == Guid.Empty)
        {
            return Result.Failure("配置标识不能为空。");
        }

        if (request.DataSourceId == Guid.Empty)
        {
            return Result.Failure("数据源标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(request.RequestedBy))
        {
            return Result.Failure("执行人不能为空。");
        }

        if (!request.BackupConfirmed)
        {
            return Result.Failure("请先确认已完成最近一次备份。");
        }

        return Result.Success();
    }

    private static PartitionExecutionTaskSummaryDto MapToSummary(PartitionExecutionTask task)
    {
        return new PartitionExecutionTaskSummaryDto
        {
            Id = task.Id,
            PartitionConfigurationId = task.PartitionConfigurationId,
            DataSourceId = task.DataSourceId,
            Status = task.Status,
            Phase = task.Phase,
            Progress = task.Progress,
            CreatedAtUtc = task.CreatedAtUtc,
            StartedAtUtc = task.StartedAtUtc,
            CompletedAtUtc = task.CompletedAtUtc,
            RequestedBy = task.RequestedBy,
            FailureReason = task.FailureReason,
            BackupReference = task.BackupReference
        };
    }

    private static PartitionExecutionTaskDetailDto MapToDetail(PartitionExecutionTask task)
    {
        var summary = MapToSummary(task);
        return new PartitionExecutionTaskDetailDto
        {
            Id = summary.Id,
            PartitionConfigurationId = summary.PartitionConfigurationId,
            DataSourceId = summary.DataSourceId,
            Status = summary.Status,
            Phase = summary.Phase,
            Progress = summary.Progress,
            CreatedAtUtc = summary.CreatedAtUtc,
            StartedAtUtc = summary.StartedAtUtc,
            CompletedAtUtc = summary.CompletedAtUtc,
            RequestedBy = summary.RequestedBy,
            FailureReason = summary.FailureReason,
            BackupReference = summary.BackupReference,
            SummaryJson = task.SummaryJson,
            Notes = task.Notes
        };
    }
}
