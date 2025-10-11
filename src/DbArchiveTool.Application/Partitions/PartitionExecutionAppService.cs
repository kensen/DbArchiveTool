using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Application.Partitions.Dtos;
using DbArchiveTool.Domain.DataSources;
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
    private readonly IDataSourceRepository dataSourceRepository;
    private readonly ILogger<PartitionExecutionAppService> logger;

    public PartitionExecutionAppService(
        IPartitionConfigurationRepository configurationRepository,
        IPartitionExecutionTaskRepository taskRepository,
        IPartitionExecutionLogRepository logRepository,
        IPartitionExecutionDispatcher dispatcher,
        IDataSourceRepository dataSourceRepository,
        ILogger<PartitionExecutionAppService> logger)
    {
        this.configurationRepository = configurationRepository;
        this.taskRepository = taskRepository;
        this.logRepository = logRepository;
        this.dispatcher = dispatcher;
        this.dataSourceRepository = dataSourceRepository;
        this.logger = logger;
    }

    public async Task<Result<Guid>> StartAsync(StartPartitionExecutionRequest request, CancellationToken cancellationToken = default)
    {
        // 1. 基础参数校验
        var validation = ValidateStartRequest(request);
        if (!validation.IsSuccess)
        {
            logger.LogWarning("分区执行请求校验失败：{Error}", validation.Error);
            return Result<Guid>.Failure(validation.Error!);
        }

        // 2. 加载并校验分区配置
        var configuration = await configurationRepository.GetByIdAsync(request.PartitionConfigurationId, cancellationToken);
        if (configuration is null)
        {
            logger.LogWarning("未找到分区配置草稿：{ConfigurationId}", request.PartitionConfigurationId);
            return Result<Guid>.Failure("未找到分区配置草稿，请刷新后重试。");
        }

        if (configuration.IsCommitted)
        {
            logger.LogWarning("配置 {ConfigurationId} 已执行，拒绝重复提交。", configuration.Id);
            return Result<Guid>.Failure("该分区配置已执行，无需重复提交。如需修改，请创建新的配置草稿。");
        }

        if (configuration.Boundaries.Count == 0)
        {
            logger.LogWarning("配置 {ConfigurationId} 未设置分区边界。", configuration.Id);
            return Result<Guid>.Failure("分区配置尚未设置任何边界值，无法执行。请先在配置向导中添加分区边界。");
        }

        if (configuration.ArchiveDataSourceId != request.DataSourceId)
        {
            logger.LogWarning(
                "数据源不一致：请求 {RequestDataSource}，配置 {ConfigDataSource}",
                request.DataSourceId, configuration.ArchiveDataSourceId);
            return Result<Guid>.Failure("请求中的数据源与配置草稿不一致，请检查参数。");
        }

        // 3. 检查是否存在重复任务（同一配置的运行中任务）
        var existingTasks = await taskRepository.ListRecentAsync(request.DataSourceId, 100, cancellationToken);
        var duplicateTask = existingTasks.FirstOrDefault(t =>
            t.PartitionConfigurationId == configuration.Id &&
            !t.IsCompleted);

        if (duplicateTask is not null)
        {
            logger.LogWarning(
                "配置 {ConfigurationId} 已有未完成的任务 {TaskId}（状态：{Status}）",
                configuration.Id, duplicateTask.Id, duplicateTask.Status);
            return Result<Guid>.Failure(
                $"该配置已有正在执行的任务（任务ID：{duplicateTask.Id}，状态：{duplicateTask.Status}），请等待完成后再试。");
        }

        // 4. 检查数据源是否有其他运行中的任务（确保串行执行）
        if (await taskRepository.HasActiveTaskAsync(request.DataSourceId, cancellationToken))
        {
            logger.LogWarning("数据源 {DataSourceId} 已有运行中的任务。", request.DataSourceId);
            return Result<Guid>.Failure("当前数据源已有正在执行的分区任务，请稍后再试。");
        }

        // 5. 备份确认二次校验（必须明确勾选）
        if (!request.BackupConfirmed)
        {
            logger.LogWarning("用户 {User} 未确认备份即尝试执行。", request.RequestedBy);
            return Result<Guid>.Failure("执行分区操作前，必须确认已完成最近一次数据备份。请勾选\"已完成备份\"复选框后重试。");
        }

        // 6. 备份参考信息建议（非强制，但记录警告）
        if (string.IsNullOrWhiteSpace(request.BackupReference))
        {
            logger.LogWarning(
                "用户 {User} 未提供备份参考信息，配置：{ConfigurationId}",
                request.RequestedBy, configuration.Id);
            // 不阻止执行，但记录警告日志
        }

        // 6. 备份参考信息建议（非强制，但记录警告）
        if (string.IsNullOrWhiteSpace(request.BackupReference))
        {
            logger.LogWarning(
                "用户 {User} 未提供备份参考信息，配置：{ConfigurationId}",
                request.RequestedBy, configuration.Id);
            // 不阻止执行，但记录警告日志
        }

        // 7. 创建执行任务
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

            // 8. 记录初始日志
            var initialLog = PartitionExecutionLogEntry.Create(
                task.Id,
                "Info",
                "任务创建",
                $"由 {request.RequestedBy} 发起的分区执行任务已创建。" +
                $"备份确认: {(request.BackupConfirmed ? "是" : "否")}，" +
                $"参考: {request.BackupReference ?? "无"}。" +
                $"目标配置：{configuration.SchemaName}.{configuration.TableName}，" +
                $"分区边界数量：{configuration.Boundaries.Count}。");

            await logRepository.AddAsync(initialLog, cancellationToken);

            // 9. 派发到执行队列
            await dispatcher.DispatchAsync(task.Id, task.DataSourceId, cancellationToken);

            logger.LogInformation(
                "分区执行任务已创建并入队：TaskId={TaskId}, ConfigurationId={ConfigurationId}, RequestedBy={User}",
                task.Id, configuration.Id, request.RequestedBy);

            return Result<Guid>.Success(task.Id);
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "创建并入队分区执行任务失败：ConfigurationId={ConfigurationId}",
                configuration.Id);

            return Result<Guid>.Failure(
                "创建分区执行任务时发生错误，请稍后重试。如问题持续，请联系管理员。");
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

    /// <inheritdoc />
    public async Task<Result> CancelAsync(Guid executionTaskId, string cancelledBy, string? reason = null, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("开始取消执行任务: ExecutionTaskId={ExecutionTaskId}, CancelledBy={CancelledBy}, Reason={Reason}",
            executionTaskId, cancelledBy, reason);

        // 1. 基本参数校验
        if (executionTaskId == Guid.Empty)
        {
            logger.LogWarning("取消任务失败: 任务标识为空");
            return Result.Failure("任务标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(cancelledBy))
        {
            logger.LogWarning("取消任务失败: 执行人为空");
            return Result.Failure("取消人不能为空。");
        }

        // 2. 加载任务
        var task = await taskRepository.GetByIdAsync(executionTaskId, cancellationToken);
        if (task is null)
        {
            logger.LogWarning("取消任务失败: 任务不存在 ExecutionTaskId={ExecutionTaskId}", executionTaskId);
            return Result.Failure($"任务不存在: {executionTaskId}");
        }

        logger.LogInformation("任务加载成功: Status={Status}, Phase={Phase}, Progress={Progress}",
            task.Status, task.Phase, task.Progress);

        // 3. 检查任务状态是否可取消
        if (task.IsCompleted)
        {
            logger.LogWarning("取消任务失败: 任务已结束 Status={Status}", task.Status);
            return Result.Failure($"任务已结束，无法取消。当前状态: {GetStatusDisplayName(task.Status)}");
        }

        // 4. 检查任务是否已在运行(运行中的任务只能标记为取消,可能需要等待当前步骤完成)
        if (task.Status == PartitionExecutionStatus.Running)
        {
            logger.LogInformation("任务正在运行，将标记为取消中，等待当前步骤完成: Phase={Phase}", task.Phase);
            // 运行中的任务可以被取消,但需要等待当前步骤完成
        }

        // 5. 执行取消操作
        var cancelReason = string.IsNullOrWhiteSpace(reason) ? $"由 {cancelledBy} 取消" : reason;
        task.Cancel(cancelReason);

        logger.LogInformation("任务已标记为已取消: Reason={Reason}", cancelReason);

        // 6. 保存任务状态
        await taskRepository.UpdateAsync(task, cancellationToken);

        logger.LogInformation("任务取消状态已保存");

        // 7. 记录取消日志
        var log = PartitionExecutionLogEntry.Create(
            task.Id,
            "Cancel",
            "任务已取消",
            $"任务已被 {cancelledBy} 取消。原因: {cancelReason}");

        await logRepository.AddAsync(log, cancellationToken);

        logger.LogInformation("取消日志已记录: LogId={LogId}", log.Id);
        logger.LogInformation("执行任务取消完成: ExecutionTaskId={ExecutionTaskId}", executionTaskId);

        return Result.Success();
    }

    public async Task<Result<ExecutionWizardContextDto>> GetExecutionContextAsync(
        Guid configurationId, 
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("开始获取执行向导上下文: ConfigurationId={ConfigurationId}", configurationId);

        // 1. 加载分区配置
        var configuration = await configurationRepository.GetByIdAsync(configurationId, cancellationToken);
        if (configuration is null)
        {
            logger.LogWarning("未找到分区配置: ConfigurationId={ConfigurationId}", configurationId);
            return Result<ExecutionWizardContextDto>.Failure("未找到指定的分区配置");
        }

        // 2. 加载数据源信息
        var dataSource = await dataSourceRepository.GetAsync(configuration.ArchiveDataSourceId, cancellationToken);
        var dataSourceName = dataSource != null 
            ? $"{dataSource.ServerAddress}\\{dataSource.DatabaseName}"
            : "未知数据源";

        // 3. 构建上下文DTO
        var context = new ExecutionWizardContextDto
        {
            ConfigurationId = configuration.Id,
            DataSourceId = configuration.ArchiveDataSourceId,
            DataSourceName = dataSourceName,
            SchemaName = configuration.SchemaName,
            TableName = configuration.TableName,
            FullTableName = $"{configuration.SchemaName}.{configuration.TableName}",
            PartitionFunctionName = configuration.PartitionFunctionName,
            PartitionSchemeName = configuration.PartitionSchemeName,
            PartitionColumnName = configuration.PartitionColumn.Name,
            PartitionColumnType = configuration.PartitionColumn.ValueKind.ToString(),
            IsRangeRight = configuration.IsRangeRight,
            RequirePartitionColumnNotNull = configuration.RequirePartitionColumnNotNull,
            PrimaryFilegroup = configuration.FilegroupStrategy.PrimaryFilegroup,
            AdditionalFilegroups = configuration.FilegroupStrategy.AdditionalFilegroups.ToList(),
            Boundaries = configuration.Boundaries.Select(b => new PartitionBoundaryDto
            {
                SortKey = b.SortKey,
                RawValue = b.Value.ToInvariantString(),
                DisplayValue = b.Value.ToInvariantString()
            }).ToList(),
            Remarks = configuration.Remarks,
            ExecutionStage = configuration.ExecutionStage,
            IsCommitted = configuration.IsCommitted
        };

        logger.LogInformation("执行向导上下文获取成功: ConfigurationId={ConfigurationId}, DataSource={DataSource}", 
            configurationId, dataSourceName);
        return Result<ExecutionWizardContextDto>.Success(context);
    }

    /// <summary>
    /// 获取状态显示名称(中文)。
    /// </summary>
    private static string GetStatusDisplayName(PartitionExecutionStatus status)
    {
        return status switch
        {
            PartitionExecutionStatus.PendingValidation => "待校验",
            PartitionExecutionStatus.Validating => "校验中",
            PartitionExecutionStatus.Queued => "已排队",
            PartitionExecutionStatus.Running => "执行中",
            PartitionExecutionStatus.Succeeded => "已成功",
            PartitionExecutionStatus.Failed => "已失败",
            PartitionExecutionStatus.Cancelled => "已取消",
            _ => status.ToString()
        };
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
