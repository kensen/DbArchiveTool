using System;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 承担分区执行任务校验、脚本生成与实际执行的处理器。
/// </summary>
internal sealed class PartitionExecutionProcessor
{
    private readonly IPartitionExecutionTaskRepository taskRepository;
    private readonly IPartitionExecutionLogRepository logRepository;
    private readonly IPartitionConfigurationRepository configurationRepository;
    private readonly IDataSourceRepository dataSourceRepository;
    private readonly IPermissionInspectionRepository permissionInspectionRepository;
    private readonly SqlPartitionCommandExecutor commandExecutor;
    private readonly ILogger<PartitionExecutionProcessor> logger;

    public PartitionExecutionProcessor(
        IPartitionExecutionTaskRepository taskRepository,
        IPartitionExecutionLogRepository logRepository,
        IPartitionConfigurationRepository configurationRepository,
        IDataSourceRepository dataSourceRepository,
        IPermissionInspectionRepository permissionInspectionRepository,
        SqlPartitionCommandExecutor commandExecutor,
        ILogger<PartitionExecutionProcessor> logger)
    {
        this.taskRepository = taskRepository;
        this.logRepository = logRepository;
        this.configurationRepository = configurationRepository;
        this.dataSourceRepository = dataSourceRepository;
        this.permissionInspectionRepository = permissionInspectionRepository;
        this.commandExecutor = commandExecutor;
        this.logger = logger;
    }

    public async Task ExecuteAsync(Guid executionTaskId, CancellationToken cancellationToken)
    {
        var task = await taskRepository.GetByIdAsync(executionTaskId, cancellationToken);
        if (task is null)
        {
            logger.LogWarning("Partition execution task {TaskId} not found.", executionTaskId);
            return;
        }

        var overallStopwatch = Stopwatch.StartNew();
        PartitionConfiguration? configuration = null;
        ArchiveDataSource? dataSource = null;

        try
        {
            // ============== 阶段 1: 任务入队与基础校验 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", $"任务由 {task.RequestedBy} 发起，开始执行。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(PartitionExecutionPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.05, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 2: 加载配置与数据源 ==============
            var stepWatch = Stopwatch.StartNew();
            await AppendLogAsync(task.Id, "Step", "加载配置", "正在加载分区配置草稿...", cancellationToken);

            configuration = await configurationRepository.GetByIdAsync(task.PartitionConfigurationId, cancellationToken);
            if (configuration is null)
            {
                await HandleValidationFailureAsync(task, "未找到分区配置草稿。", cancellationToken);
                return;
            }

            dataSource = await dataSourceRepository.GetAsync(task.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                await HandleValidationFailureAsync(task, "未找到归档数据源配置。", cancellationToken);
                return;
            }

            if (configuration.Boundaries.Count == 0)
            {
                await HandleValidationFailureAsync(task, "分区配置中未提供任何边界值。", cancellationToken);
                return;
            }

            stepWatch.Stop();
            await AppendLogAsync(
                task.Id,
                "Info",
                "配置加载完成",
                $"目标表：{configuration.SchemaName}.{configuration.TableName}，分区边界数量：{configuration.Boundaries.Count}",
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            task.UpdateProgress(0.15, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 3: 权限校验 ==============
            stepWatch.Restart();
            var permissionContext = BuildPermissionContext(dataSource, configuration);
            await AppendLogAsync(
                task.Id,
                "Step",
                "权限校验",
                $"正在检查数据库权限...\n{permissionContext}",
                cancellationToken);

            var permissionResults = await permissionInspectionRepository.CheckObjectPermissionsAsync(
                task.DataSourceId,
                configuration.SchemaName,
                configuration.TableName,
                cancellationToken);

            stepWatch.Stop();

            if (permissionResults.Count == 0)
            {
                await AppendLogAsync(
                    task.Id,
                    "Error",
                    "权限校验异常",
                    $"未能获取到当前数据库用户的权限信息，请检查连接账号配置。\n{permissionContext}",
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                await HandleValidationFailureAsync(
                    task,
                    $"权限校验失败：无法确认数据库权限（{permissionContext}）",
                    cancellationToken);
                return;
            }

            var missingPermissions = permissionResults
                .Where(result => !result.Granted)
                .Select(result => result.PermissionName)
                .ToList();

            var grantedPermissions = permissionResults
                .Where(result => result.Granted)
                .Select(result => string.IsNullOrWhiteSpace(result.ScopeDisplayName)
                    ? result.PermissionName
                    : $"{result.PermissionName}({result.ScopeDisplayName})")
                .ToList();

            if (missingPermissions.Count > 0)
            {
                var missingDisplay = string.Join("、", missingPermissions);
                var grantedDisplay = grantedPermissions.Count > 0
                    ? string.Join("、", grantedPermissions)
                    : "无";

                await AppendLogAsync(
                    task.Id,
                    "Error",
                    "权限不足",
                    $"缺少必要权限：{missingDisplay}。当前权限：{grantedDisplay}\n{permissionContext}",
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                await HandleValidationFailureAsync(
                    task,
                    $"权限校验失败：缺少 {missingDisplay}（{permissionContext}）",
                    cancellationToken);
                return;
            }

            var grantedSummary = grantedPermissions.Count > 0
                ? string.Join("、", grantedPermissions)
                : "无";

            await AppendLogAsync(
                task.Id,
                "Info",
                "权限校验通过",
                $"已授权权限：{grantedSummary}\n{permissionContext}",
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            task.UpdateProgress(0.25, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 4: 进入队列 ==============
            task.MarkQueued("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);
            await AppendLogAsync(task.Id, "Step", "进入队列", "校验完成，任务进入执行队列。", cancellationToken);

            // ============== 阶段 5: 开始执行 ==============
            task.MarkRunning("SYSTEM");
            task.UpdatePhase(PartitionExecutionPhases.Executing, "SYSTEM");
            task.UpdateProgress(0.35, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 6: 文件组准备（如果需要） ==============
            if (configuration.FilegroupStrategy is not null &&
                !string.IsNullOrWhiteSpace(configuration.FilegroupStrategy.PrimaryFilegroup) &&
                configuration.FilegroupStrategy.PrimaryFilegroup != "PRIMARY")
            {
                stepWatch.Restart();
                await AppendLogAsync(
                    task.Id,
                    "Step",
                    "文件组准备",
                    $"检查文件组 {configuration.FilegroupStrategy.PrimaryFilegroup} 是否存在...",
                    cancellationToken);

                var created = await commandExecutor.CreateFilegroupIfNeededAsync(
                    task.DataSourceId,
                    dataSource.DatabaseName,
                    configuration.FilegroupStrategy.PrimaryFilegroup,
                    cancellationToken);

                stepWatch.Stop();

                if (created)
                {
                    await AppendLogAsync(
                        task.Id,
                        "Info",
                        "文件组已创建",
                        $"成功创建文件组：{configuration.FilegroupStrategy.PrimaryFilegroup}",
                        cancellationToken,
                        durationMs: stepWatch.ElapsedMilliseconds);
                }
                else
                {
                    await AppendLogAsync(
                        task.Id,
                        "Info",
                        "文件组已存在",
                        $"文件组 {configuration.FilegroupStrategy.PrimaryFilegroup} 已存在，跳过创建。",
                        cancellationToken,
                        durationMs: stepWatch.ElapsedMilliseconds);
                }

                task.UpdateProgress(0.45, "SYSTEM");
                await taskRepository.UpdateAsync(task, cancellationToken);
            }

            // ============== 阶段 6.5: 转换表为分区表 ==============
            stepWatch.Restart();
            await AppendLogAsync(
                task.Id,
                "Step",
                "转换表为分区表",
                $"准备将表 {configuration.SchemaName}.{configuration.TableName} 转换为分区表（保存并重建所有索引到分区方案）...",
                cancellationToken);

            var tableConverted = await commandExecutor.ConvertToPartitionedTableAsync(
                task.DataSourceId,
                configuration,
                cancellationToken);

            stepWatch.Stop();

            if (tableConverted)
            {
                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "表已转换为分区表",
                    $"成功将表 {configuration.SchemaName}.{configuration.TableName} 转换为分区表，所有索引已在分区方案上重建。",
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
            }
            else
            {
                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "表已是分区表",
                    $"表 {configuration.SchemaName}.{configuration.TableName} 已经是分区表，跳过转换。",
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
            }

            task.UpdateProgress(0.6, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 7: 执行分区拆分 ==============
            stepWatch.Restart();
            var boundaryValues = configuration.Boundaries
                .OrderBy(b => b.SortKey, StringComparer.Ordinal)
                .Select(b => b.Value)
                .ToList();

            await AppendLogAsync(
                task.Id,
                "Step",
                "执行分区拆分",
                $"准备拆分 {boundaryValues.Count} 个分区边界...",
                cancellationToken);

            var executionResult = await commandExecutor.ExecuteSplitWithTransactionAsync(
                task.DataSourceId,
                configuration,
                boundaryValues,
                cancellationToken);

            stepWatch.Stop();

            if (!executionResult.IsSuccess)
            {
                await AppendLogAsync(
                    task.Id,
                    "Error",
                    "分区拆分失败",
                    executionResult.Message,
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds,
                    extraJson: JsonSerializer.Serialize(new { errorDetail = executionResult.ErrorDetail }));

                task.MarkFailed("SYSTEM", executionResult.Message);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            await AppendLogAsync(
                task.Id,
                "Info",
                "分区拆分完成",
                executionResult.Message,
                cancellationToken,
                durationMs: executionResult.ElapsedMilliseconds,
                extraJson: JsonSerializer.Serialize(new
                {
                    boundaryCount = boundaryValues.Count,
                    affectedPartitions = executionResult.AffectedCount
                }));

            task.UpdateProgress(0.75, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 8: 标记配置已提交 ==============
            stepWatch.Restart();
            await AppendLogAsync(task.Id, "Step", "更新配置状态", "标记分区配置为已提交...", cancellationToken);

            configuration.MarkCommitted("SYSTEM");
            await configurationRepository.UpdateAsync(configuration, cancellationToken);

            stepWatch.Stop();
            await AppendLogAsync(
                task.Id,
                "Info",
                "配置已提交",
                "分区配置已标记为已提交状态。",
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            task.UpdateProgress(0.9, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 9: 任务完成 ==============
            task.UpdatePhase(PartitionExecutionPhases.Finalizing, "SYSTEM");

            var summary = JsonSerializer.Serialize(new
            {
                schema = configuration.SchemaName,
                table = configuration.TableName,
                boundaryCount = boundaryValues.Count,
                affectedPartitions = executionResult.AffectedCount,
                totalDurationMs = overallStopwatch.ElapsedMilliseconds,
                splitDurationMs = executionResult.ElapsedMilliseconds,
                requestedBy = task.RequestedBy,
                backupReference = task.BackupReference,
                completedAt = DateTime.UtcNow
            });

            task.MarkSucceeded("SYSTEM", summary);
            await taskRepository.UpdateAsync(task, cancellationToken);

            overallStopwatch.Stop();

            await AppendLogAsync(
                task.Id,
                "Info",
                "任务完成",
                $"分区执行成功完成，总耗时 {overallStopwatch.Elapsed:g}。",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);

            logger.LogInformation(
                "Partition execution task {TaskId} completed successfully in {Elapsed}",
                task.Id, overallStopwatch.Elapsed);
        }
        catch (Exception ex)
        {
            overallStopwatch.Stop();

            logger.LogError(ex, "Partition execution task {TaskId} failed.", task.Id);

            await AppendLogAsync(
                task.Id,
                "Error",
                "执行异常",
                $"发生未预期的错误：{ex.Message}",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds,
                extraJson: JsonSerializer.Serialize(new
                {
                    exceptionType = ex.GetType().Name,
                    stackTrace = ex.StackTrace
                }));

            // 根据当前状态决定是取消还是标记失败
            if (task.Status is PartitionExecutionStatus.PendingValidation or PartitionExecutionStatus.Validating or PartitionExecutionStatus.Queued)
            {
                task.Cancel("SYSTEM", ex.Message);
            }
            else
            {
                var errorSummary = JsonSerializer.Serialize(new
                {
                    error = ex.Message,
                    exceptionType = ex.GetType().Name,
                    failedAt = DateTime.UtcNow,
                    totalDurationMs = overallStopwatch.ElapsedMilliseconds,
                    schema = configuration?.SchemaName,
                    table = configuration?.TableName
                });

                task.MarkFailed("SYSTEM", ex.Message ?? "执行失败", errorSummary);
            }

            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }

    private async Task HandleValidationFailureAsync(PartitionExecutionTask task, string reason, CancellationToken cancellationToken)
    {
        await AppendLogAsync(task.Id, "Warning", "校验失败", reason, cancellationToken);
        task.Cancel("SYSTEM", reason);
        await taskRepository.UpdateAsync(task, cancellationToken);
    }

    private Task AppendLogAsync(
        Guid taskId,
        string category,
        string title,
        string message,
        CancellationToken cancellationToken,
        long? durationMs = null,
        string? extraJson = null)
    {
        var entry = PartitionExecutionLogEntry.Create(taskId, category, title, message, durationMs, extraJson);
        return logRepository.AddAsync(entry, cancellationToken);
    }

    private static string BuildPermissionContext(ArchiveDataSource dataSource, PartitionConfiguration configuration)
    {
        return $"目标服务器：{BuildServerDisplay(dataSource)}，目标数据库：{dataSource.DatabaseName}，目标对象：{configuration.SchemaName}.{configuration.TableName}";
    }

    private static string BuildServerDisplay(ArchiveDataSource dataSource)
    {
        return dataSource.ServerPort == 1433
            ? dataSource.ServerAddress
            : $"{dataSource.ServerAddress}:{dataSource.ServerPort}";
    }
}
