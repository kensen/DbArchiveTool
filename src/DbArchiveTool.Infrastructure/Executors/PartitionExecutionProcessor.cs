using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.Persistence;
using DbArchiveTool.Infrastructure.SqlExecution;
using DbArchiveTool.Shared.Partitions;
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
    private readonly IPartitionMetadataRepository metadataRepository;
    private readonly ISqlExecutor sqlExecutor;
    private readonly IDbConnectionFactory connectionFactory;
    private readonly ILogger<PartitionExecutionProcessor> logger;

    public PartitionExecutionProcessor(
        IPartitionExecutionTaskRepository taskRepository,
        IPartitionExecutionLogRepository logRepository,
        IPartitionConfigurationRepository configurationRepository,
        IDataSourceRepository dataSourceRepository,
        IPermissionInspectionRepository permissionInspectionRepository,
        SqlPartitionCommandExecutor commandExecutor,
        IPartitionMetadataRepository metadataRepository,
        ISqlExecutor sqlExecutor,
        IDbConnectionFactory connectionFactory,
        ILogger<PartitionExecutionProcessor> logger)
    {
        this.taskRepository = taskRepository;
        this.logRepository = logRepository;
        this.configurationRepository = configurationRepository;
        this.dataSourceRepository = dataSourceRepository;
        this.permissionInspectionRepository = permissionInspectionRepository;
        this.commandExecutor = commandExecutor;
        this.metadataRepository = metadataRepository;
        this.sqlExecutor = sqlExecutor;
        this.connectionFactory = connectionFactory;
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

        // 对于"添加分区边界值"操作,使用简化的执行流程
        if (task.OperationType == PartitionExecutionOperationType.AddBoundary)
        {
            await ExecuteAddBoundaryAsync(task, cancellationToken);
            return;
        }

        var overallStopwatch = Stopwatch.StartNew();
    PartitionConfiguration? configuration = null;
    ArchiveDataSource? dataSource = null;
    List<PartitionValue> pendingBoundaryValues = new();
    SqlExecutionResult? splitExecutionResult = null;

        try
        {
            // ============== 阶段 1: 任务入队与基础校验 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", $"任务由 {task.RequestedBy} 发起，操作类型：{task.OperationType}。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(PartitionExecutionPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.05, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 2: 加载配置与数据源 ==============
            var stepWatch = Stopwatch.StartNew();
            
            // 判断任务执行模式：基于草稿 vs 基于快照
            bool useDraftMode = task.OperationType == PartitionExecutionOperationType.Unknown;
            
            if (useDraftMode)
            {
                // 传统模式：从分区配置向导提交，需要加载草稿
                await AppendLogAsync(task.Id, "Step", "加载配置", "正在加载分区配置草稿...", cancellationToken);

                configuration = await configurationRepository.GetByIdAsync(task.PartitionConfigurationId, cancellationToken);
                if (configuration is null)
                {
                    await HandleValidationFailureAsync(task, "未找到分区配置草稿。", cancellationToken);
                    return;
                }
            }
            else
            {
                // 快照模式：直接操作（添加边界、拆分、合并等），从 ConfigurationSnapshot 加载
                await AppendLogAsync(task.Id, "Step", "加载配置", $"正在从任务快照加载配置（操作类型：{task.OperationType}）...", cancellationToken);
                
                if (string.IsNullOrWhiteSpace(task.ConfigurationSnapshot))
                {
                    await HandleValidationFailureAsync(task, "任务快照数据为空，无法执行。", cancellationToken);
                    return;
                }

                // 从快照构建临时配置对象（仅用于执行逻辑，不持久化）
                configuration = await BuildConfigurationFromSnapshotAsync(task, cancellationToken);
                if (configuration is null)
                {
                    await HandleValidationFailureAsync(task, "无法从任务快照解析配置信息。", cancellationToken);
                    return;
                }
            }

            dataSource = await dataSourceRepository.GetAsync(task.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                await HandleValidationFailureAsync(task, "未找到归档数据源配置。", cancellationToken);
                return;
            }

            if (configuration.Boundaries.Count == 0 && task.OperationType != PartitionExecutionOperationType.AddBoundary)
            {
                await HandleValidationFailureAsync(task, "分区配置中未提供任何边界值。", cancellationToken);
                return;
            }

            stepWatch.Stop();
            await AppendLogAsync(
                task.Id,
                "Info",
                "配置加载完成",
                $"目标表：{configuration.SchemaName}.{configuration.TableName}，分区边界数量：{configuration.Boundaries.Count}，模式：{(useDraftMode ? "草稿" : "快照")}",
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

            // ============== 阶段 6: 文件组与分区对象准备 ==============
            var storageSettings = configuration.StorageSettings;
            var defaultFilegroup = storageSettings.Mode == PartitionStorageMode.DedicatedFilegroupSingleFile
                ? storageSettings.FilegroupName
                : configuration.FilegroupStrategy.PrimaryFilegroup;

            if (string.IsNullOrWhiteSpace(defaultFilegroup))
            {
                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "文件组准备",
                    "未配置文件组名称，将使用 PRIMARY 文件组。",
                    cancellationToken);
            }
            else if (!defaultFilegroup.Equals("PRIMARY", StringComparison.OrdinalIgnoreCase))
            {
                stepWatch.Restart();
                await AppendLogAsync(
                    task.Id,
                    "Step",
                    "文件组准备",
                    $"检查文件组 {defaultFilegroup} 是否存在...",
                    cancellationToken);

                var created = await commandExecutor.CreateFilegroupIfNeededAsync(
                    task.DataSourceId,
                    dataSource.DatabaseName,
                    defaultFilegroup!,
                    cancellationToken);

                stepWatch.Stop();

                if (created)
                {
                    await AppendLogAsync(
                        task.Id,
                        "Info",
                        "文件组已创建",
                        $"成功创建文件组：{defaultFilegroup}",
                        cancellationToken,
                        durationMs: stepWatch.ElapsedMilliseconds);
                }
                else
                {
                    await AppendLogAsync(
                        task.Id,
                        "Info",
                        "文件组已存在",
                        $"文件组 {defaultFilegroup} 已存在，跳过创建。",
                        cancellationToken,
                        durationMs: stepWatch.ElapsedMilliseconds);
                }
            }
            else
            {
                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "文件组准备",
                    "使用 PRIMARY 文件组，无需额外创建。",
                    cancellationToken);
            }

            if (storageSettings.Mode == PartitionStorageMode.DedicatedFilegroupSingleFile &&
                !string.IsNullOrWhiteSpace(storageSettings.DataFileDirectory) &&
                !string.IsNullOrWhiteSpace(storageSettings.DataFileName))
            {
                var dataFilePath = Path.Combine(storageSettings.DataFileDirectory, storageSettings.DataFileName);
                stepWatch.Restart();
                await AppendLogAsync(
                    task.Id,
                    "Step",
                    "数据文件准备",
                    $"检查数据文件 {storageSettings.DataFileName} 是否存在...",
                    cancellationToken);

                var dataFileCreated = await commandExecutor.CreateDataFileIfNeededAsync(
                    task.DataSourceId,
                    dataSource.DatabaseName,
                    storageSettings,
                    cancellationToken);

                stepWatch.Stop();

                if (dataFileCreated)
                {
                    await AppendLogAsync(
                        task.Id,
                        "Info",
                        "数据文件已创建",
                        $"成功创建数据文件：{storageSettings.DataFileName}（{dataFilePath}）",
                        cancellationToken,
                        durationMs: stepWatch.ElapsedMilliseconds);
                }
                else
                {
                    await AppendLogAsync(
                        task.Id,
                        "Info",
                        "数据文件已存在",
                        $"数据文件 {storageSettings.DataFileName} 已存在，跳过创建。",
                        cancellationToken,
                        durationMs: stepWatch.ElapsedMilliseconds);
                }
            }

            await AppendLogAsync(
                task.Id,
                "Step",
                "分区对象准备",
                $"检查分区函数 {configuration.PartitionFunctionName} 与分区方案 {configuration.PartitionSchemeName} 是否存在...",
                cancellationToken);

            var functionCheckWatch = Stopwatch.StartNew();
            var partitionFunctionExists = await commandExecutor.CheckPartitionFunctionExistsAsync(
                task.DataSourceId,
                configuration.PartitionFunctionName,
                cancellationToken);
            functionCheckWatch.Stop();

            if (!partitionFunctionExists)
            {
                var seedBoundaries = configuration.Boundaries.Count > 0
                    ? configuration.Boundaries.Select(b => b.Value).ToList()
                    : null;

                var createFunctionWatch = Stopwatch.StartNew();
                await commandExecutor.CreatePartitionFunctionAsync(
                    task.DataSourceId,
                    configuration,
                    seedBoundaries,
                    cancellationToken);
                createFunctionWatch.Stop();

                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "分区函数已创建",
                    $"成功创建分区函数：{configuration.PartitionFunctionName}",
                    cancellationToken,
                    durationMs: createFunctionWatch.ElapsedMilliseconds);
            }
            else
            {
                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "分区函数已存在",
                    $"分区函数 {configuration.PartitionFunctionName} 已存在，跳过创建。",
                    cancellationToken,
                    durationMs: functionCheckWatch.ElapsedMilliseconds);
            }

            var schemeCheckWatch = Stopwatch.StartNew();
            var partitionSchemeExists = await commandExecutor.CheckPartitionSchemeExistsAsync(
                task.DataSourceId,
                configuration.PartitionSchemeName,
                cancellationToken);
            schemeCheckWatch.Stop();

            if (!partitionSchemeExists)
            {
                var createSchemeWatch = Stopwatch.StartNew();
                await commandExecutor.CreatePartitionSchemeAsync(
                    task.DataSourceId,
                    configuration,
                    cancellationToken);
                createSchemeWatch.Stop();

                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "分区方案已创建",
                    $"成功创建分区方案：{configuration.PartitionSchemeName}",
                    cancellationToken,
                    durationMs: createSchemeWatch.ElapsedMilliseconds);
            }
            else
            {
                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "分区方案已存在",
                    $"分区方案 {configuration.PartitionSchemeName} 已存在，跳过创建。",
                    cancellationToken,
                    durationMs: schemeCheckWatch.ElapsedMilliseconds);
            }

            task.UpdateProgress(0.5, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 6.5: 转换表为分区表 ==============
            stepWatch.Restart();
            await AppendLogAsync(
                task.Id,
                "Step",
                "转换表为分区表",
                $"准备将表 {configuration.SchemaName}.{configuration.TableName} 转换为分区表（保存并重建所有索引到分区方案）...",
                cancellationToken);

            PartitionIndexInspection indexInspection;
            try
            {
                indexInspection = await metadataRepository.GetIndexInspectionAsync(
                    task.DataSourceId,
                    configuration.SchemaName,
                    configuration.TableName,
                    configuration.PartitionColumn.Name,
                    cancellationToken);
            }
            catch (Exception ex)
            {
                stepWatch.Stop();
                logger.LogError(ex,
                    "索引检查失败，无法执行分区转换: Schema={Schema}, Table={Table}",
                    configuration.SchemaName,
                    configuration.TableName);

                await AppendLogAsync(
                    task.Id,
                    "Error",
                    "索引检查失败",
                    $"无法获取表 {configuration.SchemaName}.{configuration.TableName} 的索引信息：{ex.Message}",
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.MarkFailed("SYSTEM", $"索引检查失败：{ex.Message}");
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            var indexesNeedingAlignment = indexInspection.IndexesMissingPartitionColumn.ToList();

            if (!indexInspection.HasClusteredIndex)
            {
                stepWatch.Stop();
                const string messageNoCluster = "索引检查失败：目标表未检测到聚集索引，无法自动对齐分区列。";
                await AppendLogAsync(task.Id, "Error", "索引检查失败", messageNoCluster, cancellationToken, durationMs: stepWatch.ElapsedMilliseconds);
                task.MarkFailed("SYSTEM", messageNoCluster);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            if (indexInspection.HasExternalForeignKeys && indexesNeedingAlignment.Count > 0)
            {
                stepWatch.Stop();
                var fkSummary = indexInspection.ExternalForeignKeys.Count > 0
                    ? string.Join("、", indexInspection.ExternalForeignKeys)
                    : "存在外部外键引用";
                var message = $"索引检查失败：检测到外部外键引用（{fkSummary}），无法自动调整索引，请手动处理后重试。";
                await AppendLogAsync(task.Id, "Error", "索引检查失败", message, cancellationToken, durationMs: stepWatch.ElapsedMilliseconds);
                task.MarkFailed("SYSTEM", message);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            var inspectionMessage = indexesNeedingAlignment.Count > 0
                ? $"检测到需补齐分区列的索引：{string.Join("、", indexesNeedingAlignment.Select(x => x.IndexName))}，执行阶段将自动对齐。"
                : "索引结构已包含分区列，无需额外调整。";

            await AppendLogAsync(
                task.Id,
                indexesNeedingAlignment.Count > 0 ? "Warning" : "Info",
                "索引检查结果",
                inspectionMessage,
                cancellationToken);

            PartitionConversionResult conversionResult;
            try
            {
                conversionResult = await commandExecutor.ConvertToPartitionedTableAsync(
                    task.DataSourceId,
                    configuration,
                    indexInspection,
                    cancellationToken);
            }
            catch (PartitionConversionException ex)
            {
                stepWatch.Stop();

                await AppendLogAsync(
                    task.Id,
                    "Error",
                    "表转换失败",
                    ex.Message,
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.MarkFailed("SYSTEM", ex.Message);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            stepWatch.Stop();

            if (conversionResult.Converted)
            {
                var droppedSummary = conversionResult.DroppedIndexNames.Count > 0
                    ? string.Join("、", conversionResult.DroppedIndexNames)
                    : "无";
                var recreatedSummary = conversionResult.RecreatedIndexNames.Count > 0
                    ? string.Join("、", conversionResult.RecreatedIndexNames)
                    : "无";
                var alignmentSummary = conversionResult.AutoAlignedIndexes.Count > 0
                    ? string.Join("、", conversionResult.AutoAlignedIndexes.Select(a => $"{a.IndexName}({a.OriginalKeyColumns} → {a.UpdatedKeyColumns})"))
                    : "无";

                var detailMessage =
                    $"成功将表 {configuration.SchemaName}.{configuration.TableName} 转换为分区表，所有索引已在分区方案上重建。\r\n" +
                    $"已删除索引：{droppedSummary}\r\n" +
                    $"已重建索引：{recreatedSummary}\r\n" +
                    $"自动对齐索引：{alignmentSummary}";

                if (conversionResult.PartitionColumnAlteredToNotNull)
                {
                    detailMessage += "\r\n分区列已自动转换为 NOT NULL。";
                }

                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "表已转换为分区表",
                    detailMessage,
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
            await AppendLogAsync(
                task.Id,
                "Step",
                "同步分区边界",
                "正在读取数据库现有分区边界并识别需要新增的边界...",
                cancellationToken);

            var databaseBoundaries = await metadataRepository.ListBoundariesAsync(
                task.DataSourceId,
                configuration.SchemaName,
                configuration.TableName,
                cancellationToken);

            var existingBoundarySet = new HashSet<string>(
                databaseBoundaries.Select(b => b.Value.ToInvariantString()),
                StringComparer.Ordinal);

            pendingBoundaryValues = configuration.Boundaries
                .Where(b => !existingBoundarySet.Contains(b.Value.ToInvariantString()))
                .Select(b => b.Value)
                .ToList();

            stepWatch.Stop();

            await AppendLogAsync(
                task.Id,
                "Info",
                "边界同步结果",
                $"数据库当前边界数：{databaseBoundaries.Count}，草稿目标边界数：{configuration.Boundaries.Count}，待新增边界数：{pendingBoundaryValues.Count}",
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            if (pendingBoundaryValues.Count == 0)
            {
                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "分区拆分跳过",
                    "数据库分区边界已与草稿配置一致，无需执行拆分。",
                    cancellationToken);

                splitExecutionResult = SqlExecutionResult.Success(0, 0, "已与数据库边界同步，无需拆分。");
            }
            else
            {
                stepWatch.Restart();
                await AppendLogAsync(
                    task.Id,
                    "Step",
                    "执行分区拆分",
                    $"准备拆分 {pendingBoundaryValues.Count} 个新的分区边界...",
                    cancellationToken);

                var executionResult = await commandExecutor.ExecuteSplitWithTransactionAsync(
                    task.DataSourceId,
                    configuration,
                    pendingBoundaryValues,
                    indexInspection,
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

                splitExecutionResult = executionResult;

                await AppendLogAsync(
                    task.Id,
                    "Info",
                    "分区拆分完成",
                    executionResult.Message,
                    cancellationToken,
                    durationMs: executionResult.ElapsedMilliseconds,
                    extraJson: JsonSerializer.Serialize(new
                    {
                        boundaryCount = pendingBoundaryValues.Count,
                        affectedPartitions = executionResult.AffectedCount
                    }));
            }

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
                boundaryCount = pendingBoundaryValues.Count,
                affectedPartitions = splitExecutionResult?.AffectedCount ?? 0,
                totalDurationMs = overallStopwatch.ElapsedMilliseconds,
                splitDurationMs = splitExecutionResult?.ElapsedMilliseconds ?? 0,
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

    /// <summary>
    /// 从任务的 ConfigurationSnapshot 构建临时的分区配置对象（仅用于执行，不持久化）
    /// </summary>
    private async Task<PartitionConfiguration?> BuildConfigurationFromSnapshotAsync(
        PartitionExecutionTask task,
        CancellationToken cancellationToken)
    {
        try
        {
            // 根据不同的操作类型解析快照
            switch (task.OperationType)
            {
                case PartitionExecutionOperationType.AddBoundary:
                    return await BuildConfigForAddBoundaryAsync(task, cancellationToken);
                
                case PartitionExecutionOperationType.SplitBoundary:
                case PartitionExecutionOperationType.MergeBoundary:
                    // TODO: 后续实现拆分/合并边界的快照解析
                    logger.LogWarning("操作类型 {OperationType} 的快照解析尚未实现。", task.OperationType);
                    return null;
                
                default:
                    logger.LogError("不支持的操作类型：{OperationType}", task.OperationType);
                    return null;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "解析任务快照失败：{Snapshot}", task.ConfigurationSnapshot);
            return null;
        }
    }

    /// <summary>
    /// 为"添加分区边界"操作构建临时配置对象
    /// </summary>
    private async Task<PartitionConfiguration?> BuildConfigForAddBoundaryAsync(
        PartitionExecutionTask task,
        CancellationToken cancellationToken)
    {
        // 解析快照JSON
        var snapshot = JsonSerializer.Deserialize<AddBoundarySnapshot>(task.ConfigurationSnapshot!);
        if (snapshot is null)
        {
            logger.LogError("无法解析 AddBoundary 快照：{Snapshot}", task.ConfigurationSnapshot);
            return null;
        }

        // 从数据库读取实际的分区元数据（这会返回完整的 PartitionConfiguration 对象）
        var config = await metadataRepository.GetConfigurationAsync(
            task.DataSourceId,
            snapshot.SchemaName,
            snapshot.TableName,
            cancellationToken);

        if (config is null)
        {
            logger.LogError("无法从数据库读取分区元数据：{Schema}.{Table}", snapshot.SchemaName, snapshot.TableName);
            return null;
        }

        // 返回实际读取的配置（已包含所有现有边界和文件组信息）
        // 注意：新边界已经在 PartitionManagementAppService 中通过DDL脚本添加
        // 这里只需要返回配置供后续权限校验等使用
        return config;
    }

    /// <summary>
    /// 添加边界操作的快照数据结构
    /// </summary>
    private sealed class AddBoundarySnapshot
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string PartitionFunctionName { get; set; } = string.Empty;
        public string PartitionSchemeName { get; set; } = string.Empty;
        public string BoundaryValue { get; set; } = string.Empty;
        public string? FilegroupName { get; set; }
        public string SortKey { get; set; } = string.Empty;
        public string DdlScript { get; set; } = string.Empty;
    }

    /// <summary>
    /// 执行"添加分区边界值"操作的简化流程
    /// </summary>
    private async Task ExecuteAddBoundaryAsync(PartitionExecutionTask task, CancellationToken cancellationToken)
    {
        var overallStopwatch = Stopwatch.StartNew();

        try
        {
            // ============== 阶段 1: 解析快照 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", 
                $"任务由 {task.RequestedBy} 发起,操作类型:添加分区边界值。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(PartitionExecutionPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.1, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            if (string.IsNullOrWhiteSpace(task.ConfigurationSnapshot))
            {
                await HandleValidationFailureAsync(task, "任务快照数据为空,无法执行。", cancellationToken);
                return;
            }

            var snapshot = JsonSerializer.Deserialize<AddBoundarySnapshot>(task.ConfigurationSnapshot);
            if (snapshot is null)
            {
                await HandleValidationFailureAsync(task, "无法解析任务快照数据。", cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "解析快照", 
                $"目标表:{snapshot.SchemaName}.{snapshot.TableName},边界值:{snapshot.BoundaryValue},文件组:{snapshot.FilegroupName ?? "NEXT USED"}", 
                cancellationToken);

            // ============== 阶段 2: 加载数据源 ==============
            var dataSource = await dataSourceRepository.GetAsync(task.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                await HandleValidationFailureAsync(task, "未找到归档数据源配置。", cancellationToken);
                return;
            }

            task.UpdateProgress(0.2, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 3: 验证分区对象存在 ==============
            var stepWatch = Stopwatch.StartNew();
            await AppendLogAsync(task.Id, "Step", "验证分区对象", 
                $"正在检查分区函数 {snapshot.PartitionFunctionName} 与分区方案 {snapshot.PartitionSchemeName} 是否存在...", 
                cancellationToken);

            var functionExists = await commandExecutor.CheckPartitionFunctionExistsAsync(
                task.DataSourceId,
                snapshot.PartitionFunctionName,
                cancellationToken);

            if (!functionExists)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "分区函数不存在", 
                    $"分区函数 {snapshot.PartitionFunctionName} 不存在,无法添加边界值。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
                await HandleValidationFailureAsync(task, $"分区函数 {snapshot.PartitionFunctionName} 不存在。", cancellationToken);
                return;
            }

            var schemeExists = await commandExecutor.CheckPartitionSchemeExistsAsync(
                task.DataSourceId,
                snapshot.PartitionSchemeName,
                cancellationToken);

            if (!schemeExists)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "分区方案不存在", 
                    $"分区方案 {snapshot.PartitionSchemeName} 不存在,无法添加边界值。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
                await HandleValidationFailureAsync(task, $"分区方案 {snapshot.PartitionSchemeName} 不存在。", cancellationToken);
                return;
            }

            stepWatch.Stop();
            await AppendLogAsync(task.Id, "Info", "分区对象验证通过", 
                $"分区函数和分区方案均已存在。", 
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            task.UpdateProgress(0.3, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 4: 进入执行队列 ==============
            task.MarkQueued("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);
            await AppendLogAsync(task.Id, "Step", "进入队列", "校验完成,任务进入执行队列。", cancellationToken);

            // ============== 阶段 5: 开始执行DDL ==============
            task.MarkRunning("SYSTEM");
            task.UpdatePhase(PartitionExecutionPhases.Executing, "SYSTEM");
            task.UpdateProgress(0.4, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            stepWatch.Restart();
            await AppendLogAsync(task.Id, "Step", "执行DDL", 
                $"正在执行分区边界添加DDL脚本...\n```sql\n{snapshot.DdlScript}\n```", 
                cancellationToken);

            // 创建数据库连接并执行DDL脚本
            try
            {
                await using var connection = await connectionFactory.CreateSqlConnectionAsync(task.DataSourceId, cancellationToken);

                await sqlExecutor.ExecuteAsync(
                    connection,
                    snapshot.DdlScript,
                    null,
                    null,
                    timeoutSeconds: 300);

                stepWatch.Stop();

                await AppendLogAsync(task.Id, "Info", "DDL执行成功", 
                    $"成功添加分区边界值 '{snapshot.BoundaryValue}'。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.UpdateProgress(0.9, "SYSTEM");
                await taskRepository.UpdateAsync(task, cancellationToken);
            }
            catch (Exception ddlEx)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "DDL执行失败", 
                    $"执行DDL脚本时发生错误:\n{ddlEx.Message}", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                // 注意: 必须先更新进度再标记失败
                task.UpdateProgress(1.0, "SYSTEM");
                task.UpdatePhase(PartitionExecutionPhases.Finalizing, "SYSTEM");
                task.MarkFailed("SYSTEM", ddlEx.Message);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            // ============== 阶段 6: 完成 ==============
            overallStopwatch.Stop();

            // 注意: 必须先更新进度再标记成功,因为 MarkSucceeded 会改变状态
            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(PartitionExecutionPhases.Finalizing, "SYSTEM");
            task.MarkSucceeded("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var durationText = overallStopwatch.ElapsedMilliseconds < 1000
                ? $"{overallStopwatch.ElapsedMilliseconds} ms"
                : $"{overallStopwatch.Elapsed.TotalSeconds:F2} s";

            await AppendLogAsync(task.Id, "Info", "任务完成", 
                $"添加分区边界值操作成功完成,总耗时:{durationText}。", 
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            overallStopwatch.Stop();
            logger.LogError(ex, "执行添加分区边界值任务时发生异常: {TaskId}", task.Id);

            await AppendLogAsync(
                task.Id,
                "Error",
                "执行异常",
                $"任务执行过程中发生未预期的错误:\n{ex.Message}\n{ex.StackTrace}",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);

            // 注意: 必须先更新进度再标记失败
            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(PartitionExecutionPhases.Finalizing, "SYSTEM");
            task.MarkFailed("SYSTEM", ex.Message);
            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }
}

