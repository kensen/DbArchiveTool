using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Application.Abstractions;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Data.SqlClient;
using DbArchiveTool.Infrastructure.Persistence;
using DbArchiveTool.Infrastructure.SqlExecution;
using DbArchiveTool.Infrastructure.Partitions;
using DbArchiveTool.Shared.Partitions;
using DbArchiveTool.Shared.Archive;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 承担分区执行任务校验、脚本生成与实际执行的处理器。
/// </summary>
internal sealed class BackgroundTaskProcessor
{
    private readonly IBackgroundTaskRepository taskRepository;
    private readonly IBackgroundTaskLogRepository logRepository;
    private readonly IPartitionConfigurationRepository configurationRepository;
    private readonly IDataSourceRepository dataSourceRepository;
    private readonly IPermissionInspectionRepository permissionInspectionRepository;
    private readonly SqlPartitionCommandExecutor commandExecutor;
    private readonly IPartitionMetadataRepository metadataRepository;
    private readonly ISqlExecutor sqlExecutor;
    private readonly IDbConnectionFactory connectionFactory;
    private readonly BcpExecutor bcpExecutor;
    private readonly SqlBulkCopyExecutor bulkCopyExecutor;
    private readonly PartitionSwitchHelper partitionSwitchHelper;
    private readonly IPasswordEncryptionService passwordEncryptionService;
    private readonly ArchiveDbContext dbContext;
    private readonly ILogger<BackgroundTaskProcessor> logger;

    /// <summary>后台任务执行超大规模 DDL 时使用的无限超时。</summary>
    private const int LongRunningCommandTimeoutSeconds = 0;

    public BackgroundTaskProcessor(
        IBackgroundTaskRepository taskRepository,
        IBackgroundTaskLogRepository logRepository,
        IPartitionConfigurationRepository configurationRepository,
        IDataSourceRepository dataSourceRepository,
        IPermissionInspectionRepository permissionInspectionRepository,
        SqlPartitionCommandExecutor commandExecutor,
        IPartitionMetadataRepository metadataRepository,
        ISqlExecutor sqlExecutor,
        IDbConnectionFactory connectionFactory,
        BcpExecutor bcpExecutor,
        SqlBulkCopyExecutor bulkCopyExecutor,
        PartitionSwitchHelper partitionSwitchHelper,
        IPasswordEncryptionService passwordEncryptionService,
        ArchiveDbContext dbContext,
        ILogger<BackgroundTaskProcessor> logger)
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
        this.bcpExecutor = bcpExecutor;
        this.bulkCopyExecutor = bulkCopyExecutor;
        this.partitionSwitchHelper = partitionSwitchHelper;
        this.passwordEncryptionService = passwordEncryptionService;
        this.dbContext = dbContext;
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

        // ⚠️ 关键修复: 立即分离实体,避免与心跳更新的 DbContext 冲突
        // 心跳更新在独立的 scope 中也会查询并更新同一个任务,导致 EntityState 冲突
        dbContext.Entry(task).State = Microsoft.EntityFrameworkCore.EntityState.Detached;

        // 对于"添加分区边界值"和"拆分分区边界"操作,使用简化的执行流程
        if (task.OperationType == BackgroundTaskOperationType.AddBoundary)
        {
            await ExecuteAddBoundaryAsync(task, cancellationToken);
            return;
        }

        if (task.OperationType == BackgroundTaskOperationType.SplitBoundary)
        {
            await ExecuteSplitBoundaryAsync(task, cancellationToken);
            return;
        }

        if (task.OperationType == BackgroundTaskOperationType.MergeBoundary)
        {
            await ExecuteMergeBoundaryAsync(task, cancellationToken);
            return;
        }

        if (task.OperationType == BackgroundTaskOperationType.ArchiveSwitch)
        {
            await ExecuteArchiveSwitchAsync(task, cancellationToken);
            return;
        }

        if (task.OperationType == BackgroundTaskOperationType.ArchiveBcp)
        {
            await ExecuteArchiveBcpAsync(task, cancellationToken);
            return;
        }

        if (task.OperationType == BackgroundTaskOperationType.ArchiveBulkCopy)
        {
            await ExecuteArchiveBulkCopyAsync(task, cancellationToken);
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
            task.UpdatePhase(BackgroundTaskPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.05, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 2: 加载配置与数据源 ==============
            var stepWatch = Stopwatch.StartNew();
            
            // 判断任务执行模式：基于草稿 vs 基于快照
            bool useDraftMode = task.OperationType == BackgroundTaskOperationType.Unknown;
            
            if (useDraftMode)
            {
                // 传统模式：从分区配置向导提交，需要加载草稿
                await AppendLogAsync(task.Id, "Step", "加载配置", "正在加载分区配置草稿...", cancellationToken);

                if (!task.PartitionConfigurationId.HasValue)
                {
                    await HandleValidationFailureAsync(task, "分区配置ID为空。", cancellationToken);
                    return;
                }

                configuration = await configurationRepository.GetByIdAsync(task.PartitionConfigurationId.Value, cancellationToken);
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

            if (configuration.Boundaries.Count == 0 && task.OperationType != BackgroundTaskOperationType.AddBoundary)
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
            task.UpdatePhase(BackgroundTaskPhases.Executing, "SYSTEM");
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
                var droppedList = conversionResult.DroppedIndexNames.Count > 0
                    ? string.Join("\n- ", conversionResult.DroppedIndexNames.Select(name => $"`{name}`"))
                    : "无";
                var recreatedList = conversionResult.RecreatedIndexNames.Count > 0
                    ? string.Join("\n- ", conversionResult.RecreatedIndexNames.Select(name => $"`{name}`"))
                    : "无";
                var alignmentList = conversionResult.AutoAlignedIndexes.Count > 0
                    ? string.Join("\n- ", conversionResult.AutoAlignedIndexes.Select(a => $"`{a.IndexName}` (列: `{a.OriginalKeyColumns}` → `{a.UpdatedKeyColumns}`)"))
                    : "无";

                var detailMessage =
                    $"成功将表 `{configuration.SchemaName}.{configuration.TableName}` 转换为分区表，所有索引已在分区方案上重建。\n\n" +
                    $"**表总行数:** {conversionResult.TotalRows:N0} 行\n\n" +
                    $"**已删除索引:**\n{(conversionResult.DroppedIndexNames.Count > 0 ? "- " : "")}{droppedList}\n\n" +
                    $"**已重建索引:**\n{(conversionResult.RecreatedIndexNames.Count > 0 ? "- " : "")}{recreatedList}\n\n" +
                    $"**自动对齐索引:**\n{(conversionResult.AutoAlignedIndexes.Count > 0 ? "- " : "")}{alignmentList}";

                if (conversionResult.PartitionColumnAlteredToNotNull)
                {
                    detailMessage += "\n\n> 📌 **注意:** 分区列已自动转换为 NOT NULL。";
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
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");

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
            if (task.Status is BackgroundTaskStatus.PendingValidation or BackgroundTaskStatus.Validating or BackgroundTaskStatus.Queued)
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

    private async Task HandleValidationFailureAsync(BackgroundTask task, string reason, CancellationToken cancellationToken)
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
        var entry = BackgroundTaskLogEntry.Create(taskId, category, title, message, durationMs, extraJson);
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
        BackgroundTask task,
        CancellationToken cancellationToken)
    {
        try
        {
            // 根据不同的操作类型解析快照
            switch (task.OperationType)
            {
                case BackgroundTaskOperationType.AddBoundary:
                    return await BuildConfigForAddBoundaryAsync(task, cancellationToken);
                
                case BackgroundTaskOperationType.SplitBoundary:
                    return await BuildConfigForSplitBoundaryAsync(task, cancellationToken);
                
                case BackgroundTaskOperationType.MergeBoundary:
                    return await BuildConfigForMergeBoundaryAsync(task, cancellationToken);
                
                case BackgroundTaskOperationType.ArchiveSwitch:
                    return await BuildConfigForArchiveSwitchAsync(task, cancellationToken);
                
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
        BackgroundTask task,
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
    /// 拆分边界操作的快照数据结构
    /// </summary>
    private sealed class SplitBoundarySnapshot
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string PartitionFunctionName { get; set; } = string.Empty;
        public string PartitionSchemeName { get; set; } = string.Empty;
        public string[] Boundaries { get; set; } = Array.Empty<string>();
        public string DdlScript { get; set; } = string.Empty;
        public bool BackupConfirmed { get; set; }
        public string? FilegroupName { get; set; }  // 用户指定的文件组
    }

    private sealed class MergeBoundarySnapshot
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string PartitionFunctionName { get; set; } = string.Empty;
        public string PartitionSchemeName { get; set; } = string.Empty;
        public string BoundaryKey { get; set; } = string.Empty;
        public string DdlScript { get; set; } = string.Empty;
        public bool BackupConfirmed { get; set; }
    }

    /// <summary>
    /// 分区切换(归档)操作的快照数据结构
    /// </summary>
    private sealed class ArchiveSwitchSnapshot
    {
        public Guid ConfigurationId { get; set; }
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string SourcePartitionKey { get; set; } = string.Empty;
        public string TargetSchema { get; set; } = string.Empty;
        public string TargetTable { get; set; } = string.Empty;
        public string TargetDatabase { get; set; } = string.Empty;
        public bool CreateStagingTable { get; set; }
        public string DdlScript { get; set; } = string.Empty;
    }

    /// <summary>
    /// 为"拆分分区边界"操作构建临时配置对象
    /// </summary>
    private async Task<PartitionConfiguration?> BuildConfigForSplitBoundaryAsync(
        BackgroundTask task,
        CancellationToken cancellationToken)
    {
        // 解析快照JSON
        var snapshot = JsonSerializer.Deserialize<SplitBoundarySnapshot>(task.ConfigurationSnapshot!);
        if (snapshot is null)
        {
            logger.LogError("无法解析 SplitBoundary 快照：{Snapshot}", task.ConfigurationSnapshot);
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
        // 注意：拆分操作与添加边界类似，都是直接操作模式，不需要草稿配置
        return config;
    }

    /// <summary>
    /// 为"合并分区边界"操作构建临时配置对象
    /// </summary>
    private async Task<PartitionConfiguration?> BuildConfigForMergeBoundaryAsync(
        BackgroundTask task,
        CancellationToken cancellationToken)
    {
        // 解析快照JSON
        var snapshot = JsonSerializer.Deserialize<MergeBoundarySnapshot>(task.ConfigurationSnapshot!);
        if (snapshot is null)
        {
            logger.LogError("无法解析 MergeBoundary 快照：{Snapshot}", task.ConfigurationSnapshot);
            return null;
        }

        // 从数据库读取实际的分区元数据
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

        return config;
    }

    /// <summary>
    /// 为"分区切换(归档)"操作构建临时配置对象
    /// </summary>
    private async Task<PartitionConfiguration?> BuildConfigForArchiveSwitchAsync(
        BackgroundTask task,
        CancellationToken cancellationToken)
    {
        // 解析快照JSON
        var snapshot = JsonSerializer.Deserialize<ArchiveSwitchSnapshot>(task.ConfigurationSnapshot!);
        if (snapshot is null)
        {
            logger.LogError("无法解析 ArchiveSwitch 快照：{Snapshot}", task.ConfigurationSnapshot);
            return null;
        }

        // 从数据库读取源表的分区元数据
        var config = await metadataRepository.GetConfigurationAsync(
            task.DataSourceId,
            snapshot.SchemaName,
            snapshot.TableName,
            cancellationToken);

        if (config is null)
        {
            logger.LogError("无法从数据库读取源表分区元数据：{Schema}.{Table}", snapshot.SchemaName, snapshot.TableName);
            return null;
        }

        return config;
    }

    /// <summary>
    /// 执行"添加分区边界值"操作的简化流程
    /// </summary>
    private async Task ExecuteAddBoundaryAsync(BackgroundTask task, CancellationToken cancellationToken)
    {
        var overallStopwatch = Stopwatch.StartNew();

        try
        {
            // ============== 阶段 1: 解析快照 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", 
                $"任务由 {task.RequestedBy} 发起,操作类型:添加分区边界值。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Validation, "SYSTEM");
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
            task.UpdatePhase(BackgroundTaskPhases.Executing, "SYSTEM");
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
                    timeoutSeconds: LongRunningCommandTimeoutSeconds);

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
                task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
                task.MarkFailed("SYSTEM", ddlEx.Message);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            // ============== 阶段 6: 完成 ==============
            overallStopwatch.Stop();

            // 注意: 必须先更新进度再标记成功,因为 MarkSucceeded 会改变状态
            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
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
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkFailed("SYSTEM", ex.Message);
            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }

    /// <summary>
    /// 执行"拆分分区边界"操作的简化流程(参考添加边界的流程)
    /// </summary>
    private async Task ExecuteSplitBoundaryAsync(BackgroundTask task, CancellationToken cancellationToken)
    {
        var overallStopwatch = Stopwatch.StartNew();

        try
        {
            // ============== 阶段 1: 解析快照 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", 
                $"任务由 {task.RequestedBy} 发起,操作类型:拆分分区边界。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.1, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            if (string.IsNullOrWhiteSpace(task.ConfigurationSnapshot))
            {
                await HandleValidationFailureAsync(task, "任务快照数据为空,无法执行。", cancellationToken);
                return;
            }

            var snapshot = JsonSerializer.Deserialize<SplitBoundarySnapshot>(task.ConfigurationSnapshot);
            if (snapshot is null)
            {
                await HandleValidationFailureAsync(task, "无法解析任务快照数据。", cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "解析快照", 
                $"目标表:{snapshot.SchemaName}.{snapshot.TableName},边界值数量:{snapshot.Boundaries.Length},文件组:{snapshot.FilegroupName ?? "默认"}", 
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
                    $"分区函数 {snapshot.PartitionFunctionName} 不存在,无法拆分边界值。", 
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
                    $"分区方案 {snapshot.PartitionSchemeName} 不存在,无法拆分边界值。", 
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
            task.UpdatePhase(BackgroundTaskPhases.Executing, "SYSTEM");
            task.UpdateProgress(0.4, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            stepWatch.Restart();
            await AppendLogAsync(task.Id, "Step", "执行DDL", 
                $"正在执行分区拆分DDL脚本,将拆分 {snapshot.Boundaries.Length} 个边界值...\n```sql\n{snapshot.DdlScript}\n```", 
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
                    timeoutSeconds: LongRunningCommandTimeoutSeconds);

                stepWatch.Stop();

                var boundariesDisplay = snapshot.Boundaries.Length == 1 
                    ? $"'{snapshot.Boundaries[0]}'" 
                    : $"{snapshot.Boundaries.Length} 个边界值";

                await AppendLogAsync(task.Id, "Info", "DDL执行成功", 
                    $"成功拆分分区边界值: {boundariesDisplay}。", 
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

                task.UpdateProgress(1.0, "SYSTEM");
                task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
                task.MarkFailed("SYSTEM", ddlEx.Message);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            // ============== 阶段 6: 完成 ==============
            overallStopwatch.Stop();

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkSucceeded("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var durationText = overallStopwatch.ElapsedMilliseconds < 1000
                ? $"{overallStopwatch.ElapsedMilliseconds} ms"
                : $"{overallStopwatch.Elapsed.TotalSeconds:F2} s";

            await AppendLogAsync(task.Id, "Info", "任务完成", 
                $"拆分分区边界操作成功完成,处理了 {snapshot.Boundaries.Length} 个边界值,总耗时:{durationText}。", 
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            overallStopwatch.Stop();
            logger.LogError(ex, "执行拆分分区边界任务时发生异常: {TaskId}", task.Id);

            await AppendLogAsync(
                task.Id,
                "Error",
                "执行异常",
                $"任务执行过程中发生未预期的错误:\n{ex.Message}\n{ex.StackTrace}",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkFailed("SYSTEM", ex.Message);
            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }

    /// <summary>
    /// 执行"合并分区边界"操作的简化流程
    /// </summary>
    private async Task ExecuteMergeBoundaryAsync(BackgroundTask task, CancellationToken cancellationToken)
    {
        var overallStopwatch = Stopwatch.StartNew();

        try
        {
            // ============== 阶段 1: 解析快照 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", 
                $"任务由 {task.RequestedBy} 发起,操作类型:合并分区边界。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.1, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            if (string.IsNullOrWhiteSpace(task.ConfigurationSnapshot))
            {
                await HandleValidationFailureAsync(task, "任务快照数据为空,无法执行。", cancellationToken);
                return;
            }

            var snapshot = JsonSerializer.Deserialize<MergeBoundarySnapshot>(task.ConfigurationSnapshot);
            if (snapshot is null)
            {
                await HandleValidationFailureAsync(task, "无法解析任务快照数据。", cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "解析快照", 
                $"目标表:{snapshot.SchemaName}.{snapshot.TableName},删除边界:{snapshot.BoundaryKey}", 
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
                    $"分区函数 {snapshot.PartitionFunctionName} 不存在,无法合并边界值。", 
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
                    $"分区方案 {snapshot.PartitionSchemeName} 不存在,无法合并边界值。", 
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
            task.UpdatePhase(BackgroundTaskPhases.Executing, "SYSTEM");
            task.UpdateProgress(0.4, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            stepWatch.Restart();
            await AppendLogAsync(task.Id, "Step", "执行DDL", 
                $"正在执行分区合并DDL脚本,将删除边界值: '{snapshot.BoundaryKey}'...\n```sql\n{snapshot.DdlScript}\n```", 
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
                    timeoutSeconds: LongRunningCommandTimeoutSeconds);

                stepWatch.Stop();

                await AppendLogAsync(task.Id, "Info", "DDL执行成功", 
                    $"成功合并分区边界值: '{snapshot.BoundaryKey}'。", 
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

                task.UpdateProgress(1.0, "SYSTEM");
                task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
                task.MarkFailed("SYSTEM", ddlEx.Message);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            // ============== 阶段 6: 完成 ==============
            overallStopwatch.Stop();

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkSucceeded("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var durationText = overallStopwatch.ElapsedMilliseconds < 1000
                ? $"{overallStopwatch.ElapsedMilliseconds} ms"
                : $"{overallStopwatch.Elapsed.TotalSeconds:F2} s";

            await AppendLogAsync(task.Id, "Info", "任务完成", 
                $"合并分区边界操作成功完成,已删除边界值: '{snapshot.BoundaryKey}',总耗时:{durationText}。", 
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            overallStopwatch.Stop();
            logger.LogError(ex, "执行合并分区边界任务时发生异常: {TaskId}", task.Id);

            await AppendLogAsync(
                task.Id,
                "Error",
                "执行异常",
                $"任务执行过程中发生未预期的错误:\n{ex.Message}\n{ex.StackTrace}",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkFailed("SYSTEM", ex.Message);
            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }

    /// <summary>
    /// 执行"分区切换(归档)"操作的简化流程
    /// </summary>
    private async Task ExecuteArchiveSwitchAsync(BackgroundTask task, CancellationToken cancellationToken)
    {
        var overallStopwatch = Stopwatch.StartNew();

        try
        {
            // ============== 阶段 1: 解析快照 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", 
                $"任务由 {task.RequestedBy} 发起,操作类型:分区切换(归档)。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.1, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            if (string.IsNullOrWhiteSpace(task.ConfigurationSnapshot))
            {
                await HandleValidationFailureAsync(task, "任务快照数据为空,无法执行。", cancellationToken);
                return;
            }

            var snapshot = JsonSerializer.Deserialize<ArchiveSwitchSnapshot>(task.ConfigurationSnapshot);
            if (snapshot is null)
            {
                await HandleValidationFailureAsync(task, "无法解析任务快照数据。", cancellationToken);
                return;
            }

            var targetDisplay = string.IsNullOrWhiteSpace(snapshot.TargetDatabase)
                ? $"{snapshot.TargetSchema}.{snapshot.TargetTable}"
                : $"{snapshot.TargetDatabase}.{snapshot.TargetSchema}.{snapshot.TargetTable}";

            await AppendLogAsync(task.Id, "Info", "解析快照", 
                $"源表:{snapshot.SchemaName}.{snapshot.TableName},分区:{snapshot.SourcePartitionKey},目标:{targetDisplay}", 
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

            // ============== 阶段 3: 验证分区配置 ==============
            var stepWatch = Stopwatch.StartNew();
            await AppendLogAsync(task.Id, "Step", "验证分区配置", 
                "正在从数据库加载分区配置...", 
                cancellationToken);

            // 从数据库重新加载分区配置
            var config = await metadataRepository.GetConfigurationAsync(
                task.DataSourceId,
                snapshot.SchemaName,
                snapshot.TableName,
                cancellationToken);

            if (config is null)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "配置不存在", 
                    $"未找到表 {snapshot.SchemaName}.{snapshot.TableName} 的分区配置。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
                await HandleValidationFailureAsync(task, "未找到分区配置。", cancellationToken);
                return;
            }

            stepWatch.Stop();
            await AppendLogAsync(task.Id, "Info", "配置验证通过", 
                $"已加载分区配置,分区边界数量: {config.Boundaries.Count}。", 
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            task.UpdateProgress(0.3, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 4: 分区边界检查 ==============
            stepWatch.Restart();
            await AppendLogAsync(task.Id, "Step", "分区边界检查", 
                $"正在检查源表的分区边界是否符合要求...", 
                cancellationToken);

            if (config.Boundaries.Count == 0)
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Error", "分区边界为空", 
                    $"配置中未找到任何边界值,无法切换分区。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
                await HandleValidationFailureAsync(task, "分区边界为空,无法执行切换。", cancellationToken);
                return;
            }

            stepWatch.Stop();
            await AppendLogAsync(task.Id, "Info", "分区边界检查通过", 
                $"当前分区边界数量: {config.Boundaries.Count}。", 
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            task.UpdateProgress(0.4, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 5: 检测并修复源表索引对齐 ==============
            stepWatch.Restart();
            await AppendLogAsync(task.Id, "Step", "检测源表索引", 
                $"正在检测源表 {snapshot.SchemaName}.{snapshot.TableName} 的索引是否对齐到分区方案...", 
                cancellationToken);

            await using var sourceConnection = await connectionFactory.CreateSqlConnectionAsync(task.DataSourceId, cancellationToken);
            
            // 查询未对齐的索引
            var unalignedIndexesSql = @"
SELECT 
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.index_id AS IndexId
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN sys.data_spaces ds_index ON i.data_space_id = ds_index.data_space_id
LEFT JOIN sys.data_spaces ds_table ON t.lob_data_space_id = ds_table.data_space_id
WHERE s.name = @SchemaName
  AND t.name = @TableName
  AND i.type IN (1, 2)  -- 聚集和非聚集
  AND i.name IS NOT NULL
  AND ds_index.type <> 'PS'  -- 索引不在分区方案上
  AND EXISTS (  -- 表本身是分区表
    SELECT 1 FROM sys.partition_schemes ps
    WHERE ps.data_space_id = COALESCE(
        (SELECT TOP 1 data_space_id FROM sys.indexes WHERE object_id = t.object_id AND type IN (0,1)),
        t.filestream_data_space_id
    )
  );";

            var unalignedIndexes = await sqlExecutor.QueryAsync<UnalignedIndexInfo>(
                sourceConnection,
                unalignedIndexesSql,
                new { snapshot.SchemaName, snapshot.TableName });

            if (unalignedIndexes.Any())
            {
                stepWatch.Stop();
                var indexNames = string.Join(", ", unalignedIndexes.Select(idx => idx.IndexName));
                await AppendLogAsync(task.Id, "Warning", "发现未对齐索引", 
                    $"源表存在 {unalignedIndexes.Count()} 个未对齐到分区方案的索引: {indexNames}。\n这些索引会阻止 SWITCH 操作,系统将自动修复。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                // 自动修复:重建索引并对齐到分区方案
                await AppendLogAsync(task.Id, "Step", "修复索引对齐", 
                    "正在重建源表索引以对齐到分区方案...", 
                    cancellationToken);

                var alignedCount = 0;
                foreach (var index in unalignedIndexes)
                {
                    try
                    {
                        // 获取索引详细信息并重建
                        var rebuildSql = await GenerateAlignIndexScript(
                            sourceConnection,
                            snapshot.SchemaName,
                            snapshot.TableName,
                            index.IndexName,
                            config.PartitionSchemeName,
                            config.PartitionColumn.Name);

                        if (!string.IsNullOrWhiteSpace(rebuildSql))
                        {
                            await sqlExecutor.ExecuteAsync(sourceConnection, rebuildSql, timeoutSeconds: LongRunningCommandTimeoutSeconds);
                            alignedCount++;
                            logger.LogInformation("已对齐索引 {IndexName} 到分区方案", index.IndexName);
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "对齐索引 {IndexName} 失败,但将继续尝试 SWITCH", index.IndexName);
                        await AppendLogAsync(task.Id, "Warning", "索引对齐警告", 
                            $"索引 {index.IndexName} 对齐失败: {ex.Message}", 
                            cancellationToken);
                    }
                }

                await AppendLogAsync(task.Id, "Info", "索引对齐完成", 
                    $"已成功对齐 {alignedCount}/{unalignedIndexes.Count()} 个索引到分区方案。", 
                    cancellationToken);
            }
            else
            {
                stepWatch.Stop();
                await AppendLogAsync(task.Id, "Info", "索引检测通过", 
                    "源表所有索引已正确对齐到分区方案。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
            }

            task.UpdateProgress(0.6, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 6: 进入执行队列 ==============
            task.MarkQueued("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);
            await AppendLogAsync(task.Id, "Step", "进入队列", "校验完成,任务进入执行队列。", cancellationToken);

            // ============== 阶段 6: 开始执行分区切换 ==============
            task.MarkRunning("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Executing, "SYSTEM");
            task.UpdateProgress(0.8, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            stepWatch.Restart();
            await AppendLogAsync(task.Id, "Step", "执行分区切换", 
                $"正在执行 SWITCH 操作,将分区 {snapshot.SourcePartitionKey} 切换到 {targetDisplay}...\n```sql\n{snapshot.DdlScript}\n```", 
                cancellationToken);

            // 创建数据库连接并执行分区切换脚本
            try
            {
                await using var connection = await connectionFactory.CreateSqlConnectionAsync(task.DataSourceId, cancellationToken);

                await sqlExecutor.ExecuteAsync(
                    connection,
                    snapshot.DdlScript,
                    null,
                    null,
                    timeoutSeconds: LongRunningCommandTimeoutSeconds);

                stepWatch.Stop();

                await AppendLogAsync(task.Id, "Info", "分区切换成功", 
                    $"成功将分区 {snapshot.SourcePartitionKey} 切换到目标表 {targetDisplay}。", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.UpdateProgress(0.95, "SYSTEM");
                await taskRepository.UpdateAsync(task, cancellationToken);
            }
            catch (Exception ddlEx)
            {
                stepWatch.Stop();
                
                var errorMessage = ddlEx.Message;
                var diagnosticInfo = new StringBuilder();
                diagnosticInfo.AppendLine($"执行SWITCH脚本时发生错误:\n{errorMessage}");
                
                // 如果是索引未对齐错误,提供修复建议
                if (errorMessage.Contains("未分区") || errorMessage.Contains("not partitioned", StringComparison.OrdinalIgnoreCase))
                {
                    diagnosticInfo.AppendLine();
                    diagnosticInfo.AppendLine("【问题诊断】");
                    diagnosticInfo.AppendLine("源表上存在未对齐到分区方案的索引,这会阻止 SWITCH 操作。");
                    diagnosticInfo.AppendLine();
                    diagnosticInfo.AppendLine("【修复建议】");
                    diagnosticInfo.AppendLine("请在 SSMS 中执行以下步骤修复源表索引:");
                    diagnosticInfo.AppendLine();
                    diagnosticInfo.AppendLine("1. 查询未对齐的索引:");
                    diagnosticInfo.AppendLine($@"
SELECT 
    i.name AS IndexName,
    i.type_desc AS IndexType,
    CASE WHEN ds.type = 'PS' THEN 'Already Aligned' ELSE 'NOT Aligned' END AS AlignmentStatus
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
LEFT JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = '{snapshot.SchemaName}'
  AND t.name = '{snapshot.TableName}'
  AND i.type IN (1, 2)
  AND i.name IS NOT NULL
  AND ds.type <> 'PS';");
                    diagnosticInfo.AppendLine();
                    diagnosticInfo.AppendLine("2. 对于每个未对齐的索引,执行重建(示例):");
                    diagnosticInfo.AppendLine($@"
-- 假设分区方案为 PS_YourScheme, 分区列为 YourPartitionColumn
-- 重建索引并对齐到分区方案:
DROP INDEX [IndexName] ON [{snapshot.SchemaName}].[{snapshot.TableName}];
GO

CREATE NONCLUSTERED INDEX [IndexName] 
ON [{snapshot.SchemaName}].[{snapshot.TableName}] ([YourColumns])
ON [YourPartitionScheme]([YourPartitionColumn]);
GO");
                    diagnosticInfo.AppendLine();
                    diagnosticInfo.AppendLine("3. 完成修复后,重新提交分区切换任务。");
                }
                
                await AppendLogAsync(task.Id, "Error", "分区切换失败", 
                    diagnosticInfo.ToString(), 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.UpdateProgress(1.0, "SYSTEM");
                task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
                task.MarkFailed("SYSTEM", errorMessage);
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            // ============== 阶段 9: 完成 ==============
            overallStopwatch.Stop();

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkSucceeded("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var durationText = overallStopwatch.ElapsedMilliseconds < 1000
                ? $"{overallStopwatch.ElapsedMilliseconds} ms"
                : $"{overallStopwatch.Elapsed.TotalSeconds:F2} s";

            await AppendLogAsync(task.Id, "Info", "任务完成", 
                $"分区切换操作成功完成,已将分区 {snapshot.SourcePartitionKey} 切换到 {targetDisplay},总耗时:{durationText}。", 
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            overallStopwatch.Stop();
            logger.LogError(ex, "执行分区切换任务时发生异常: {TaskId}", task.Id);

            await AppendLogAsync(
                task.Id,
                "Error",
                "执行异常",
                $"任务执行过程中发生未预期的错误:\n{ex.Message}\n{ex.StackTrace}",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkFailed("SYSTEM", ex.Message);
            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }

    /// <summary>
    /// 生成对齐索引到分区方案的SQL脚本
    /// </summary>
    private async Task<string> GenerateAlignIndexScript(
        SqlConnection connection,
        string schemaName,
        string tableName,
        string indexName,
        string partitionSchemeName,
        string partitionColumnName)
    {
        // 查询索引详细信息
        const string sql = @"
SELECT 
    i.index_id,
    i.type AS IndexType,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    STUFF((
        SELECT ', [' + c.name + ']' + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), 1, 2, '') AS KeyColumns,
    STUFF((
        SELECT ', [' + c.name + ']'
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), 1, 2, '') AS IncludedColumns,
    i.filter_definition AS FilterDefinition
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName
  AND t.name = @TableName
  AND i.name = @IndexName;";

        var indexInfo = (await sqlExecutor.QueryAsync<IndexDetailsForAlign>(
            connection,
            sql,
            new { SchemaName = schemaName, TableName = tableName, IndexName = indexName }))
            .FirstOrDefault();

        if (indexInfo == null)
        {
            return string.Empty;
        }

        var script = new StringBuilder();

        // 删除旧索引
        if (indexInfo.IsPrimaryKey)
        {
            script.AppendLine($"ALTER TABLE [{schemaName}].[{tableName}] DROP CONSTRAINT [{indexName}];");
        }
        else
        {
            script.AppendLine($"DROP INDEX [{indexName}] ON [{schemaName}].[{tableName}];");
        }

        script.AppendLine("GO");
        script.AppendLine();

        // 重建索引并对齐到分区方案
        if (indexInfo.IsPrimaryKey)
        {
            var clustered = indexInfo.IndexType == 1 ? "CLUSTERED" : "NONCLUSTERED";
            script.AppendLine($"ALTER TABLE [{schemaName}].[{tableName}] ADD CONSTRAINT [{indexName}]");
            script.AppendLine($"    PRIMARY KEY {clustered} ({indexInfo.KeyColumns})");
            script.AppendLine($"    ON [{partitionSchemeName}]([{partitionColumnName}]);");
        }
        else
        {
            var clustered = indexInfo.IndexType == 1 ? "CLUSTERED" : "NONCLUSTERED";
            var unique = indexInfo.IsUnique ? "UNIQUE " : "";
            script.AppendLine($"CREATE {unique}{clustered} INDEX [{indexName}]");
            script.AppendLine($"    ON [{schemaName}].[{tableName}] ({indexInfo.KeyColumns})");

            if (!string.IsNullOrWhiteSpace(indexInfo.IncludedColumns))
            {
                script.AppendLine($"    INCLUDE ({indexInfo.IncludedColumns})");
            }

            if (!string.IsNullOrWhiteSpace(indexInfo.FilterDefinition))
            {
                script.AppendLine($"    WHERE {indexInfo.FilterDefinition}");
            }

            script.AppendLine($"    ON [{partitionSchemeName}]([{partitionColumnName}]);");
        }

        return script.ToString();
    }

    private sealed class UnalignedIndexInfo
    {
        public string IndexName { get; set; } = string.Empty;
        public string IndexType { get; set; } = string.Empty;
        public int IndexId { get; set; }
    }

    private sealed class IndexDetailsForAlign
    {
        public int IndexType { get; set; }
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }
        public string KeyColumns { get; set; } = string.Empty;
        public string? IncludedColumns { get; set; }
        public string? FilterDefinition { get; set; }
    }

    /// <summary>
    /// BCP 归档快照结构
    /// </summary>
    private sealed class ArchiveBcpSnapshot
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string SourcePartitionKey { get; set; } = string.Empty;
        public string TargetTable { get; set; } = string.Empty;
        public string TargetDatabase { get; set; } = string.Empty;
        public string TempDirectory { get; set; } = string.Empty;
        public int BatchSize { get; set; }
        public bool UseNativeFormat { get; set; }
        public int MaxErrors { get; set; }
        public int TimeoutSeconds { get; set; }
    }

    /// <summary>
    /// BulkCopy 归档快照结构
    /// </summary>
    private sealed class ArchiveBulkCopySnapshot
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string SourcePartitionKey { get; set; } = string.Empty;
        public string TargetTable { get; set; } = string.Empty;
        public string TargetDatabase { get; set; } = string.Empty;
        public int BatchSize { get; set; }
        public int NotifyAfterRows { get; set; }
        public int TimeoutSeconds { get; set; }
        public bool EnableStreaming { get; set; }
    }

    /// <summary>
    /// 执行 BCP 归档任务
    /// </summary>
    private async Task ExecuteArchiveBcpAsync(BackgroundTask task, CancellationToken cancellationToken)
    {
        var overallStopwatch = Stopwatch.StartNew();

        try
        {
            // ============== 阶段 1: 解析快照 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", 
                $"任务由 {task.RequestedBy} 发起,操作类型: BCP 归档。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.1, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            if (string.IsNullOrWhiteSpace(task.ConfigurationSnapshot))
            {
                await HandleValidationFailureAsync(task, "任务快照数据为空,无法执行。", cancellationToken);
                return;
            }

            var snapshot = JsonSerializer.Deserialize<ArchiveBcpSnapshot>(task.ConfigurationSnapshot);
            if (snapshot is null)
            {
                await HandleValidationFailureAsync(task, "无法解析 BCP 归档快照数据。", cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "解析快照", 
                $"源表: {snapshot.SchemaName}.{snapshot.TableName}, 分区: {snapshot.SourcePartitionKey}, " +
                $"目标: {snapshot.TargetDatabase}.{snapshot.TargetTable}", 
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

            // ============== 阶段 3: 构建连接字符串 ==============
            var sourceConnectionString = BuildConnectionString(dataSource);
            var targetConnectionString = BuildTargetConnectionString(dataSource, snapshot.TargetDatabase);

            await AppendLogAsync(task.Id, "Info", "BCP执行连接信息", 
                $"UseSourceAsTarget={dataSource.UseSourceAsTarget}, TargetDatabase={snapshot.TargetDatabase}", 
                cancellationToken);

            await AppendLogAsync(task.Id, "Step", "准备归档", 
                $"准备执行 BCP 归档,目标数据库: {snapshot.TargetDatabase},批次大小: {snapshot.BatchSize}", 
                cancellationToken);

            task.UpdateProgress(0.25, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 4: 进入执行队列 ==============
            task.MarkQueued("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);
            await AppendLogAsync(task.Id, "Step", "进入队列", "准备工作完成,任务进入执行队列。", cancellationToken);

            // ============== 阶段 5: 开始执行 BCP 归档 ==============
            task.MarkRunning("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Executing, "SYSTEM");
            task.UpdateProgress(0.3, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var stepWatch = Stopwatch.StartNew();
            await AppendLogAsync(task.Id, "Step", "执行 BCP 归档", 
                $"正在通过 BCP 工具导出并导入数据...", 
                cancellationToken);

            // ============== 分区优化方案: 检测分区表并 SWITCH ==============
            string sourceQuery;
            string? tempTableName = null;
            long expectedRowCount = 0;
            bool usedPartitionSwitch = false;

            // 1. 检查是否为分区表
            var isPartitionedTable = await partitionSwitchHelper.IsPartitionedTableAsync(
                sourceConnectionString,
                snapshot.SchemaName,
                snapshot.TableName,
                cancellationToken);

            if (isPartitionedTable && !string.IsNullOrWhiteSpace(snapshot.SourcePartitionKey))
            {
                await AppendLogAsync(task.Id, "Info", "分区优化", 
                    $"检测到分区表，将使用优化方案：SWITCH 分区到临时表 → 归档临时表 → 删除临时表", 
                    cancellationToken);

                // 2. 获取分区信息
                var partitionInfo = await partitionSwitchHelper.GetPartitionInfoAsync(
                    sourceConnectionString,
                    snapshot.SchemaName,
                    snapshot.TableName,
                    snapshot.SourcePartitionKey,
                    cancellationToken);

                if (partitionInfo is null)
                {
                    await AppendLogAsync(task.Id, "Warning", "分区未找到", 
                        $"未找到分区: {snapshot.SourcePartitionKey}，将尝试使用 $PARTITION 函数查询", 
                        cancellationToken);
                    
                    // 尝试获取分区函数信息，使用 $PARTITION 函数查询
                    var partitionFuncInfo = await partitionSwitchHelper.GetPartitionFunctionInfoAsync(
                        sourceConnectionString,
                        snapshot.SchemaName,
                        snapshot.TableName,
                        cancellationToken);
                    
                    if (partitionFuncInfo != null && int.TryParse(snapshot.SourcePartitionKey, out var partNum))
                    {
                        // 使用 $PARTITION 函数精确查询分区数据
                        sourceQuery = $"SELECT * FROM [{snapshot.SchemaName}].[{snapshot.TableName}] " +
                                     $"WHERE $PARTITION.[{partitionFuncInfo.PartitionFunctionName}]([{partitionFuncInfo.PartitionColumnName}]) = {partNum}";
                        
                        await AppendLogAsync(task.Id, "Info", "使用 $PARTITION 函数", 
                            $"使用分区函数查询: {partitionFuncInfo.PartitionFunctionName}({partitionFuncInfo.PartitionColumnName}) = {partNum}", 
                            cancellationToken);
                    }
                    else
                    {
                        // 降级为全表查询
                        sourceQuery = $"SELECT * FROM [{snapshot.SchemaName}].[{snapshot.TableName}]";
                    }
                }
                else
                {
                    await AppendLogAsync(task.Id, "Info", "分区信息", 
                        $"分区号: {partitionInfo.PartitionNumber}, 边界值: {partitionInfo.BoundaryValue}, " +
                        $"行数: {partitionInfo.RowCount:N0}, 文件组: {partitionInfo.FileGroupName}", 
                        cancellationToken);

                    expectedRowCount = partitionInfo.RowCount;

                    // 2.5 检测是否存在未完成归档的临时表（恢复机制）
                    try
                    {
                        var existingTempTables = await GetExistingTempTablesAsync(
                            sourceConnectionString,
                            snapshot.SchemaName,
                            snapshot.TableName,
                            cancellationToken);
                        
                        if (existingTempTables.Count > 0)
                        {
                            // ⚠️ 关键修复: 发现旧临时表，尝试恢复而不是删除
                            var recoveryTempTable = existingTempTables[0]; // 使用最新的临时表
                            
                            await AppendLogAsync(task.Id, "Warning", "发现未完成归档", 
                                $"检测到 {existingTempTables.Count} 个历史临时表。尝试恢复归档: [{snapshot.SchemaName}].[{recoveryTempTable}]", 
                                cancellationToken);
                            
                            // 检查临时表的行数
                            var tempTableRowCount = await GetTableRowCountAsync(
                                sourceConnectionString,
                                snapshot.SchemaName,
                                recoveryTempTable,
                                cancellationToken);
                            
                            await AppendLogAsync(task.Id, "Info", "临时表状态", 
                                $"临时表 [{recoveryTempTable}] 包含 {tempTableRowCount:N0} 行数据，将继续归档这些数据", 
                                cancellationToken);
                            
                            // 使用已有的临时表，跳过 SWITCH 步骤
                            tempTableName = recoveryTempTable;
                            sourceQuery = $"SELECT * FROM [{snapshot.SchemaName}].[{tempTableName}]";
                            usedPartitionSwitch = true;
                            expectedRowCount = tempTableRowCount;
                            
                            // 跳到 BCP 执行阶段
                            goto ExecuteBcp;
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "检查旧临时表时出错，继续正常流程");
                    }

                    // 3. 创建临时表
                    tempTableName = await partitionSwitchHelper.CreateTempTableForSwitchAsync(
                        sourceConnectionString,
                        snapshot.SchemaName,
                        snapshot.TableName,
                        partitionInfo,
                        cancellationToken);

                    await AppendLogAsync(task.Id, "Step", "创建临时表", 
                        $"临时表创建成功: [{snapshot.SchemaName}].[{tempTableName}]", 
                        cancellationToken);

                    // 4. SWITCH 分区到临时表
                    await partitionSwitchHelper.SwitchPartitionAsync(
                        sourceConnectionString,
                        snapshot.SchemaName,
                        snapshot.TableName,
                        partitionInfo.PartitionNumber,
                        snapshot.SchemaName,
                        tempTableName,
                        cancellationToken);

                    await AppendLogAsync(task.Id, "Step", "分区切换完成", 
                        $"分区 {partitionInfo.PartitionNumber} 已 SWITCH 到临时表，生产表影响时间 < 1秒", 
                        cancellationToken);

                    sourceQuery = $"SELECT * FROM [{snapshot.SchemaName}].[{tempTableName}]";
                    usedPartitionSwitch = true;
                }
            }
            else
            {
                // 非分区表或未指定分区键，直接对源表执行 BCP
                sourceQuery = $"SELECT * FROM [{snapshot.SchemaName}].[{snapshot.TableName}]";
                
                if (!string.IsNullOrWhiteSpace(snapshot.SourcePartitionKey))
                {
                    await AppendLogAsync(task.Id, "Warning", "分区键筛选", 
                        $"表不是分区表，无法使用 SWITCH 优化。将直接对源表执行 BCP（可能长时间锁定）。分区键: {snapshot.SourcePartitionKey}", 
                        cancellationToken);
                }
            }

            // ============== 执行 BCP 归档 ==============
            ExecuteBcp: // 恢复流程的跳转点
            
            BcpResult? result = null; // 初始化结果变量,支持恢复和增量导入路径
            
            await AppendLogAsync(task.Id, "Step", "开始 BCP", 
                $"源查询: {sourceQuery}\n目标表: {snapshot.TargetTable}\n预期行数: {(expectedRowCount > 0 ? expectedRowCount.ToString("N0") : "未知")}", 
                cancellationToken);

            // 预先记录配置信息
            await AppendLogAsync(task.Id, "Debug", "BCP 配置", 
                $"批次大小: {snapshot.BatchSize}, 超时: {snapshot.TimeoutSeconds}秒, " +
                $"Native 格式: {snapshot.UseNativeFormat}, 最大错误: {snapshot.MaxErrors}", 
                cancellationToken);

            // ⚠️ 关键修复: 检查目标表是否已有临时表的数据(处理重复导入)
            // 注意: 跨服务器场景下无法执行此检查,因为目标服务器无法访问源服务器的临时表
            if (!string.IsNullOrWhiteSpace(tempTableName) && dataSource.UseSourceAsTarget)
            {
                try
                {
                    var targetParts = snapshot.TargetTable.Split('.');
                    var targetSchema = targetParts.Length > 1 ? targetParts[0].Trim('[', ']') : "dbo";
                    var targetTable = targetParts.Length > 1 ? targetParts[1].Trim('[', ']') : targetParts[0].Trim('[', ']');
                    
                    // 检查目标表中是否已有临时表的数据
                    var duplicateCheckSql = $@"
                        SELECT COUNT_BIG(*)
                        FROM [{targetSchema}].[{targetTable}] t
                        WHERE EXISTS (
                            SELECT 1 FROM [{snapshot.SchemaName}].[{tempTableName}] s
                            WHERE s.Id = t.Id  -- 假设主键是 Id
                        )";
                    
                    using var checkConn = new SqlConnection(targetConnectionString);
                    await checkConn.OpenAsync(cancellationToken);
                    using var checkCmd = new SqlCommand(duplicateCheckSql, checkConn);
                    var duplicateCount = (long)(await checkCmd.ExecuteScalarAsync(cancellationToken) ?? 0L);
                    
                    if (duplicateCount > 0)
                    {
                        // ❌ 发现重复数据,直接报错中断
                        var errorMessage = $"目标表已存在 {duplicateCount:N0} 行待归档数据，无法继续归档。\n" +
                                         $"临时表: [{snapshot.SchemaName}].[{tempTableName}]\n" +
                                         $"目标表: [{targetSchema}].[{targetTable}]\n\n" +
                                         $"请按以下步骤处理:\n" +
                                         $"1. 检查目标表中的重复数据:\n" +
                                         $"   SELECT * FROM [{targetSchema}].[{targetTable}] WHERE Id IN (SELECT Id FROM [{snapshot.SchemaName}].[{tempTableName}])\n" +
                                         $"2. 手动删除目标表中的重复数据(如果是误操作):\n" +
                                         $"   DELETE FROM [{targetSchema}].[{targetTable}] WHERE Id IN (SELECT Id FROM [{snapshot.SchemaName}].[{tempTableName}])\n" +
                                         $"3. 处理完成后,重新提交此任务将自动从临时表继续归档";
                        
                        await AppendLogAsync(task.Id, "Error", "发现重复数据", errorMessage, cancellationToken);
                        
                        // 保留临时表供用户检查和重新提交
                        await AppendLogAsync(task.Id, "Warning", "临时表保留", 
                            $"临时表 [{snapshot.SchemaName}].[{tempTableName}] 已保留，包含 {expectedRowCount:N0} 行数据。\n" +
                            $"请手动处理重复数据后，重新提交此任务继续归档。", 
                            cancellationToken);
                        
                        task.UpdateProgress(1.0, "SYSTEM");
                        task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
                        task.MarkFailed("SYSTEM", $"目标表已存在 {duplicateCount:N0} 行重复数据，请手动处理后重新提交");
                        await taskRepository.UpdateAsync(task, cancellationToken);
                        return;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "检查重复数据时出错，继续使用 BCP");
                    await AppendLogAsync(task.Id, "Warning", "重复检查失败", 
                        $"无法检查重复数据: {ex.Message}，继续使用 BCP 导入", 
                        cancellationToken);
                }
            }
            else if (!string.IsNullOrWhiteSpace(tempTableName) && !dataSource.UseSourceAsTarget)
            {
                // 跨服务器场景:无法检查重复数据
                await AppendLogAsync(task.Id, "Info", "跨服务器归档", 
                    "跨服务器归档模式:目标服务器无法访问源服务器的临时表,跳过重复数据检查", 
                    cancellationToken);
            }

            // 执行 BCP 归档
            // 注意: Progress 回调中不能访问 DbContext,会导致并发问题
            // 进度更新已通过心跳机制处理,这里只更新内存状态
            var progress = new Progress<BulkCopyProgress>(p =>
            {
                task.UpdateProgress(0.4 + p.PercentComplete * 0.5 / 100, "SYSTEM");
                // 移除数据库更新: _ = taskRepository.UpdateAsync(task, CancellationToken.None);
            });

            var bcpOptions = new BcpOptions
            {
                TempDirectory = snapshot.TempDirectory,
                BatchSize = snapshot.BatchSize,
                UseNativeFormat = snapshot.UseNativeFormat,
                MaxErrors = snapshot.MaxErrors,
                TimeoutSeconds = snapshot.TimeoutSeconds,
                KeepTempFiles = false
            };

            result = await bcpExecutor.ExecuteAsync(
                sourceConnectionString,
                targetConnectionString,
                sourceQuery,
                snapshot.TargetTable,
                bcpOptions,
                progress,
                cancellationToken);
            
            stepWatch.Stop();

            // 详细记录 BCP 执行结果 (优化: 只记录摘要,避免日志过大)
            if (result != null)
            {
                // 提取命令输出的最后几行 (通常包含总结信息)
                var outputSummary = GetCommandOutputSummary(result.CommandOutput, maxLines: 5);
                
                await AppendLogAsync(task.Id, "Debug", "BCP 执行结果", 
                    $"成功: {result.Succeeded}\n" +
                    $"复制行数: {result.RowsCopied:N0}\n" +
                    $"耗时: {result.Duration:g}\n" +
                    $"吞吐量: {result.ThroughputRowsPerSecond:N0} 行/秒\n" +
                    $"临时文件: {result.TempFilePath ?? "已清理"}\n" +
                    $"输出摘要 (最后 5 行):\n{outputSummary}", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);
            }

            if (result == null || !result.Succeeded)
            {
                // 失败时记录完整输出以便排查
                await AppendLogAsync(task.Id, "Error", "BCP 导出失败", 
                    $"BCP 进程退出出错 {result?.ErrorMessage ?? "未知错误"}\n\n完整输出:\n{result?.CommandOutput ?? "无输出"}", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                // BCP 失败，保留临时表供人工检查
                if (!string.IsNullOrWhiteSpace(tempTableName))
                {
                    await AppendLogAsync(task.Id, "Warning", "临时表保留", 
                        $"归档失败，临时表 [{snapshot.SchemaName}].[{tempTableName}] 已保留，可手动处理或回滚", 
                        cancellationToken);
                }

                task.UpdateProgress(1.0, "SYSTEM");
                task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
                task.MarkFailed("SYSTEM", result?.ErrorMessage ?? "BCP 执行失败");
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "BCP 归档完成", 
                $"成功归档 {result.RowsCopied:N0} 行数据,耗时: {result.Duration:g},吞吐量: {result.ThroughputRowsPerSecond:N0} 行/秒", 
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            // ============== 清理临时表 ==============
            
            if (!string.IsNullOrWhiteSpace(tempTableName))
            {
                try
                {
                    await AppendLogAsync(task.Id, "Step", "开始清理", 
                        $"准备删除临时表 [{snapshot.SchemaName}].[{tempTableName}]", 
                        cancellationToken);

                    await partitionSwitchHelper.DropTempTableAsync(
                        sourceConnectionString,
                        snapshot.SchemaName,
                        tempTableName,
                        cancellationToken);

                    await AppendLogAsync(task.Id, "Step", "清理临时表", 
                        $"临时表 [{snapshot.SchemaName}].[{tempTableName}] 已成功删除", 
                        cancellationToken);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "删除临时表失败: {Schema}.{TempTable}", snapshot.SchemaName, tempTableName);
                    await AppendLogAsync(task.Id, "Warning", "清理失败", 
                        $"临时表删除失败: {ex.Message}\n堆栈: {ex.StackTrace}\n需要手动清理表: [{snapshot.SchemaName}].[{tempTableName}]", 
                        cancellationToken);
                }
            }
            else if (usedPartitionSwitch)
            {
                await AppendLogAsync(task.Id, "Warning", "清理跳过", 
                    $"使用了分区优化但临时表名为空，请检查是否有遗留临时表", 
                    cancellationToken);
            }

            task.UpdateProgress(0.95, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 6: 完成 ==============
            overallStopwatch.Stop();

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");

            var summary = JsonSerializer.Serialize(new
            {
                rowsCopied = result.RowsCopied,
                duration = result.Duration.ToString("g"),
                throughput = result.ThroughputRowsPerSecond,
                sourceTable = $"{snapshot.SchemaName}.{snapshot.TableName}",
                targetTable = snapshot.TargetTable,
                partitionKey = snapshot.SourcePartitionKey,
                usedPartitionSwitch = usedPartitionSwitch,
                tempTable = tempTableName
            });

            task.MarkSucceeded("SYSTEM", summary);
            await taskRepository.UpdateAsync(task, cancellationToken);

            await AppendLogAsync(task.Id, "Info", "任务完成", 
                $"BCP 归档任务成功完成,总耗时: {overallStopwatch.Elapsed:g}。" +
                (usedPartitionSwitch ? $" 使用分区优化方案（SWITCH + BCP），生产表影响 < 1秒" : ""), 
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            overallStopwatch.Stop();
            logger.LogError(ex, "执行 BCP 归档任务时发生异常: {TaskId}", task.Id);

            await AppendLogAsync(
                task.Id,
                "Error",
                "执行异常",
                $"任务执行过程中发生未预期的错误:\n{ex.Message}\n{ex.StackTrace}",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkFailed("SYSTEM", ex.Message);
            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }

    /// <summary>
    /// 执行 BulkCopy 归档任务
    /// </summary>
    private async Task ExecuteArchiveBulkCopyAsync(BackgroundTask task, CancellationToken cancellationToken)
    {
        var overallStopwatch = Stopwatch.StartNew();

        try
        {
            // ============== 阶段 1: 解析快照 ==============
            await AppendLogAsync(task.Id, "Info", "任务启动", 
                $"任务由 {task.RequestedBy} 发起,操作类型: BulkCopy 归档。", cancellationToken);

            task.MarkValidating("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Validation, "SYSTEM");
            task.UpdateProgress(0.1, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            if (string.IsNullOrWhiteSpace(task.ConfigurationSnapshot))
            {
                await HandleValidationFailureAsync(task, "任务快照数据为空,无法执行。", cancellationToken);
                return;
            }

            var snapshot = JsonSerializer.Deserialize<ArchiveBulkCopySnapshot>(task.ConfigurationSnapshot);
            if (snapshot is null)
            {
                await HandleValidationFailureAsync(task, "无法解析 BulkCopy 归档快照数据。", cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "解析快照", 
                $"源表: {snapshot.SchemaName}.{snapshot.TableName}, 分区: {snapshot.SourcePartitionKey}, " +
                $"目标: {snapshot.TargetDatabase}.{snapshot.TargetTable}", 
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

            // ============== 阶段 3: 构建连接字符串 ==============
            var sourceConnectionString = BuildConnectionString(dataSource);
            var targetConnectionString = BuildTargetConnectionString(dataSource, snapshot.TargetDatabase);

            await AppendLogAsync(task.Id, "Info", "BulkCopy执行连接信息", 
                $"UseSourceAsTarget={dataSource.UseSourceAsTarget}, TargetDatabase={snapshot.TargetDatabase}", 
                cancellationToken);

            await AppendLogAsync(task.Id, "Step", "准备归档", 
                $"准备执行 BulkCopy 归档,目标数据库: {snapshot.TargetDatabase},批次大小: {snapshot.BatchSize}", 
                cancellationToken);

            task.UpdateProgress(0.25, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 4: 进入执行队列 ==============
            task.MarkQueued("SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);
            await AppendLogAsync(task.Id, "Step", "进入队列", "准备工作完成,任务进入执行队列。", cancellationToken);

            // ============== 阶段 5: 开始执行 BulkCopy 归档 ==============
            task.MarkRunning("SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Executing, "SYSTEM");
            task.UpdateProgress(0.3, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            var stepWatch = Stopwatch.StartNew();
            await AppendLogAsync(task.Id, "Step", "执行 BulkCopy 归档", 
                $"正在通过 SqlBulkCopy 流式传输数据...", 
                cancellationToken);

            // 构建源查询 SQL
            // 注意：SourcePartitionKey 在 BCP/BulkCopy 场景下可能是分区键值或其他筛选条件
            // 这里简化实现，直接导出整个表（实际应该根据业务需求添加 WHERE 条件）
            string sourceQuery;
            if (!string.IsNullOrWhiteSpace(snapshot.SourcePartitionKey))
            {
                // 如果提供了分区键，尝试作为筛选条件（需要根据实际表结构调整）
                // TODO: 这里需要根据实际的分区列名动态构建 WHERE 条件
                sourceQuery = $"SELECT * FROM [{snapshot.SchemaName}].[{snapshot.TableName}]";
                
                await AppendLogAsync(task.Id, "Warning", "分区键筛选", 
                    $"当前实现暂不支持按分区键筛选，将导出整个表。分区键: {snapshot.SourcePartitionKey}", 
                    cancellationToken);
            }
            else
            {
                sourceQuery = $"SELECT * FROM [{snapshot.SchemaName}].[{snapshot.TableName}]";
            }

            // 执行 BulkCopy 归档
            // 注意: Progress 回调中不能访问 DbContext,会导致并发问题
            // 进度更新已通过心跳机制处理,这里只更新内存状态
            var progress = new Progress<BulkCopyProgress>(p =>
            {
                task.UpdateProgress(0.4 + p.PercentComplete * 0.5 / 100, "SYSTEM");
                // 移除数据库更新: _ = taskRepository.UpdateAsync(task, CancellationToken.None);
            });

            var bulkCopyOptions = new BulkCopyOptions
            {
                BatchSize = snapshot.BatchSize,
                NotifyAfterRows = snapshot.NotifyAfterRows,
                TimeoutSeconds = snapshot.TimeoutSeconds
            };

            var result = await bulkCopyExecutor.ExecuteAsync(
                sourceConnectionString,
                targetConnectionString,
                sourceQuery,
                snapshot.TargetTable,
                bulkCopyOptions,
                progress,
                cancellationToken);

            stepWatch.Stop();

            if (!result.Succeeded)
            {
                await AppendLogAsync(task.Id, "Error", "BulkCopy 归档失败", 
                    $"BulkCopy 执行失败: {result.ErrorMessage}", 
                    cancellationToken,
                    durationMs: stepWatch.ElapsedMilliseconds);

                task.UpdateProgress(1.0, "SYSTEM");
                task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
                task.MarkFailed("SYSTEM", result.ErrorMessage ?? "BulkCopy 执行失败");
                await taskRepository.UpdateAsync(task, cancellationToken);
                return;
            }

            await AppendLogAsync(task.Id, "Info", "BulkCopy 归档完成", 
                $"成功归档 {result.RowsCopied:N0} 行数据,耗时: {result.Duration:g},吞吐量: {result.ThroughputRowsPerSecond:N0} 行/秒", 
                cancellationToken,
                durationMs: stepWatch.ElapsedMilliseconds);

            task.UpdateProgress(0.95, "SYSTEM");
            await taskRepository.UpdateAsync(task, cancellationToken);

            // ============== 阶段 6: 完成 ==============
            overallStopwatch.Stop();

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");

            var summary = JsonSerializer.Serialize(new
            {
                rowsCopied = result.RowsCopied,
                duration = result.Duration.ToString("g"),
                throughput = result.ThroughputRowsPerSecond,
                sourceTable = $"{snapshot.SchemaName}.{snapshot.TableName}",
                targetTable = snapshot.TargetTable,
                partitionKey = snapshot.SourcePartitionKey
            });

            task.MarkSucceeded("SYSTEM", summary);
            await taskRepository.UpdateAsync(task, cancellationToken);

            await AppendLogAsync(task.Id, "Info", "任务完成", 
                $"BulkCopy 归档任务成功完成,总耗时: {overallStopwatch.Elapsed:g}", 
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            overallStopwatch.Stop();
            logger.LogError(ex, "执行 BulkCopy 归档任务时发生异常: {TaskId}", task.Id);

            await AppendLogAsync(
                task.Id,
                "Error",
                "执行异常",
                $"任务执行过程中发生未预期的错误:\n{ex.Message}\n{ex.StackTrace}",
                cancellationToken,
                durationMs: overallStopwatch.ElapsedMilliseconds);

            task.UpdateProgress(1.0, "SYSTEM");
            task.UpdatePhase(BackgroundTaskPhases.Finalizing, "SYSTEM");
            task.MarkFailed("SYSTEM", ex.Message);
            await taskRepository.UpdateAsync(task, cancellationToken);
        }
    }

    /// <summary>
    /// 获取现有的临时表列表（用于清理上次失败遗留的临时表）
    /// </summary>
    private static async Task<List<string>> GetExistingTempTablesAsync(
        string connectionString,
        string schemaName,
        string baseTableName,
        CancellationToken cancellationToken)
    {
        const string sql = @"
            SELECT name 
            FROM sys.tables 
            WHERE schema_id = SCHEMA_ID(@SchemaName)
              AND name LIKE @Pattern
            ORDER BY create_date DESC";

        var pattern = $"{baseTableName}_Temp_%";
        var tempTables = new List<string>();

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@SchemaName", schemaName);
        cmd.Parameters.AddWithValue("@Pattern", pattern);

        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            tempTables.Add(reader.GetString(0));
        }

        return tempTables;
    }

    /// <summary>
    /// 获取指定表的行数
    /// </summary>
    private static async Task<long> GetTableRowCountAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = "SELECT COUNT_BIG(*) FROM [{0}].[{1}]";
        var query = string.Format(sql, schemaName, tableName);

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        using var cmd = new SqlCommand(query, conn);
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        
        return result is long count ? count : 0;
    }

    /// <summary>
    /// 从命令输出中提取最后 N 行(摘要信息)
    /// </summary>
    private static string GetCommandOutputSummary(string? output, int maxLines = 5)
    {
        if (string.IsNullOrWhiteSpace(output))
        {
            return "(无输出)";
        }

        var lines = output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
        if (lines.Length <= maxLines)
        {
            return output;
        }

        // 返回最后 maxLines 行
        var summaryLines = lines.TakeLast(maxLines);
        return string.Join(Environment.NewLine, summaryLines);
    }

    /// <summary>
    /// 构建数据库连接字符串
    /// </summary>
    private string BuildConnectionString(ArchiveDataSource dataSource)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = dataSource.ServerPort == 1433
                ? dataSource.ServerAddress
                : $"{dataSource.ServerAddress},{dataSource.ServerPort}",
            InitialCatalog = dataSource.DatabaseName,
            IntegratedSecurity = dataSource.UseIntegratedSecurity,
            TrustServerCertificate = true,
            ConnectTimeout = 30
        };

        if (!dataSource.UseIntegratedSecurity)
        {
            builder.UserID = dataSource.UserName;
            // 解密密码
            if (!string.IsNullOrEmpty(dataSource.Password))
            {
                builder.Password = passwordEncryptionService.Decrypt(dataSource.Password);
            }
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// 构建目标服务器连接字符串(支持自定义目标服务器)
    /// </summary>
    /// <param name="dataSource">数据源配置</param>
    /// <param name="targetDatabase">目标数据库名(可选,用于覆盖默认目标数据库)</param>
    /// <returns>目标服务器连接字符串</returns>
    private string BuildTargetConnectionString(ArchiveDataSource dataSource, string? targetDatabase = null)
    {
        // 如果使用源服务器作为目标服务器
        if (dataSource.UseSourceAsTarget)
        {
            // 如果指定了目标数据库,则使用源连接字符串但切换数据库
            if (!string.IsNullOrWhiteSpace(targetDatabase))
            {
                var builder = new SqlConnectionStringBuilder(BuildConnectionString(dataSource))
                {
                    InitialCatalog = targetDatabase
                };
                return builder.ConnectionString;
            }
            // 否则直接使用源连接字符串
            return BuildConnectionString(dataSource);
        }

        // 使用自定义目标服务器配置
        var targetBuilder = new SqlConnectionStringBuilder
        {
            DataSource = dataSource.TargetServerPort == 1433
                ? dataSource.TargetServerAddress
                : $"{dataSource.TargetServerAddress},{dataSource.TargetServerPort}",
            // 优先使用传入的目标数据库,其次使用配置的目标数据库,最后使用源数据库名
            InitialCatalog = targetDatabase ?? dataSource.TargetDatabaseName ?? dataSource.DatabaseName,
            IntegratedSecurity = dataSource.TargetUseIntegratedSecurity,
            TrustServerCertificate = true,
            ConnectTimeout = 30
        };

        if (!dataSource.TargetUseIntegratedSecurity)
        {
            targetBuilder.UserID = dataSource.TargetUserName;
            // 解密密码
            if (!string.IsNullOrEmpty(dataSource.TargetPassword))
            {
                targetBuilder.Password = passwordEncryptionService.Decrypt(dataSource.TargetPassword);
            }
        }

        return targetBuilder.ConnectionString;
    }
}
