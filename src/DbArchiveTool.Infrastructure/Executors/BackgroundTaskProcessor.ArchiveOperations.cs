using System.Data;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.SqlExecution;
using DbArchiveTool.Shared.Archive;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// BackgroundTaskProcessor 部分类 - 归档操作相关方法
/// 包含分区切换、BCP归档、BulkCopy归档的配置构建和执行逻辑
/// </summary>
internal sealed partial class BackgroundTaskProcessor
{
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
                            
                            // 跳到 BulkCopy 执行阶段
                            goto ExecuteBulkCopy;
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
                // 非分区表或未指定分区键，直接对源表执行 BulkCopy
                sourceQuery = $"SELECT * FROM [{snapshot.SchemaName}].[{snapshot.TableName}]";
                
                if (!string.IsNullOrWhiteSpace(snapshot.SourcePartitionKey))
                {
                    await AppendLogAsync(task.Id, "Warning", "分区键筛选", 
                        $"表不是分区表，无法使用 SWITCH 优化。将直接对源表执行 BulkCopy（可能长时间锁定）。分区键: {snapshot.SourcePartitionKey}", 
                        cancellationToken);
                }
            }

            // ============== 执行 BulkCopy 归档 ==============
            BulkCopyResult? result = null; // 初始化结果变量,支持恢复和增量导入路径
            
            ExecuteBulkCopy: // 恢复流程的跳转点
            
            await AppendLogAsync(task.Id, "Step", "开始 BulkCopy", 
                $"源查询: {sourceQuery}\n目标表: {snapshot.TargetTable}\n预期行数: {(expectedRowCount > 0 ? expectedRowCount.ToString("N0") : "未知")}", 
                cancellationToken);

            // 预先记录配置信息
            await AppendLogAsync(task.Id, "Debug", "BulkCopy 配置", 
                $"批次大小: {snapshot.BatchSize}, 超时: {snapshot.TimeoutSeconds}秒, " +
                $"每批通知行数: {snapshot.NotifyAfterRows}", 
                cancellationToken);

            // ⚠️ 关键优化: 检查目标表是否已有数据,启用增量导入模式
            // 注意: 跨服务器场景下无法执行此检查,因为目标服务器无法访问源服务器的临时表/源表
            long existingRowsInTarget = 0;
            List<string> primaryKeyColumns = new();
            
            if (dataSource.UseSourceAsTarget) // 仅同服务器场景支持增量导入
            {
                try
                {
                    var targetParts = snapshot.TargetTable.Split('.');
                    var targetSchema = targetParts.Length > 1 ? targetParts[0].Trim('[', ']') : "dbo";
                    var targetTable = targetParts.Length > 1 ? targetParts[1].Trim('[', ']') : targetParts[0].Trim('[', ']');
                    
                    // 1. 获取主键列信息
                    var primaryKeySql = $@"
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                        AND TABLE_SCHEMA = @Schema AND TABLE_NAME = @Table
                        ORDER BY ORDINAL_POSITION";
                    
                    using var pkConn = new SqlConnection(targetConnectionString);
                    await pkConn.OpenAsync(cancellationToken);
                    using var pkCmd = new SqlCommand(primaryKeySql, pkConn);
                    pkCmd.Parameters.AddWithValue("@Schema", targetSchema);
                    pkCmd.Parameters.AddWithValue("@Table", targetTable);
                    
                    using var pkReader = await pkCmd.ExecuteReaderAsync(cancellationToken);
                    while (await pkReader.ReadAsync(cancellationToken))
                    {
                        primaryKeyColumns.Add(pkReader.GetString(0));
                    }
                    
                    if (primaryKeyColumns.Count == 0)
                    {
                        await AppendLogAsync(task.Id, "Warning", "增量导入检查", 
                            $"目标表 [{targetSchema}].[{targetTable}] 没有定义主键，无法启用增量导入模式。将导入所有数据（可能产生重复）。", 
                            cancellationToken);
                    }
                    else
                    {
                        // 2. 检查目标表中是否已有源表/临时表的数据
                        var sourceTableRef = !string.IsNullOrWhiteSpace(tempTableName) 
                            ? $"[{snapshot.SchemaName}].[{tempTableName}]" 
                            : $"[{snapshot.SchemaName}].[{snapshot.TableName}]";
                        
                        var pkJoinCondition = string.Join(" AND ", 
                            primaryKeyColumns.Select(col => $"s.[{col}] = t.[{col}]"));
                        
                        var duplicateCheckSql = $@"
                            SELECT COUNT_BIG(*)
                            FROM [{targetSchema}].[{targetTable}] t
                            WHERE EXISTS (
                                SELECT 1 FROM {sourceTableRef} s
                                WHERE {pkJoinCondition}
                            )";
                        
                        using var checkConn = new SqlConnection(targetConnectionString);
                        await checkConn.OpenAsync(cancellationToken);
                        using var checkCmd = new SqlCommand(duplicateCheckSql, checkConn);
                        existingRowsInTarget = (long)(await checkCmd.ExecuteScalarAsync(cancellationToken) ?? 0L);
                        
                        if (existingRowsInTarget > 0)
                        {
                            await AppendLogAsync(task.Id, "Info", "增量导入模式", 
                                $"目标表已存在 {existingRowsInTarget:N0} 行数据，将启用增量导入模式（只导入目标表不存在的数据）。\n" +
                                $"主键列: {string.Join(", ", primaryKeyColumns)}", 
                                cancellationToken);
                            
                            // 3. 修改源查询SQL,只查询目标表不存在的数据
                            var pkColumns = string.Join(", ", primaryKeyColumns.Select(col => $"[{col}]"));
                            var pkJoinConditionInQuery = string.Join(" AND ", 
                                primaryKeyColumns.Select(col => $"t.[{col}] = s.[{col}]"));
                            
                            // 从原始查询中提取表名
                            var fromClause = sourceQuery.Contains("WHERE") 
                                ? sourceQuery.Substring(0, sourceQuery.IndexOf("WHERE")).Trim()
                                : sourceQuery;
                            
                            var whereClause = sourceQuery.Contains("WHERE")
                                ? sourceQuery.Substring(sourceQuery.IndexOf("WHERE"))
                                : "";
                            
                            // 构建增量查询SQL
                            sourceQuery = $@"
                                SELECT s.* 
                                FROM ({sourceQuery}) s
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM [{targetSchema}].[{targetTable}] t
                                    WHERE {pkJoinConditionInQuery}
                                )";
                            
                            await AppendLogAsync(task.Id, "Debug", "增量查询SQL", 
                                $"已修改源查询为增量模式:\n{sourceQuery}", 
                                cancellationToken);
                        }
                        else
                        {
                            await AppendLogAsync(task.Id, "Info", "目标表状态", 
                                "目标表为空，将执行全量导入。", 
                                cancellationToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "检查目标表状态时出错，将使用全量导入模式");
                    await AppendLogAsync(task.Id, "Warning", "增量导入检查失败", 
                        $"无法检查目标表状态: {ex.Message}，将使用全量导入模式（可能产生重复数据）", 
                        cancellationToken);
                }
            }
            else
            {
                // 跨服务器场景:无法检查重复数据
                await AppendLogAsync(task.Id, "Info", "跨服务器归档", 
                    "跨服务器归档模式:目标服务器无法访问源服务器数据,无法启用增量导入,将执行全量导入", 
                    cancellationToken);
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

            result = await bulkCopyExecutor.ExecuteAsync(
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

                // 如果使用了临时表，保留它以便恢复
                if (!string.IsNullOrWhiteSpace(tempTableName))
                {
                    await AppendLogAsync(task.Id, "Warning", "临时表保留", 
                        $"临时表 [{snapshot.SchemaName}].[{tempTableName}] 已保留，包含 {expectedRowCount:N0} 行数据。\n" +
                        $"请修复错误后，重新提交此任务将自动从临时表继续归档。", 
                        cancellationToken);
                }

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

            // ============== 行数验证 ==============
            if (existingRowsInTarget > 0)
            {
                // 增量导入模式: 实际导入数 = 预期总数 - 已存在数
                var expectedNewRows = expectedRowCount - existingRowsInTarget;
                
                if (result.RowsCopied != expectedNewRows && expectedNewRows > 0)
                {
                    await AppendLogAsync(task.Id, "Warning", "行数不一致", 
                        $"增量导入模式 - 预期导入: {expectedNewRows:N0} 行 (总数 {expectedRowCount:N0} - 已存在 {existingRowsInTarget:N0}), " +
                        $"实际导入: {result.RowsCopied:N0} 行, 差异: {Math.Abs(result.RowsCopied - expectedNewRows):N0}", 
                        cancellationToken);
                }
                else if (expectedNewRows > 0)
                {
                    await AppendLogAsync(task.Id, "Info", "行数验证", 
                        $"增量导入行数验证通过 - 新增 {result.RowsCopied:N0} 行 (总数 {expectedRowCount:N0}, 已存在 {existingRowsInTarget:N0})", 
                        cancellationToken);
                }
                else if (result.RowsCopied == 0 && expectedNewRows <= 0)
                {
                    await AppendLogAsync(task.Id, "Info", "行数验证", 
                        "目标表已包含所有数据，无需导入新数据", 
                        cancellationToken);
                }
            }
            else if (expectedRowCount > 0 && result.RowsCopied != expectedRowCount)
            {
                // 全量导入模式: 验证总数
                await AppendLogAsync(task.Id, "Warning", "行数不一致", 
                    $"全量导入模式 - 预期行数: {expectedRowCount:N0}, 实际复制: {result.RowsCopied:N0}, 差异: {Math.Abs(result.RowsCopied - expectedRowCount):N0}", 
                    cancellationToken);
            }
            else if (expectedRowCount > 0)
            {
                await AppendLogAsync(task.Id, "Info", "行数验证", 
                    $"全量导入行数验证通过: {result.RowsCopied:N0} 行", 
                    cancellationToken);
            }

            // ============== 删除临时表 ==============
            if (!string.IsNullOrWhiteSpace(tempTableName) && usedPartitionSwitch)
            {
                try
                {
                    await partitionSwitchHelper.DropTempTableAsync(
                        sourceConnectionString,
                        snapshot.SchemaName,
                        tempTableName,
                        cancellationToken);

                    await AppendLogAsync(task.Id, "Step", "清理临时表", 
                        $"临时表已删除: [{snapshot.SchemaName}].[{tempTableName}]", 
                        cancellationToken);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "删除临时表失败: {TempTableName}", tempTableName);
                    await AppendLogAsync(task.Id, "Warning", "清理临时表失败", 
                        $"无法删除临时表 [{snapshot.SchemaName}].[{tempTableName}]: {ex.Message}\n" +
                        $"请手动执行: DROP TABLE [{snapshot.SchemaName}].[{tempTableName}]", 
                        cancellationToken);
                }
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
}
