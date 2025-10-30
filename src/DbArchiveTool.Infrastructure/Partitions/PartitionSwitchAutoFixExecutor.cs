using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.Executors;
using DbArchiveTool.Infrastructure.Models;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Partitions;

/// <summary>
/// SQL Server 环境下的分区切换自动补齐执行器。
/// </summary>
internal sealed class PartitionSwitchAutoFixExecutor : IPartitionSwitchAutoFixExecutor
{
    private readonly IDbConnectionFactory connectionFactory;
    private readonly ISqlExecutor sqlExecutor;
    private readonly SqlPartitionCommandExecutor partitionCommandExecutor;
    private readonly IPartitionConfigurationRepository configurationRepository;
    private readonly IPartitionAuditLogRepository auditLogRepository;
    private readonly ILogger<PartitionSwitchAutoFixExecutor> logger;

    public PartitionSwitchAutoFixExecutor(
        IDbConnectionFactory connectionFactory,
        ISqlExecutor sqlExecutor,
        SqlPartitionCommandExecutor partitionCommandExecutor,
        IPartitionConfigurationRepository configurationRepository,
        IPartitionAuditLogRepository auditLogRepository,
        ILogger<PartitionSwitchAutoFixExecutor> logger)
    {
        this.connectionFactory = connectionFactory;
        this.sqlExecutor = sqlExecutor;
        this.partitionCommandExecutor = partitionCommandExecutor;
        this.configurationRepository = configurationRepository;
        this.auditLogRepository = auditLogRepository;
        this.logger = logger;
    }

    /// <inheritdoc />
    public async Task<PartitionSwitchAutoFixResult> ExecuteAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        PartitionSwitchInspectionContext context,
        IReadOnlyList<PartitionSwitchPlanAutoFix> steps,
        CancellationToken cancellationToken = default)
    {
        if (steps.Count == 0)
        {
            return PartitionSwitchAutoFixResult.Success(Array.Empty<PartitionSwitchAutoFixExecution>());
        }

        var executions = new List<PartitionSwitchAutoFixExecution>(steps.Count);
        var rollbackStack = new Stack<AutoFixRollbackAction>();

        foreach (var step in steps)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                var outcome = step.Code switch
                {
                    "CreateTargetTable" => await ExecuteCreateTargetTableAsync(dataSourceId, configuration, context, cancellationToken),
                    "CleanupResidualData" => await ExecuteCleanupTargetTableAsync(dataSourceId, context, cancellationToken),
                    "RefreshStatistics" => await ExecuteRefreshStatisticsAsync(dataSourceId, context, cancellationToken),
                    "SyncPartitionObjects" => await ExecuteSyncPartitionObjectsAsync(dataSourceId, configuration, cancellationToken),
                    "SyncIndexes" => await ExecuteSyncIndexesAsync(dataSourceId, configuration, context, cancellationToken),
                    "SyncConstraints" => await ExecuteSyncConstraintsAsync(dataSourceId, configuration, context, cancellationToken),
                    "AlignSourceIndexes" => await ExecuteAlignSourceIndexesAsync(dataSourceId, configuration, cancellationToken),
                    "AlignTargetIndexes" => await ExecuteAlignTargetIndexesAsync(dataSourceId, configuration, context, cancellationToken),
                    _ => throw new NotSupportedException($"暂不支持的自动补齐步骤：{step.Code}")
                };

                stopwatch.Stop();
                var execution = outcome.Execution with { ElapsedMilliseconds = stopwatch.ElapsedMilliseconds };
                executions.Add(execution);

                if (outcome.RollbackAction is not null)
                {
                    rollbackStack.Push(outcome.RollbackAction);
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                logger.LogError(ex, "自动补齐步骤 {Code} 执行失败。", step.Code);

                executions.Add(new PartitionSwitchAutoFixExecution(
                    step.Code,
                    false,
                    ex.Message,
                    $"-- 自动补齐步骤失败：{step.Title}",
                    stopwatch.ElapsedMilliseconds));

                var rollbackExecutions = await TryRollbackAsync(context, rollbackStack, cancellationToken);
                executions.AddRange(rollbackExecutions);

                // 记录失败的审计日志
                var failureResult = PartitionSwitchAutoFixResult.Failure(executions);
                await RecordAuditLogAsync(configuration, steps, executions, failureResult.Succeeded, cancellationToken);

                return failureResult;
            }
        }

        // 记录成功的审计日志
        var result = PartitionSwitchAutoFixResult.Success(executions);
        await RecordAuditLogAsync(configuration, steps, executions, result.Succeeded, cancellationToken);

        return result;
    }

    /// <summary>
    /// 记录自动补齐操作的审计日志
    /// </summary>
    private async Task RecordAuditLogAsync(
        PartitionConfiguration configuration,
        IReadOnlyList<PartitionSwitchPlanAutoFix> steps,
        List<PartitionSwitchAutoFixExecution> executions,
        bool succeeded,
        CancellationToken cancellationToken)
    {
        try
        {
            var summary = $"对分区表 [{configuration.SchemaName}].[{configuration.TableName}] 执行自动补齐: " +
                          $"共 {steps.Count} 个步骤, " +
                          $"成功 {executions.Count(e => e.Succeeded)} 个, " +
                          $"失败 {executions.Count(e => !e.Succeeded)} 个";

            var payloadJson = System.Text.Json.JsonSerializer.Serialize(new
            {
                Steps = steps.Select(s => new { s.Code, s.Title, s.Category, s.ImpactScope }).ToList(),
                Executions = executions.Select(e => new
                {
                    e.Code,
                    e.Succeeded,
                    e.Message,
                    ElapsedMs = e.ElapsedMilliseconds
                }).ToList()
            });

            var combinedScript = string.Join(
                Environment.NewLine + Environment.NewLine + "-- ============================================" + Environment.NewLine + Environment.NewLine,
                executions.Select(e => $"-- {e.Code}: {e.Message}" + Environment.NewLine + e.Script));

            var auditLog = PartitionAuditLog.Create(
                userId: "SYSTEM", // TODO: 从上下文获取当前用户
                action: "PartitionSwitchAutoFix",
                resourceType: "PartitionConfiguration",
                resourceId: configuration.Id.ToString(),
                summary: summary,
                payloadJson: payloadJson,
                result: succeeded ? "Success" : "Failure",
                script: combinedScript);

            await auditLogRepository.AddAsync(auditLog, cancellationToken);

            logger.LogInformation("已记录自动补齐审计日志: {Summary}", summary);
        }
        catch (Exception ex)
        {
            // 审计日志记录失败不应影响主流程
            logger.LogError(ex, "记录自动补齐审计日志时发生异常");
        }
    }

    // 创建目标表脚本，复制源表列定义（不包含约束/索引，后续步骤再同步）
    private async Task<AutoFixStepOutcome> ExecuteCreateTargetTableAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        PartitionSwitchInspectionContext context,
        CancellationToken cancellationToken)
    {
        await using var targetConnection = context.UseSourceAsTarget
            ? await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken)
            : await connectionFactory.CreateTargetSqlConnectionAsync(dataSourceId, cancellationToken);

        if (!context.UseSourceAsTarget && !string.IsNullOrWhiteSpace(context.TargetDatabase))
        {
            targetConnection.ChangeDatabase(context.TargetDatabase);
        }

        var schemaExistsSql = @"SELECT COUNT(1) FROM sys.schemas WHERE name = @SchemaName";
        var schemaExists = await sqlExecutor.QuerySingleAsync<int>(
            targetConnection,
            schemaExistsSql,
            new { SchemaName = context.TargetSchema });

        if (schemaExists == 0)
        {
            var createSchemaSql = $"CREATE SCHEMA [{context.TargetSchema}];";
            await sqlExecutor.ExecuteAsync(targetConnection, createSchemaSql, timeoutSeconds: 30);
        }

        var tableExistsSql = @"SELECT COUNT(1) FROM sys.tables t INNER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = @SchemaName AND t.name = @TableName";
        var exists = await sqlExecutor.QuerySingleAsync<int>(
            targetConnection,
            tableExistsSql,
            new { SchemaName = context.TargetSchema, TableName = context.TargetTable });

        if (exists > 0)
        {
            return new AutoFixStepOutcome(
                new PartitionSwitchAutoFixExecution(
                    "CreateTargetTable",
                    true,
                    $"目标表 [{context.TargetSchema}].[{context.TargetTable}] 已存在，跳过创建。",
                    $"-- Skip create table for [{context.TargetSchema}].[{context.TargetTable}]",
                    0),
                null);
        }

        await using var sourceConnection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
        if (!string.IsNullOrWhiteSpace(context.SourceDatabase))
        {
            sourceConnection.ChangeDatabase(context.SourceDatabase);
        }

        var columns = await LoadColumnMetadataAsync(sourceConnection, configuration.SchemaName, configuration.TableName, cancellationToken);
        
        // 获取源表分区信息,创建相同分区结构的目标表
        var partitionInfo = await LoadPartitionInfoAsync(sourceConnection, configuration.SchemaName, configuration.TableName, cancellationToken);

        var scriptBuilder = new StringBuilder();
        var createTableScript = BuildCreateTableScript(context.TargetSchema, context.TargetTable, columns, partitionInfo);
        scriptBuilder.AppendLine(createTableScript);
        await sqlExecutor.ExecuteAsync(targetConnection, createTableScript, timeoutSeconds: 120);

        // 立即创建聚集索引和主键(必须先于其他唯一索引,因为分区表要求)
        var sourceIndexes = await LoadClusteredAndPrimaryKeyIndexesAsync(
            sourceConnection,
            configuration.SchemaName,
            configuration.TableName,
            cancellationToken);

        var schemaExistingNamesForCreate = await LoadSchemaIndexAndConstraintNamesAsync(
            targetConnection,
            context.TargetSchema,
            cancellationToken);

        foreach (var index in sourceIndexes)
        {
            var effectiveName = index.IsPrimaryKey
                ? $"PK_{context.TargetTable}"
                : index.IndexName;

            if (schemaExistingNamesForCreate.Contains(effectiveName))
            {
                effectiveName = GenerateUniqueIndexName(effectiveName, context.TargetTable, schemaExistingNamesForCreate);
            }

            schemaExistingNamesForCreate.Add(effectiveName);

            var indexScript = BuildCreateIndexScript(context.TargetSchema, context.TargetTable, index, effectiveName, partitionInfo);
            scriptBuilder.AppendLine("GO");
            scriptBuilder.AppendLine(indexScript);
            
            try
            {
                await sqlExecutor.ExecuteAsync(targetConnection, indexScript, timeoutSeconds: 300);
                logger.LogInformation("已创建{IndexType} {IndexName}", 
                    index.IsPrimaryKey ? "主键" : "聚集索引", effectiveName);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "创建{IndexType} {IndexName} 失败", 
                    index.IsPrimaryKey ? "主键" : "聚集索引", effectiveName);
                throw; // 主键/聚集索引失败必须中断,不能继续
            }
        }

        var script = scriptBuilder.ToString();
        var message = partitionInfo != null 
            ? $"已创建分区目标表 [{context.TargetSchema}].[{context.TargetTable}],分区列: {partitionInfo.ColumnName},并同步了聚集索引/主键。"
            : $"已创建目标表 [{context.TargetSchema}].[{context.TargetTable}],并同步了聚集索引/主键。";
        return new AutoFixStepOutcome(
            new PartitionSwitchAutoFixExecution("CreateTargetTable", true, message, script, 0),
            new AutoFixRollbackAction(async ct => await RollbackCreateTargetTableAsync(dataSourceId, context, ct)));
    }

    /// <summary>
    /// 清空目标分区的残留数据，以满足 SWITCH 操作要求（目标分区必须为空）
    /// </summary>
    private async Task<AutoFixStepOutcome> ExecuteCleanupTargetTableAsync(
        Guid dataSourceId,
        PartitionSwitchInspectionContext context,
        CancellationToken cancellationToken)
    {
        await using var targetConnection = context.UseSourceAsTarget
            ? await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken)
            : await connectionFactory.CreateTargetSqlConnectionAsync(dataSourceId, cancellationToken);

        if (!context.UseSourceAsTarget && !string.IsNullOrWhiteSpace(context.TargetDatabase))
        {
            targetConnection.ChangeDatabase(context.TargetDatabase);
        }

        // 解析分区号
        if (!int.TryParse(context.SourcePartitionKey, System.Globalization.NumberStyles.Integer, 
            System.Globalization.CultureInfo.InvariantCulture, out var partitionNumber) || partitionNumber <= 0)
        {
            throw new InvalidOperationException($"无效的分区编号: {context.SourcePartitionKey}");
        }

        // 检查目标表是否为分区表
        const string checkPartitionedSql = @"
SELECT COUNT(1)
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
WHERE s.name = @Schema AND t.name = @Table AND i.index_id IN (0, 1);";

        var isPartitioned = await sqlExecutor.QuerySingleAsync<int>(
            targetConnection,
            checkPartitionedSql,
            new { Schema = context.TargetSchema, Table = context.TargetTable },
            transaction: null) > 0;

        string script;
        string message;

        if (isPartitioned)
        {
            // 目标表是分区表，使用 TRUNCATE TABLE ... WITH (PARTITIONS (...)) 清空特定分区
            script = $@"-- 清空目标表分区 {partitionNumber} 的残留数据
SET XACT_ABORT ON;
BEGIN TRANSACTION;
    TRUNCATE TABLE [{context.TargetSchema}].[{context.TargetTable}] 
    WITH (PARTITIONS ({partitionNumber}));
COMMIT TRANSACTION;";
            message = $"已清空目标表 [{context.TargetSchema}].[{context.TargetTable}] 分区 {partitionNumber} 的残留数据。";
        }
        else
        {
            // 目标表是普通表，使用 TRUNCATE TABLE 清空整个表
            script = $@"-- 清空目标表的残留数据（普通表）
SET XACT_ABORT ON;
BEGIN TRANSACTION;
    TRUNCATE TABLE [{context.TargetSchema}].[{context.TargetTable}];
COMMIT TRANSACTION;";
            message = $"已清空目标表 [{context.TargetSchema}].[{context.TargetTable}] 的所有数据（普通表）。";
        }

        // 执行清空操作
        await sqlExecutor.ExecuteAsync(targetConnection, script, timeoutSeconds: 300);

        logger.LogInformation("已清空目标表 {Schema}.{Table} 的残留数据，分区号: {PartitionNumber}, 是否分区表: {IsPartitioned}", 
            context.TargetSchema, context.TargetTable, partitionNumber, isPartitioned);

        return new AutoFixStepOutcome(
            new PartitionSwitchAutoFixExecution(
                "CleanupResidualData",
                true,
                message,
                script,
                0),
            null); // 清空数据操作不支持回滚
    }

    // 刷新统计信息，降低执行计划偏差风险
    private async Task<AutoFixStepOutcome> ExecuteRefreshStatisticsAsync(
        Guid dataSourceId,
        PartitionSwitchInspectionContext context,
        CancellationToken cancellationToken)
    {
        await using var targetConnection = context.UseSourceAsTarget
            ? await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken)
            : await connectionFactory.CreateTargetSqlConnectionAsync(dataSourceId, cancellationToken);

        if (!context.UseSourceAsTarget && !string.IsNullOrWhiteSpace(context.TargetDatabase))
        {
            targetConnection.ChangeDatabase(context.TargetDatabase);
        }

        var script = $"UPDATE STATISTICS [{context.TargetSchema}].[{context.TargetTable}];";
        await sqlExecutor.ExecuteAsync(targetConnection, script, timeoutSeconds: 120);

        return new AutoFixStepOutcome(
            new PartitionSwitchAutoFixExecution(
                "RefreshStatistics",
                true,
                "已刷新目标表统计信息。",
                script,
                0),
            null);
    }

    // 同步分区函数及分区方案
    private async Task<AutoFixStepOutcome> ExecuteSyncPartitionObjectsAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        CancellationToken cancellationToken)
    {
        var createdFunction = await partitionCommandExecutor.CreatePartitionFunctionAsync(dataSourceId, configuration, configuration.Boundaries.Select(b => b.Value).ToList(), cancellationToken);
        var createdScheme = await partitionCommandExecutor.CreatePartitionSchemeAsync(dataSourceId, configuration, cancellationToken);

        var message = createdFunction || createdScheme
            ? "已补齐分区函数/方案定义。"
            : "分区函数与分区方案已存在，未做变更。";

        var script = "-- 调用托管 API 创建/校验分区函数与方案";
        return new AutoFixStepOutcome(
            new PartitionSwitchAutoFixExecution(
                "SyncPartitionObjects",
                true,
                message,
                script,
                0),
            null);
    }

    // 同步索引定义，从源表复制索引到目标表
    private async Task<AutoFixStepOutcome> ExecuteSyncIndexesAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        PartitionSwitchInspectionContext context,
        CancellationToken cancellationToken)
    {
        await using var sourceConnection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
        if (!string.IsNullOrWhiteSpace(context.SourceDatabase))
        {
            sourceConnection.ChangeDatabase(context.SourceDatabase);
        }

        await using var targetConnection = context.UseSourceAsTarget
            ? null
            : await connectionFactory.CreateTargetSqlConnectionAsync(dataSourceId, cancellationToken);

        var connectionForTarget = targetConnection ?? sourceConnection;

        if (!context.UseSourceAsTarget && !string.IsNullOrWhiteSpace(context.TargetDatabase))
        {
            connectionForTarget.ChangeDatabase(context.TargetDatabase);
        }

        // 加载源表的所有索引(包括聚集索引和主键)
        var sourceAllIndexes = await LoadAllIndexesAsync(
            sourceConnection,
            configuration.SchemaName,
            configuration.TableName,
            cancellationToken);

        // 加载目标表的所有索引
        var targetAllIndexes = await LoadAllIndexesAsync(
            connectionForTarget,
            context.TargetSchema,
            context.TargetTable,
            cancellationToken);

        // 加载当前架构下所有索引/约束名称,用于检测命名冲突
        var schemaExistingNames = await LoadSchemaIndexAndConstraintNamesAsync(
            connectionForTarget,
            context.TargetSchema,
            cancellationToken);

        // 加载目标表分区信息(如果是分区表)
        var targetPartitionInfo = await LoadPartitionInfoAsync(
            connectionForTarget,
            context.TargetSchema,
            context.TargetTable,
            cancellationToken);

        var scriptBuilder = new StringBuilder();
        var createdIndexNames = new List<string>();
        var droppedIndexNames = new List<string>();
        var failedIndexes = new List<string>();

        // 分离聚集索引和非聚集索引
        var sourceClusteredIndex = sourceAllIndexes.FirstOrDefault(i => i.IsClustered);
        var targetClusteredIndex = targetAllIndexes.FirstOrDefault(i => i.IsClustered);
        var sourceNonClusteredIndexes = sourceAllIndexes.Where(i => !i.IsClustered).ToList();
        var targetNonClusteredIndexes = targetAllIndexes.Where(i => !i.IsClustered).ToList();

        // 处理聚集索引: 表只能有一个聚集索引,按键列和定义对比,不按名称对比
        if (sourceClusteredIndex != null)
        {
            if (targetClusteredIndex != null)
            {
                // 目标表已有聚集索引,检查键列是否一致
                if (!IsIndexDefinitionMatching(sourceClusteredIndex, targetClusteredIndex))
                {
                    // 键列或定义不一致,需要删除重建
                    logger.LogInformation("目标表聚集索引 {IndexName} 定义不一致,需要删除重建。键列: 源=[{SourceKeys}], 目标=[{TargetKeys}]", 
                        targetClusteredIndex.IndexName, sourceClusteredIndex.KeyColumns, targetClusteredIndex.KeyColumns);
                    
                    var dropScript = BuildDropIndexScript(context.TargetSchema, context.TargetTable, targetClusteredIndex);
                    scriptBuilder.AppendLine($"-- 删除不一致的聚集索引 [{targetClusteredIndex.IndexName}]");
                    scriptBuilder.AppendLine(dropScript);
                    scriptBuilder.AppendLine("GO");
                    scriptBuilder.AppendLine();

                    var targetClusteredIndexName = targetClusteredIndex.IndexName;
                    
                    try
                    {
                        await sqlExecutor.ExecuteAsync(connectionForTarget, dropScript, timeoutSeconds: 300);
                        droppedIndexNames.Add(targetClusteredIndexName);
                        schemaExistingNames.Remove(targetClusteredIndexName);
                        logger.LogInformation("已删除聚集索引 {IndexName}", targetClusteredIndexName);
                        
                        // 删除成功后,需要重建
                        targetClusteredIndex = null;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "删除聚集索引 {IndexName} 失败", targetClusteredIndexName);
                        failedIndexes.Add($"删除聚集索引 {targetClusteredIndexName} 失败: {ex.Message}");
                    }
                }
                else
                {
                    // 聚集索引定义一致,跳过
                    logger.LogInformation("聚集索引 {IndexName} 定义一致,跳过。", targetClusteredIndex.IndexName);
                    scriptBuilder.AppendLine($"-- 聚集索引 [{targetClusteredIndex.IndexName}] 定义一致,跳过");
                }
            }

            // 如果目标表没有聚集索引(或已被删除),创建源表的聚集索引
            if (targetClusteredIndex == null)
            {
                var effectiveName = sourceClusteredIndex.IndexName;
                if (schemaExistingNames.Contains(effectiveName))
                {
                    effectiveName = GenerateUniqueIndexName(effectiveName, context.TargetTable, schemaExistingNames);
                }

                schemaExistingNames.Add(effectiveName);

                var indexForSync = new IndexDefinitionForSync
                {
                    IndexId = sourceClusteredIndex.IndexId,
                    IndexName = sourceClusteredIndex.IndexName,
                    IsUnique = sourceClusteredIndex.IsUnique,
                    IsPrimaryKey = sourceClusteredIndex.IsPrimaryKey,
                    IndexType = (byte)1, // 聚集
                    KeyColumns = sourceClusteredIndex.KeyColumns,
                    IncludedColumns = sourceClusteredIndex.IncludedColumns,
                    FilterDefinition = sourceClusteredIndex.FilterDefinition
                };

                var indexScript = BuildCreateIndexScript(context.TargetSchema, context.TargetTable, indexForSync, effectiveName, targetPartitionInfo);
                scriptBuilder.AppendLine($"-- 创建聚集索引 [{effectiveName}]");
                scriptBuilder.AppendLine(indexScript);
                scriptBuilder.AppendLine("GO");
                scriptBuilder.AppendLine();

                try
                {
                    await sqlExecutor.ExecuteAsync(connectionForTarget, indexScript, timeoutSeconds: 300);
                    createdIndexNames.Add(effectiveName);
                    logger.LogInformation("已创建聚集索引 {IndexName}", effectiveName);
                }
                catch (Exception ex)
                {
                    var errorDetail = $"聚集索引 {sourceClusteredIndex.IndexName} 创建失败: {ex.Message}";
                    failedIndexes.Add(errorDetail);
                    scriptBuilder.AppendLine($"-- 失败: {errorDetail}");
                    logger.LogError(ex, "创建聚集索引 {IndexName} 失败", sourceClusteredIndex.IndexName);
                }
            }
        }

        // 处理非聚集索引: 按名称匹配,检查定义是否一致
        foreach (var targetIndex in targetNonClusteredIndexes)
        {
            var sourceIndex = sourceNonClusteredIndexes.FirstOrDefault(s => 
                s.IndexName.Equals(targetIndex.IndexName, StringComparison.OrdinalIgnoreCase));

            if (sourceIndex == null)
            {
                // 目标表有但源表没有的索引,保留
                logger.LogInformation("目标表非聚集索引 {IndexName} 在源表中不存在,保留。", targetIndex.IndexName);
                scriptBuilder.AppendLine($"-- 目标表索引 [{targetIndex.IndexName}] 在源表中不存在,保留");
                continue;
            }

            // 检查索引定义是否一致
            if (!IsIndexDefinitionMatching(sourceIndex, targetIndex))
            {
                // 索引不一致,需要删除重建
                logger.LogInformation("目标表索引 {IndexName} 定义不一致,需要删除重建。", targetIndex.IndexName);
                
                var dropScript = BuildDropIndexScript(context.TargetSchema, context.TargetTable, targetIndex);
                scriptBuilder.AppendLine($"-- 删除不一致的索引 [{targetIndex.IndexName}]");
                scriptBuilder.AppendLine(dropScript);
                scriptBuilder.AppendLine("GO");
                scriptBuilder.AppendLine();

                try
                {
                    await sqlExecutor.ExecuteAsync(connectionForTarget, dropScript, timeoutSeconds: 300);
                    droppedIndexNames.Add(targetIndex.IndexName);
                    schemaExistingNames.Remove(targetIndex.IndexName);
                    logger.LogInformation("已删除索引 {IndexName}", targetIndex.IndexName);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "删除索引 {IndexName} 失败", targetIndex.IndexName);
                    failedIndexes.Add($"删除索引 {targetIndex.IndexName} 失败: {ex.Message}");
                    // 删除失败,不能重建,标记为已存在
                    continue;
                }
            }
            else
            {
                // 索引定义一致,跳过
                logger.LogInformation("索引 {IndexName} 定义一致,跳过。", targetIndex.IndexName);
                scriptBuilder.AppendLine($"-- 索引 [{targetIndex.IndexName}] 定义一致,跳过");
            }
        }

        // 创建源表中存在但目标表中不存在(或已被删除)的非聚集索引
        foreach (var sourceIndex in sourceNonClusteredIndexes)
        {
            var targetIndex = targetNonClusteredIndexes.FirstOrDefault(t => 
                t.IndexName.Equals(sourceIndex.IndexName, StringComparison.OrdinalIgnoreCase));

            // 如果目标表有这个索引且定义一致,已经在上面跳过了
            if (targetIndex != null && !droppedIndexNames.Contains(targetIndex.IndexName))
            {
                continue;
            }

            // 特殊检查:如果源索引是主键,且目标表已经有主键(不管是聚集还是非聚集),则跳过
            // SQL Server 规则:每个表只能有一个主键
            if (sourceIndex.IsPrimaryKey)
            {
                var targetHasPrimaryKey = targetAllIndexes.Any(t => t.IsPrimaryKey && !droppedIndexNames.Contains(t.IndexName));
                if (targetHasPrimaryKey)
                {
                    logger.LogWarning("目标表已有主键,跳过创建源表的非聚集主键 {IndexName}", sourceIndex.IndexName);
                    scriptBuilder.AppendLine($"-- 跳过: 目标表已有主键,不创建非聚集主键 [{sourceIndex.IndexName}]");
                    failedIndexes.Add($"⚠️ 跳过主键 {sourceIndex.IndexName}: 目标表已有主键(类型可能不同)");
                    continue;
                }
            }

            // 需要创建的索引
            var effectiveName = sourceIndex.IndexName;
            if (schemaExistingNames.Contains(effectiveName))
            {
                effectiveName = GenerateUniqueIndexName(effectiveName, context.TargetTable, schemaExistingNames);
            }

            schemaExistingNames.Add(effectiveName);

            var indexForSync = new IndexDefinitionForSync
            {
                IndexId = sourceIndex.IndexId,
                IndexName = sourceIndex.IndexName,
                IsUnique = sourceIndex.IsUnique,
                IsPrimaryKey = sourceIndex.IsPrimaryKey,
                IndexType = (byte)2, // 非聚集
                KeyColumns = sourceIndex.KeyColumns,
                IncludedColumns = sourceIndex.IncludedColumns,
                FilterDefinition = sourceIndex.FilterDefinition
            };

            var indexScript = BuildCreateIndexScript(context.TargetSchema, context.TargetTable, indexForSync, effectiveName, targetPartitionInfo);
            scriptBuilder.AppendLine($"-- 创建索引 [{effectiveName}]");
            scriptBuilder.AppendLine(indexScript);
            scriptBuilder.AppendLine("GO");
            scriptBuilder.AppendLine();

            try
            {
                await sqlExecutor.ExecuteAsync(connectionForTarget, indexScript, timeoutSeconds: 300);
                createdIndexNames.Add(effectiveName);
                logger.LogInformation("已创建索引 {IndexName}", effectiveName);
            }
            catch (Exception ex)
            {
                var errorDetail = sourceIndex.IsUnique 
                    ? $"唯一索引 {sourceIndex.IndexName} 创建失败(可能存在重复数据): {ex.Message}"
                    : $"索引 {sourceIndex.IndexName} 创建失败: {ex.Message}";
                
                failedIndexes.Add(errorDetail);
                scriptBuilder.AppendLine($"-- 失败: {errorDetail}");
                
                logger.LogError(ex, "创建索引 {IndexName} 失败。{Details}", 
                    sourceIndex.IndexName, 
                    sourceIndex.IsUnique ? "这是唯一索引,可能存在重复数据导致无法创建。" : "");
            }
        }

        var script = scriptBuilder.ToString();
        var totalCreated = createdIndexNames.Count;
        var totalDropped = droppedIndexNames.Count;
        
        string message;
        if (totalCreated > 0 || totalDropped > 0)
        {
            var parts = new List<string>();
            if (totalDropped > 0)
            {
                parts.Add($"删除 {totalDropped} 个不一致的索引");
            }
            if (totalCreated > 0)
            {
                parts.Add($"创建 {totalCreated} 个索引");
            }
            
            var successMessage = $"已同步索引: {string.Join(", ", parts)}。";
            
            if (failedIndexes.Count > 0)
            {
                message = $"{successMessage}\n⚠️ {failedIndexes.Count} 个操作失败:\n• {string.Join("\n• ", failedIndexes)}";
            }
            else
            {
                message = successMessage;
            }
        }
        else
        {
            if (failedIndexes.Count > 0)
            {
                message = $"❌ 所有操作均失败:\n• {string.Join("\n• ", failedIndexes)}";
            }
            else
            {
                message = "所有索引均已一致,无需同步。";
            }
        }

        return new AutoFixStepOutcome(
            new PartitionSwitchAutoFixExecution("SyncIndexes", (totalCreated > 0 || totalDropped > 0) && failedIndexes.Count == 0, message, script, 0),
            new AutoFixRollbackAction(async ct => await RollbackSyncIndexesAsync(dataSourceId, context, createdIndexNames, ct)));
    }

    // 同步约束定义，从源表复制 CHECK/DEFAULT 约束到目标表
    private async Task<AutoFixStepOutcome> ExecuteSyncConstraintsAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        PartitionSwitchInspectionContext context,
        CancellationToken cancellationToken)
    {
        await using var sourceConnection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
        if (!string.IsNullOrWhiteSpace(context.SourceDatabase))
        {
            sourceConnection.ChangeDatabase(context.SourceDatabase);
        }

        await using var targetConnection = context.UseSourceAsTarget
            ? null
            : await connectionFactory.CreateTargetSqlConnectionAsync(dataSourceId, cancellationToken);

        var connectionForTarget = targetConnection ?? sourceConnection;

        if (!context.UseSourceAsTarget && !string.IsNullOrWhiteSpace(context.TargetDatabase))
        {
            connectionForTarget.ChangeDatabase(context.TargetDatabase);
        }

        var sourceDefaults = await LoadDefaultConstraintsForSyncAsync(
            sourceConnection,
            context.SourceDatabase,
            configuration.SchemaName,
            configuration.TableName,
            cancellationToken);

        var sourceChecks = await LoadCheckConstraintsForSyncAsync(
            sourceConnection,
            context.SourceDatabase,
            configuration.SchemaName,
            configuration.TableName,
            cancellationToken);

        if (sourceDefaults.Count == 0 && sourceChecks.Count == 0)
        {
            return new AutoFixStepOutcome(
                new PartitionSwitchAutoFixExecution(
                    "SyncConstraints",
                    true,
                    "源表无需同步的约束，跳过。",
                    "-- No constraints to sync",
                    0),
                null);
        }

        var scriptBuilder = new StringBuilder();
        var createdConstraints = new List<string>();

        // 同步 DEFAULT 约束
        foreach (var constraint in sourceDefaults)
        {
            var constraintScript = BuildCreateDefaultConstraintScript(
                context.TargetSchema,
                context.TargetTable,
                constraint.ColumnName,
                constraint.Definition);

            scriptBuilder.AppendLine(constraintScript);
            scriptBuilder.AppendLine("GO");

            try
            {
                await sqlExecutor.ExecuteAsync(connectionForTarget, constraintScript, timeoutSeconds: 60);
                createdConstraints.Add($"DEFAULT on {constraint.ColumnName}");
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "创建 DEFAULT 约束失败，列 {ColumnName}。", constraint.ColumnName);
            }
        }

        // 同步 CHECK 约束
        foreach (var constraint in sourceChecks)
        {
            var constraintScript = BuildCreateCheckConstraintScript(
                context.TargetSchema,
                context.TargetTable,
                constraint.ConstraintName,
                constraint.Definition);

            scriptBuilder.AppendLine(constraintScript);
            scriptBuilder.AppendLine("GO");

            try
            {
                await sqlExecutor.ExecuteAsync(connectionForTarget, constraintScript, timeoutSeconds: 60);
                createdConstraints.Add($"CHECK {constraint.ConstraintName}");
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "创建 CHECK 约束失败，{ConstraintName}。", constraint.ConstraintName);
            }
        }

        var script = scriptBuilder.ToString();
        var message = createdConstraints.Count > 0
            ? $"已同步 {createdConstraints.Count} 个约束：{string.Join(", ", createdConstraints)}。"
            : "约束同步未成功创建任何约束。";

        return new AutoFixStepOutcome(
            new PartitionSwitchAutoFixExecution("SyncConstraints", createdConstraints.Count > 0, message, script, 0),
            new AutoFixRollbackAction(async ct => await RollbackSyncConstraintsAsync(dataSourceId, context, sourceDefaults, sourceChecks, ct)));
    }

    private async Task<IReadOnlyList<ColumnMetadata>> LoadColumnMetadataAsync(
        SqlConnection connection,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    c.column_id AS ColumnId,
    c.name AS ColumnName,
    t.name AS DataType,
    c.max_length AS MaxLength,
    c.precision AS Precision,
    c.scale AS Scale,
    c.is_nullable AS IsNullable,
    c.is_identity AS IsIdentity,
    c.is_computed AS IsComputed,
    ic.seed_value AS IdentitySeed,
    ic.increment_value AS IdentityIncrement,
    cc.definition AS ComputedDefinition,
    cc.is_persisted AS IsPersisted,
    c.collation_name AS CollationName,
    c.is_rowguidcol AS IsRowGuid
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id AND c.system_type_id = t.system_type_id
LEFT JOIN sys.identity_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
WHERE c.object_id = OBJECT_ID(@FullName)
ORDER BY c.column_id;";

        var fullName = $"[{schema}].[{table}]";
        var rows = await sqlExecutor.QueryAsync<ColumnMetadata>(connection, sql, new { FullName = fullName });
        cancellationToken.ThrowIfCancellationRequested();
        return rows.ToList();
    }

    /// <summary>
    /// 加载源表的分区信息(分区方案、分区列)
    /// </summary>
    private async Task<PartitionInfo?> LoadPartitionInfoAsync(
        SqlConnection connection,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT 
    ps.name AS PartitionSchemeName,
    c.name AS ColumnName,
    pf.name AS PartitionFunctionName
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0,1)
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND ic.partition_ordinal = 1
INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE s.name = @SchemaName AND t.name = @TableName;";

        var result = await sqlExecutor.QueryAsync<PartitionInfo>(connection, sql, new { SchemaName = schema, TableName = table });
        return result.FirstOrDefault();
    }

    private static string BuildCreateTableScript(
        string schema, 
        string table, 
        IReadOnlyList<ColumnMetadata> columns, 
        PartitionInfo? partitionInfo = null)
    {
        if (columns.Count == 0)
        {
            throw new InvalidOperationException("未能获取源表列信息，无法创建目标表。");
        }

        var builder = new StringBuilder();
        builder.AppendLine($"IF OBJECT_ID('[{schema}].[{table}]', 'U') IS NULL");
        builder.AppendLine("BEGIN");
        builder.AppendLine($"    CREATE TABLE [{schema}].[{table}] (");

        for (var i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            var columnSql = column.IsComputed
                ? BuildComputedColumnDefinition(column)
                : BuildRegularColumnDefinition(column);

            var suffix = i == columns.Count - 1 ? string.Empty : ",";
            builder.AppendLine($"        {columnSql}{suffix}");
        }

        builder.Append("    )");
        
        // 如果源表是分区表,目标表也创建为分区表(使用相同分区方案和分区列)
        if (partitionInfo != null)
        {
            builder.AppendLine();
            builder.Append($"    ON [{partitionInfo.PartitionSchemeName}]([{partitionInfo.ColumnName}])");
        }
        
        builder.AppendLine(";");
        builder.AppendLine("END");

        return builder.ToString();
    }

    private static string BuildRegularColumnDefinition(ColumnMetadata column)
    {
        var builder = new StringBuilder();
        builder.Append($"[{column.ColumnName}] {BuildDataType(column)}");

        if (column.IsIdentity)
        {
            var seed = column.IdentitySeed ?? 1;
            var increment = column.IdentityIncrement ?? 1;
            builder.Append($" IDENTITY({seed},{increment})");
        }

        builder.Append(column.IsNullable ? " NULL" : " NOT NULL");

        if (column.IsRowGuid)
        {
            builder.Append(" ROWGUIDCOL");
        }

        return builder.ToString();
    }

    private static string BuildComputedColumnDefinition(ColumnMetadata column)
    {
        if (string.IsNullOrWhiteSpace(column.ComputedDefinition))
        {
            throw new InvalidOperationException($"列 {column.ColumnName} 标记为计算列但缺少定义。");
        }

        var builder = new StringBuilder();
        builder.Append($"[{column.ColumnName}] AS {column.ComputedDefinition}");
        if (column.IsPersisted)
        {
            builder.Append(" PERSISTED");
        }

        return builder.ToString();
    }

    private static string BuildDataType(ColumnMetadata column)
    {
        var dataType = column.DataType.ToLowerInvariant();
        return dataType switch
        {
            "nvarchar" or "nchar" => BuildLengthType(column, divisor: 2),
            "varchar" or "char" or "varbinary" or "binary" => BuildLengthType(column),
            "decimal" or "numeric" => $"{column.DataType.ToUpperInvariant()}({column.Precision},{column.Scale})",
            "datetime2" or "datetimeoffset" or "time" => column.Scale > 0
                ? $"{column.DataType.ToUpperInvariant()}({column.Scale})"
                : column.DataType.ToUpperInvariant(),
            "float" or "real" or "bigint" or "int" or "smallint" or "tinyint" or "bit" or "datetime" or "date" or "smalldatetime" or "money" or "smallmoney" or "uniqueidentifier" or "image" or "text" or "ntext"
                => column.DataType.ToUpperInvariant(),
            _ => column.DataType.ToUpperInvariant()
        } + BuildCollationSuffix(column);
    }

    private static string BuildLengthType(ColumnMetadata column, int divisor = 1)
    {
        var length = (int)column.MaxLength;
        if (length < 0)
        {
            return $"{column.DataType.ToUpperInvariant()}(MAX)";
        }

        if (divisor > 1)
        {
            length /= divisor;
        }

        var actual = length == 0 ? 1 : length;
        return $"{column.DataType.ToUpperInvariant()}({actual})";
    }

    private static string BuildCollationSuffix(ColumnMetadata column)
    {
        if (string.IsNullOrWhiteSpace(column.CollationName))
        {
            return string.Empty;
        }

        var dataType = column.DataType.ToLowerInvariant();
        if (dataType.Contains("char") || dataType.Contains("text"))
        {
            return $" COLLATE {column.CollationName}";
        }

        return string.Empty;
    }

    private sealed class ColumnMetadata
    {
        public int ColumnId { get; set; }
        public string ColumnName { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public short MaxLength { get; set; }
        public byte Precision { get; set; }
        public byte Scale { get; set; }
        public bool IsNullable { get; set; }
        public bool IsIdentity { get; set; }
        public bool IsComputed { get; set; }
        public int? IdentitySeed { get; set; }
        public int? IdentityIncrement { get; set; }
        public string? ComputedDefinition { get; set; }
        public bool IsPersisted { get; set; }
        public string? CollationName { get; set; }
        public bool IsRowGuid { get; set; }
    }

    /// <summary>
    /// 分区表信息
    /// </summary>
    private sealed class PartitionInfo
    {
        /// <summary>分区方案名称</summary>
        public string PartitionSchemeName { get; set; } = string.Empty;
        
        /// <summary>分区列名称</summary>
        public string ColumnName { get; set; } = string.Empty;
        
        /// <summary>分区函数名称</summary>
        public string PartitionFunctionName { get; set; } = string.Empty;
    }

    private sealed record AutoFixStepOutcome(
        PartitionSwitchAutoFixExecution Execution,
        AutoFixRollbackAction? RollbackAction);

    private sealed record AutoFixRollbackAction(
        Func<CancellationToken, Task<PartitionSwitchAutoFixExecution>> Handler);

    private async Task<IReadOnlyList<PartitionSwitchAutoFixExecution>> TryRollbackAsync(
        PartitionSwitchInspectionContext context,
        Stack<AutoFixRollbackAction> rollbackStack,
        CancellationToken cancellationToken)
    {
        if (rollbackStack.Count == 0)
        {
            return Array.Empty<PartitionSwitchAutoFixExecution>();
        }

        var results = new List<PartitionSwitchAutoFixExecution>(rollbackStack.Count);

        while (rollbackStack.TryPop(out var action))
        {
            try
            {
                var execution = await action.Handler(cancellationToken);
                results.Add(execution);
            }
            catch (Exception rollbackEx)
            {
                logger.LogError(rollbackEx, "自动补齐回滚操作执行失败。目标：{Schema}.{Table}", context.TargetSchema, context.TargetTable);
                results.Add(new PartitionSwitchAutoFixExecution(
                    "RollbackFailure",
                    false,
                    rollbackEx.Message,
                    "-- 回滚过程中出现异常，请人工确认数据库状态。",
                    0));
            }
        }

        return results;
    }

    private async Task<PartitionSwitchAutoFixExecution> RollbackCreateTargetTableAsync(
        Guid dataSourceId,
        PartitionSwitchInspectionContext context,
        CancellationToken cancellationToken)
    {
        await using var targetConnection = context.UseSourceAsTarget
            ? await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken)
            : await connectionFactory.CreateTargetSqlConnectionAsync(dataSourceId, cancellationToken);

        if (!context.UseSourceAsTarget && !string.IsNullOrWhiteSpace(context.TargetDatabase))
        {
            targetConnection.ChangeDatabase(context.TargetDatabase);
        }

        const string existsSql = @"SELECT COUNT(1) FROM sys.tables t INNER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = @SchemaName AND t.name = @TableName";
        var exists = await sqlExecutor.QuerySingleAsync<int>(
            targetConnection,
            existsSql,
            new { SchemaName = context.TargetSchema, TableName = context.TargetTable });

        if (exists == 0)
        {
            return new PartitionSwitchAutoFixExecution(
                "RollbackCreateTargetTable",
                true,
                $"目标表 [{context.TargetSchema}].[{context.TargetTable}] 不存在，无需回滚。",
                $"-- Skip drop table for [{context.TargetSchema}].[{context.TargetTable}]",
                0);
        }

        var script = $"DROP TABLE [{context.TargetSchema}].[{context.TargetTable}];";
        await sqlExecutor.ExecuteAsync(targetConnection, script, timeoutSeconds: 60);

        return new PartitionSwitchAutoFixExecution(
            "RollbackCreateTargetTable",
            true,
            $"已回滚目标表创建，删除 [{context.TargetSchema}].[{context.TargetTable}]。",
            script,
            0);
    }

    private async Task<PartitionSwitchAutoFixExecution> RollbackSyncIndexesAsync(
        Guid dataSourceId,
        PartitionSwitchInspectionContext context,
        IReadOnlyList<string> createdIndexNames,
        CancellationToken cancellationToken)
    {
        if (createdIndexNames.Count == 0)
        {
            return new PartitionSwitchAutoFixExecution(
                "RollbackSyncIndexes",
                true,
                "无需回滚索引，未创建任何索引。",
                "-- No indexes to rollback",
                0);
        }

        await using var targetConnection = context.UseSourceAsTarget
            ? await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken)
            : await connectionFactory.CreateTargetSqlConnectionAsync(dataSourceId, cancellationToken);

        if (!context.UseSourceAsTarget && !string.IsNullOrWhiteSpace(context.TargetDatabase))
        {
            targetConnection.ChangeDatabase(context.TargetDatabase);
        }

        var scriptBuilder = new StringBuilder();
        var droppedCount = 0;

        foreach (var indexName in createdIndexNames)
        {
            var dropScript = $"DROP INDEX IF EXISTS [{indexName}] ON [{context.TargetSchema}].[{context.TargetTable}];";
            scriptBuilder.AppendLine(dropScript);

            try
            {
                await sqlExecutor.ExecuteAsync(targetConnection, dropScript, timeoutSeconds: 60);
                droppedCount++;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "回滚删除索引 {IndexName} 失败。", indexName);
            }
        }

        return new PartitionSwitchAutoFixExecution(
            "RollbackSyncIndexes",
            droppedCount > 0,
            $"已回滚索引同步，删除 {droppedCount}/{createdIndexNames.Count} 个索引。",
            scriptBuilder.ToString(),
            0);
    }

    private async Task<PartitionSwitchAutoFixExecution> RollbackSyncConstraintsAsync(
        Guid dataSourceId,
        PartitionSwitchInspectionContext context,
        IReadOnlyList<ConstraintForSync> sourceDefaults,
        IReadOnlyList<ConstraintForSync> sourceChecks,
        CancellationToken cancellationToken)
    {
        var totalCount = sourceDefaults.Count + sourceChecks.Count;
        if (totalCount == 0)
        {
            return new PartitionSwitchAutoFixExecution(
                "RollbackSyncConstraints",
                true,
                "无需回滚约束，未创建任何约束。",
                "-- No constraints to rollback",
                0);
        }

        await using var targetConnection = context.UseSourceAsTarget
            ? await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken)
            : await connectionFactory.CreateTargetSqlConnectionAsync(dataSourceId, cancellationToken);

        if (!context.UseSourceAsTarget && !string.IsNullOrWhiteSpace(context.TargetDatabase))
        {
            targetConnection.ChangeDatabase(context.TargetDatabase);
        }

        var scriptBuilder = new StringBuilder();
        var droppedCount = 0;

        // 回滚 DEFAULT 约束
        foreach (var constraint in sourceDefaults)
        {
            var findConstraintSql = $@"
SELECT dc.name
FROM sys.default_constraints dc
INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE dc.parent_object_id = OBJECT_ID('[{context.TargetSchema}].[{context.TargetTable}]')
  AND c.name = @ColumnName";

            try
            {
                var constraintName = await sqlExecutor.QuerySingleAsync<string>(
                    targetConnection,
                    findConstraintSql,
                    new { ColumnName = constraint.ColumnName });

                if (!string.IsNullOrWhiteSpace(constraintName))
                {
                    var dropScript = $"ALTER TABLE [{context.TargetSchema}].[{context.TargetTable}] DROP CONSTRAINT [{constraintName}];";
                    scriptBuilder.AppendLine(dropScript);
                    await sqlExecutor.ExecuteAsync(targetConnection, dropScript, timeoutSeconds: 60);
                    droppedCount++;
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "回滚删除 DEFAULT 约束失败，列 {ColumnName}。", constraint.ColumnName);
            }
        }

        // 回滚 CHECK 约束
        foreach (var constraint in sourceChecks)
        {
            var dropScript = $"ALTER TABLE [{context.TargetSchema}].[{context.TargetTable}] DROP CONSTRAINT IF EXISTS [{constraint.ConstraintName}];";
            scriptBuilder.AppendLine(dropScript);

            try
            {
                await sqlExecutor.ExecuteAsync(targetConnection, dropScript, timeoutSeconds: 60);
                droppedCount++;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "回滚删除 CHECK 约束失败，{ConstraintName}。", constraint.ConstraintName);
            }
        }

        return new PartitionSwitchAutoFixExecution(
            "RollbackSyncConstraints",
            droppedCount > 0,
            $"已回滚约束同步，删除 {droppedCount}/{totalCount} 个约束。",
            scriptBuilder.ToString(),
            0);
    }

    private async Task<IReadOnlyList<IndexDefinitionForSync>> LoadNonClusteredIndexesAsync(
        SqlConnection connection,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    i.index_id AS IndexId,
    i.name AS IndexName,
    i.is_unique AS IsUnique,
    STUFF((
        SELECT ', ' + '[' + c.name + ']' + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), 1, 2, '') AS KeyColumns,
    STUFF((
        SELECT ', ' + '[' + c.name + ']'
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
  AND i.type = 2
  AND i.is_primary_key = 0
  AND i.is_unique_constraint = 0
  AND i.is_disabled = 0
ORDER BY i.index_id;";

        var result = await sqlExecutor.QueryAsync<IndexDefinitionForSync>(
            connection,
            sql,
            new { SchemaName = schema, TableName = table });

        cancellationToken.ThrowIfCancellationRequested();
        return result.ToList();
    }

    /// <summary>
    /// 加载聚集索引和主键(必须优先创建,其他唯一索引依赖它们)
    /// </summary>
    private async Task<IReadOnlyList<IndexDefinitionForSync>> LoadClusteredAndPrimaryKeyIndexesAsync(
        SqlConnection connection,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    i.index_id AS IndexId,
    i.name AS IndexName,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    i.type AS IndexType,
    STUFF((
        SELECT ', ' + '[' + c.name + ']' + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), 1, 2, '') AS KeyColumns,
    STUFF((
        SELECT ', ' + '[' + c.name + ']'
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
  AND (i.type = 1 OR i.is_primary_key = 1)
  AND i.is_disabled = 0
ORDER BY i.is_primary_key DESC, i.index_id;";

        var result = await sqlExecutor.QueryAsync<IndexDefinitionForSync>(
            connection,
            sql,
            new { SchemaName = schema, TableName = table });

        cancellationToken.ThrowIfCancellationRequested();
        return result.ToList();
    }

    /// <summary>
    /// 加载目标表已存在的索引和约束名称列表
    /// </summary>
    private async Task<IReadOnlyList<string>> LoadExistingIndexNamesAsync(
        SqlConnection connection,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string sql = @"
-- 加载索引名称
SELECT i.name
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName
  AND t.name = @TableName
  AND i.name IS NOT NULL

UNION

-- 加载主键和唯一约束名称
SELECT kc.name
FROM sys.key_constraints kc
INNER JOIN sys.tables t ON kc.parent_object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName
  AND t.name = @TableName
  AND kc.name IS NOT NULL;";

        var result = await sqlExecutor.QueryAsync<string>(
            connection,
            sql,
            new { SchemaName = schema, TableName = table });

        cancellationToken.ThrowIfCancellationRequested();
        return result.ToList();
    }

    /// <summary>
    /// 检查表是否已存在主键
    /// </summary>
    private async Task<bool> CheckIfPrimaryKeyExistsAsync(
        SqlConnection connection,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COUNT(1)
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName
  AND t.name = @TableName
  AND i.is_primary_key = 1;";

        var count = await sqlExecutor.QuerySingleAsync<int>(
            connection,
            sql,
            new { SchemaName = schema, TableName = table });

        cancellationToken.ThrowIfCancellationRequested();
        return count > 0;
    }

    private async Task<HashSet<string>> LoadSchemaIndexAndConstraintNamesAsync(
        SqlConnection connection,
        string schema,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT i.name
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName
  AND i.name IS NOT NULL

UNION

SELECT kc.name
FROM sys.key_constraints kc
INNER JOIN sys.tables t ON kc.parent_object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName
  AND kc.name IS NOT NULL;";

        var names = await sqlExecutor.QueryAsync<string>(
            connection,
            sql,
            new { SchemaName = schema });

        cancellationToken.ThrowIfCancellationRequested();
        return new HashSet<string>(names, StringComparer.OrdinalIgnoreCase);
    }

    private static string GenerateUniqueIndexName(
        string desiredName,
        string targetTable,
        ISet<string> existingNames)
    {
        if (!existingNames.Contains(desiredName))
        {
            return desiredName;
        }

        var baseName = $"{desiredName}_{targetTable}";
        var candidate = baseName;
        var counter = 1;

        while (existingNames.Contains(candidate))
        {
            candidate = $"{baseName}_{counter}";
            counter++;
        }

        return candidate;
    }

    private async Task<IReadOnlyList<ConstraintForSync>> LoadDefaultConstraintsForSyncAsync(
        SqlConnection connection,
        string database,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    c.name AS ColumnName,
    dc.name AS ConstraintName,
    dc.definition AS Definition
FROM sys.default_constraints dc
INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE dc.parent_object_id = OBJECT_ID(@FullName)
  AND dc.is_ms_shipped = 0;";

        var fullName = string.IsNullOrWhiteSpace(database)
            ? $"[{schema}].[{table}]"
            : $"[{database}].[{schema}].[{table}]";

        var result = await sqlExecutor.QueryAsync<ConstraintForSync>(
            connection,
            sql,
            new { FullName = fullName });

        cancellationToken.ThrowIfCancellationRequested();
        return result.ToList();
    }

    private async Task<IReadOnlyList<ConstraintForSync>> LoadCheckConstraintsForSyncAsync(
        SqlConnection connection,
        string database,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    cc.name AS ConstraintName,
    cc.definition AS Definition,
    CASE WHEN cc.parent_column_id = 0 THEN NULL ELSE c.name END AS ColumnName
FROM sys.check_constraints cc
LEFT JOIN sys.columns c ON cc.parent_object_id = c.object_id AND cc.parent_column_id = c.column_id
WHERE cc.parent_object_id = OBJECT_ID(@FullName)
  AND cc.is_ms_shipped = 0
  AND cc.is_disabled = 0;";

        var fullName = string.IsNullOrWhiteSpace(database)
            ? $"[{schema}].[{table}]"
            : $"[{database}].[{schema}].[{table}]";

        var result = await sqlExecutor.QueryAsync<ConstraintForSync>(
            connection,
            sql,
            new { FullName = fullName });

        cancellationToken.ThrowIfCancellationRequested();
        return result.ToList();
    }

    private static string BuildCreateIndexScript(string schema, string table, IndexDefinitionForSync index, string indexName, PartitionInfo? partitionInfo = null)
    {
        var builder = new StringBuilder();
        
        // 处理主键约束
        if (index.IsPrimaryKey)
        {
            builder.Append($"ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT [{indexName}] PRIMARY KEY");
            builder.Append(index.IndexType == 1 ? " CLUSTERED (" : " NONCLUSTERED (");
            builder.Append(index.KeyColumns);
            builder.Append(");");
            return builder.ToString();
        }
        
        // 处理聚集索引或非聚集索引
        var clusteredKeyword = index.IndexType == 1 ? "CLUSTERED" : "NONCLUSTERED";
        var uniqueKeyword = index.IsUnique ? "UNIQUE " : string.Empty;

        builder.Append($"CREATE {uniqueKeyword}{clusteredKeyword} INDEX [{indexName}] ON [{schema}].[{table}] (");
        
        var keyColumns = index.KeyColumns;
        
        // 检查键列是否已包含分区列
        bool containsPartitionColumn = false;
        if (partitionInfo != null && !string.IsNullOrWhiteSpace(partitionInfo.ColumnName))
        {
            // 解析列名:去掉 [ ] 和排序关键字(ASC/DESC)
            var columnList = keyColumns.Split(',')
                .Select(c => {
                    var trimmed = c.Trim();
                    // 去掉方括号
                    trimmed = trimmed.Replace("[", "").Replace("]", "");
                    // 去掉 ASC/DESC
                    trimmed = trimmed.Replace(" ASC", "").Replace(" DESC", "").Trim();
                    return trimmed;
                })
                .ToList();
            
            containsPartitionColumn = columnList.Contains(partitionInfo.ColumnName, StringComparer.OrdinalIgnoreCase);
        }
        
        // 对于分区表,所有索引都必须包含分区列才能对齐到分区方案
        // 这对于 SWITCH PARTITION 操作是必需的
        if (partitionInfo != null && !containsPartitionColumn && !string.IsNullOrWhiteSpace(partitionInfo.ColumnName))
        {
            // 添加分区列到键列末尾
            keyColumns = $"{keyColumns}, [{partitionInfo.ColumnName}] ASC";
            containsPartitionColumn = true;
        }
        
        builder.Append(keyColumns);
        builder.Append(")");

        if (!string.IsNullOrWhiteSpace(index.IncludedColumns))
        {
            builder.Append($" INCLUDE ({index.IncludedColumns})");
        }

        if (!string.IsNullOrWhiteSpace(index.FilterDefinition))
        {
            builder.Append($" WHERE {index.FilterDefinition}");
        }

        // 所有索引都必须对齐到分区方案(因为已经添加了分区列)
        if (partitionInfo != null && !string.IsNullOrWhiteSpace(partitionInfo.PartitionSchemeName) && !string.IsNullOrWhiteSpace(partitionInfo.ColumnName))
        {
            builder.Append($" ON [{partitionInfo.PartitionSchemeName}]([{partitionInfo.ColumnName}])");
        }

        builder.Append(";");
        return builder.ToString();
    }

    private static string BuildCreateDefaultConstraintScript(string schema, string table, string columnName, string definition)
    {
        var constraintName = $"DF_{table}_{columnName}";
        return $"ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT [{constraintName}] DEFAULT {definition} FOR [{columnName}];";
    }

    private static string BuildCreateCheckConstraintScript(string schema, string table, string constraintName, string definition)
    {
        return $"ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT [{constraintName}] CHECK {definition};";
    }

    private sealed class IndexDefinitionForSync
    {
        public int IndexId { get; set; }
        public string IndexName { get; set; } = string.Empty;
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }
        public byte IndexType { get; set; } // 1=聚集, 2=非聚集
        public string KeyColumns { get; set; } = string.Empty;
        public string? IncludedColumns { get; set; }
        public string? FilterDefinition { get; set; }
    }

    private sealed class ConstraintForSync
    {
        public string ColumnName { get; set; } = string.Empty;
        public string ConstraintName { get; set; } = string.Empty;
        public string Definition { get; set; } = string.Empty;
    }
    
    /// <summary>
    /// 对齐源表索引到分区方案
    /// </summary>
    private async Task<AutoFixStepOutcome> ExecuteAlignSourceIndexesAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        CancellationToken cancellationToken)
    {
        var log = new StringBuilder();
        log.AppendLine($"开始对齐源表索引: [{configuration.SchemaName}].[{configuration.TableName}]");

        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
            
            // 1. 查询未对齐的索引
            var unalignedIndexes = await GetUnalignedIndexesForFixAsync(
                connection,
                configuration.SchemaName,
                configuration.TableName,
                configuration.PartitionSchemeName,
                cancellationToken);

            if (!unalignedIndexes.Any())
            {
                log.AppendLine("未发现需要对齐的索引");
                return new AutoFixStepOutcome(
                    new PartitionSwitchAutoFixExecution(
                        "AlignSourceIndexes",
                        true,
                        "源表索引已对齐,无需处理",
                        log.ToString(),
                        0),
                    null);
            }

            log.AppendLine($"发现 {unalignedIndexes.Count} 个未对齐的索引:");
            foreach (var idx in unalignedIndexes)
            {
                log.AppendLine($"  - {idx.IndexName} ({idx.IndexType})");
            }

            var droppedIndexes = new List<string>();
            var recreatedIndexes = new List<string>();

            // 2. 对每个未对齐的索引进行重建
            foreach (var index in unalignedIndexes)
            {
                log.AppendLine();
                log.AppendLine($"处理索引: {index.IndexName}");

                try
                {
                    // 2.1 删除索引
                    var dropSql = $"DROP INDEX [{index.IndexName}] ON [{configuration.SchemaName}].[{configuration.TableName}];";
                    log.AppendLine($"  执行: {dropSql}");
                    await sqlExecutor.ExecuteAsync(connection, dropSql, timeoutSeconds: 600);
                    droppedIndexes.Add(index.IndexName);
                    log.AppendLine($"  ✓ 索引已删除");

                    // 2.2 重建索引(包含分区列,对齐到分区方案)
                    var createSql = BuildAlignedIndexCreateSql(
                        configuration.SchemaName,
                        configuration.TableName,
                        index,
                        configuration.PartitionColumn.Name,
                        configuration.PartitionSchemeName);
                    
                    log.AppendLine($"  执行: {createSql}");
                    await sqlExecutor.ExecuteAsync(connection, createSql, timeoutSeconds: 600);
                    recreatedIndexes.Add(index.IndexName);
                    log.AppendLine($"  ✓ 索引已重建并对齐到分区方案");
                }
                catch (Exception ex)
                {
                    log.AppendLine($"  ✗ 处理索引失败: {ex.Message}");
                    logger.LogError(ex, "处理索引 {IndexName} 时发生异常", index.IndexName);
                    throw;
                }
            }

            log.AppendLine();
            log.AppendLine("索引对齐完成:");
            log.AppendLine($"  - 删除索引: {string.Join(", ", droppedIndexes)}");
            log.AppendLine($"  - 重建索引: {string.Join(", ", recreatedIndexes)}");

            return new AutoFixStepOutcome(
                new PartitionSwitchAutoFixExecution(
                    "AlignSourceIndexes",
                    true,
                    $"成功对齐 {recreatedIndexes.Count} 个索引",
                    log.ToString(),
                    0),
                null);
        }
        catch (Exception ex)
        {
            log.AppendLine();
            log.AppendLine($"对齐源表索引失败: {ex.Message}");
            logger.LogError(ex, "对齐源表 [{Schema}].[{Table}] 索引时发生异常", configuration.SchemaName, configuration.TableName);

            return new AutoFixStepOutcome(
                new PartitionSwitchAutoFixExecution(
                    "AlignSourceIndexes",
                    false,
                    $"对齐源表索引失败: {ex.Message}",
                    log.ToString(),
                    0),
                null);
        }
    }

    /// <summary>
    /// 对齐目标表索引到分区方案
    /// </summary>
    private async Task<AutoFixStepOutcome> ExecuteAlignTargetIndexesAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        PartitionSwitchInspectionContext context,
        CancellationToken cancellationToken)
    {
        var log = new StringBuilder();
        var targetSchema = context.TargetSchema ?? configuration.SchemaName;
        var targetTable = context.TargetTable;
        log.AppendLine($"开始对齐目标表索引: [{targetSchema}].[{targetTable}]");

        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
            
            // 1. 查询未对齐的索引
            var unalignedIndexes = await GetUnalignedIndexesForFixAsync(
                connection,
                targetSchema,
                targetTable,
                configuration.PartitionSchemeName,
                cancellationToken);

            if (!unalignedIndexes.Any())
            {
                log.AppendLine("未发现需要对齐的索引");
                return new AutoFixStepOutcome(
                    new PartitionSwitchAutoFixExecution(
                        "AlignTargetIndexes",
                        true,
                        "目标表索引已对齐,无需处理",
                        log.ToString(),
                        0),
                    null);
            }

            log.AppendLine($"发现 {unalignedIndexes.Count} 个未对齐的索引:");
            foreach (var idx in unalignedIndexes)
            {
                log.AppendLine($"  - {idx.IndexName} ({idx.IndexType})");
            }

            var droppedIndexes = new List<string>();
            var recreatedIndexes = new List<string>();

            // 2. 对每个未对齐的索引进行重建
            foreach (var index in unalignedIndexes)
            {
                log.AppendLine();
                log.AppendLine($"处理索引: {index.IndexName}");

                try
                {
                    // 2.1 删除索引
                    var dropSql = $"DROP INDEX [{index.IndexName}] ON [{targetSchema}].[{targetTable}];";
                    log.AppendLine($"  执行: {dropSql}");
                    await sqlExecutor.ExecuteAsync(connection, dropSql, timeoutSeconds: 600);
                    droppedIndexes.Add(index.IndexName);
                    log.AppendLine($"  ✓ 索引已删除");

                    // 2.2 重建索引(包含分区列,对齐到分区方案)
                    var createSql = BuildAlignedIndexCreateSql(
                        targetSchema,
                        targetTable,
                        index,
                        configuration.PartitionColumn.Name,
                        configuration.PartitionSchemeName);
                    
                    log.AppendLine($"  执行: {createSql}");
                    await sqlExecutor.ExecuteAsync(connection, createSql, timeoutSeconds: 600);
                    recreatedIndexes.Add(index.IndexName);
                    log.AppendLine($"  ✓ 索引已重建并对齐到分区方案");
                }
                catch (Exception ex)
                {
                    log.AppendLine($"  ✗ 处理索引失败: {ex.Message}");
                    logger.LogError(ex, "处理目标表索引 {IndexName} 时发生异常", index.IndexName);
                    throw;
                }
            }

            log.AppendLine();
            log.AppendLine("目标表索引对齐完成:");
            log.AppendLine($"  - 删除索引: {string.Join(", ", droppedIndexes)}");
            log.AppendLine($"  - 重建索引: {string.Join(", ", recreatedIndexes)}");

            return new AutoFixStepOutcome(
                new PartitionSwitchAutoFixExecution(
                    "AlignTargetIndexes",
                    true,
                    $"成功对齐 {recreatedIndexes.Count} 个索引",
                    log.ToString(),
                    0),
                null);
        }
        catch (Exception ex)
        {
            log.AppendLine();
            log.AppendLine($"对齐目标表索引失败: {ex.Message}");
            logger.LogError(ex, "对齐目标表 [{Schema}].[{Table}] 索引时发生异常", targetSchema, targetTable);

            return new AutoFixStepOutcome(
                new PartitionSwitchAutoFixExecution(
                    "AlignTargetIndexes",
                    false,
                    $"对齐目标表索引失败: {ex.Message}",
                    log.ToString(),
                    0),
                null);
        }
    }

    /// <summary>
    /// 查询需要对齐的索引信息(用于修复)
    /// </summary>
    private async Task<List<IndexForFix>> GetUnalignedIndexesForFixAsync(
        SqlConnection connection,
        string schema,
        string table,
        string partitionSchemeName,
        CancellationToken cancellationToken)
    {
        var sql = @"
SELECT 
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    STUFF((
        SELECT ', ' + c.name
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS KeyColumns,
    STUFF((
        SELECT ', ' + c.name
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS IncludedColumns,
    i.filter_definition AS FilterDefinition
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
WHERE s.name = @Schema
  AND t.name = @Table
  AND i.type IN (1, 2)  -- 聚集索引和非聚集索引
  AND (
      ds.type <> 'PS'  -- 不在分区方案上
      OR ds.name <> @PartitionSchemeName  -- 或者不是目标分区方案
  )
ORDER BY i.index_id;";

        var result = await sqlExecutor.QueryAsync<IndexForFix>(
            connection,
            sql,
            new { Schema = schema, Table = table, PartitionSchemeName = partitionSchemeName });

        return result.ToList();
    }

    /// <summary>
    /// 构建对齐的索引创建SQL
    /// </summary>
    private string BuildAlignedIndexCreateSql(
        string schema,
        string table,
        IndexForFix index,
        string partitionColumn,
        string partitionSchemeName)
    {
        var sb = new StringBuilder();

        // CREATE [UNIQUE] [CLUSTERED|NONCLUSTERED] INDEX
        sb.Append("CREATE ");
        if (index.IsUnique)
            sb.Append("UNIQUE ");
        
        sb.Append(index.IndexType == "CLUSTERED" ? "CLUSTERED " : "NONCLUSTERED ");
        sb.Append($"INDEX [{index.IndexName}] ");
        sb.AppendLine($"ON [{schema}].[{table}]");

        // 键列(确保包含分区列)
        var keyColumns = index.KeyColumns?.Split(new[] { ", " }, StringSplitOptions.RemoveEmptyEntries) ?? Array.Empty<string>();
        var keyColumnsList = new List<string>(keyColumns);
        
        // 如果分区列不在键列中,添加到末尾
        if (!keyColumnsList.Any(c => c.Equals(partitionColumn, StringComparison.OrdinalIgnoreCase)))
        {
            keyColumnsList.Add(partitionColumn);
        }

        sb.Append("(");
        sb.Append(string.Join(", ", keyColumnsList.Select(c => $"[{c}]")));
        sb.AppendLine(")");

        // INCLUDE列
        if (!string.IsNullOrWhiteSpace(index.IncludedColumns))
        {
            var includedColumns = index.IncludedColumns.Split(new[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
            sb.Append("INCLUDE (");
            sb.Append(string.Join(", ", includedColumns.Select(c => $"[{c}]")));
            sb.AppendLine(")");
        }

        // WHERE过滤条件
        if (!string.IsNullOrWhiteSpace(index.FilterDefinition))
        {
            sb.AppendLine($"WHERE {index.FilterDefinition}");
        }

        // ON分区方案(使用分区列)
        sb.AppendLine($"ON [{partitionSchemeName}]([{partitionColumn}]);");

        return sb.ToString();
    }

    /// <summary>
    /// 用于修复的索引信息
    /// </summary>
    private sealed class IndexForFix
    {
        public string IndexName { get; set; } = string.Empty;
        public string IndexType { get; set; } = string.Empty;
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }
        public string? KeyColumns { get; set; }
        public string? IncludedColumns { get; set; }
        public string? FilterDefinition { get; set; }
    }

    /// <summary>
    /// 加载表的所有索引(用于SyncIndexes)
    /// </summary>
    private async Task<List<TableIndexDefinition>> LoadAllIndexesAsync(
        SqlConnection connection,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        var sql = @"
SELECT 
    i.name AS IndexName,
    i.type AS IndexType,
    i.type_desc AS IndexTypeDesc,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    i.is_unique_constraint AS IsUniqueConstraint,
    STUFF((
        SELECT ', ' + c.name + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS KeyColumns,
    STUFF((
        SELECT ', ' + c.name
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS IncludedColumns,
    i.filter_definition AS FilterDefinition
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @Schema
  AND t.name = @Table
  AND i.type IN (1, 2)  -- 聚集索引和非聚集索引
ORDER BY i.index_id;";

        var result = await sqlExecutor.QueryAsync<TableIndexDefinition>(
            connection,
            sql,
            new { Schema = schema, Table = table });

        return result.ToList();
    }

    /// <summary>
    /// 检查两个索引定义是否匹配
    /// </summary>
    private bool IsIndexDefinitionMatching(TableIndexDefinition source, TableIndexDefinition target)
    {
        // 比较索引类型
        if (source.IndexType != target.IndexType)
            return false;

        // 比较唯一性
        if (source.IsUnique != target.IsUnique)
            return false;

        // 比较键列(规范化后比较)
        var sourceKeys = NormalizeColumnList(source.KeyColumns);
        var targetKeys = NormalizeColumnList(target.KeyColumns);
        if (sourceKeys != targetKeys)
            return false;

        // 比较INCLUDE列
        var sourceIncludes = NormalizeColumnList(source.IncludedColumns);
        var targetIncludes = NormalizeColumnList(target.IncludedColumns);
        if (sourceIncludes != targetIncludes)
            return false;

        // 比较过滤条件
        var sourceFilter = NormalizeFilterDefinition(source.FilterDefinition);
        var targetFilter = NormalizeFilterDefinition(target.FilterDefinition);
        if (sourceFilter != targetFilter)
            return false;

        return true;
    }

    /// <summary>
    /// 规范化列列表用于比较
    /// </summary>
    /// <remarks>
    /// 注意:索引键列的顺序很重要,不能排序!只做空格规范化处理。
    /// </remarks>
    private string NormalizeColumnList(string? columns)
    {
        if (string.IsNullOrWhiteSpace(columns))
            return string.Empty;

        // 不能改变列的顺序!只规范化空格
        var parts = columns.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(p => p.Trim());

        return string.Join(", ", parts);
    }

    /// <summary>
    /// 规范化过滤条件用于比较
    /// </summary>
    private string NormalizeFilterDefinition(string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
            return string.Empty;

        return filter.Trim();
    }

    /// <summary>
    /// 构建删除索引的SQL脚本
    /// </summary>
    private string BuildDropIndexScript(string schema, string table, TableIndexDefinition index)
    {
        if (index.IsPrimaryKey)
        {
            return $"ALTER TABLE [{schema}].[{table}] DROP CONSTRAINT [{index.IndexName}];";
        }
        else if (index.IsUniqueConstraint)
        {
            return $"ALTER TABLE [{schema}].[{table}] DROP CONSTRAINT [{index.IndexName}];";
        }
        else
        {
            return $"DROP INDEX [{index.IndexName}] ON [{schema}].[{table}];";
        }
    }
}

