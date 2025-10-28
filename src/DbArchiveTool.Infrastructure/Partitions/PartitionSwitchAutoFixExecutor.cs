using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.Executors;
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
    private readonly ILogger<PartitionSwitchAutoFixExecutor> logger;

    public PartitionSwitchAutoFixExecutor(
        IDbConnectionFactory connectionFactory,
        ISqlExecutor sqlExecutor,
        SqlPartitionCommandExecutor partitionCommandExecutor,
        ILogger<PartitionSwitchAutoFixExecutor> logger)
    {
        this.connectionFactory = connectionFactory;
        this.sqlExecutor = sqlExecutor;
        this.partitionCommandExecutor = partitionCommandExecutor;
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

                return PartitionSwitchAutoFixResult.Failure(executions);
            }
        }

        return PartitionSwitchAutoFixResult.Success(executions);
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

        var script = BuildCreateTableScript(context.TargetSchema, context.TargetTable, columns);
        await sqlExecutor.ExecuteAsync(targetConnection, script, timeoutSeconds: 120);

        var message = $"已创建目标表 [{context.TargetSchema}].[{context.TargetTable}]。";
        return new AutoFixStepOutcome(
            new PartitionSwitchAutoFixExecution("CreateTargetTable", true, message, script, 0),
            new AutoFixRollbackAction(async ct => await RollbackCreateTargetTableAsync(dataSourceId, context, ct)));
    }

    // 清空目标表残留数据，确保 SWITCH 时为空
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

        const string rowCountSql = @"SELECT SUM(p.rows) FROM sys.partitions p INNER JOIN sys.tables t ON p.object_id = t.object_id INNER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = @SchemaName AND t.name = @TableName AND p.index_id IN (0,1)";
        var rows = await sqlExecutor.QuerySingleAsync<long>(
            targetConnection,
            rowCountSql,
            new { SchemaName = context.TargetSchema, TableName = context.TargetTable });

        if (rows == 0)
        {
            return new AutoFixStepOutcome(
                new PartitionSwitchAutoFixExecution(
                    "CleanupResidualData",
                    true,
                    "目标表无数据，跳过清理。",
                    $"-- Skip cleanup, table empty: [{context.TargetSchema}].[{context.TargetTable}]",
                    0),
                null);
        }

        var script = $"TRUNCATE TABLE [{context.TargetSchema}].[{context.TargetTable}];";
        await sqlExecutor.ExecuteAsync(targetConnection, script, timeoutSeconds: 60);

        return new AutoFixStepOutcome(
            new PartitionSwitchAutoFixExecution(
                "CleanupResidualData",
                true,
                $"已清空目标表数据，原有行数 {rows}。",
                script,
                0),
            null);
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

        // 加载源表的所有非聚集索引（聚集索引通常与主键关联，在表创建时已处理）
        var sourceIndexes = await LoadNonClusteredIndexesAsync(
            sourceConnection,
            configuration.SchemaName,
            configuration.TableName,
            cancellationToken);

        if (sourceIndexes.Count == 0)
        {
            return new AutoFixStepOutcome(
                new PartitionSwitchAutoFixExecution(
                    "SyncIndexes",
                    true,
                    "源表无需同步的非聚集索引，跳过。",
                    "-- No non-clustered indexes to sync",
                    0),
                null);
        }

        var scriptBuilder = new StringBuilder();
        var createdIndexNames = new List<string>(sourceIndexes.Count);

        foreach (var index in sourceIndexes)
        {
            var indexScript = BuildCreateIndexScript(context.TargetSchema, context.TargetTable, index);
            scriptBuilder.AppendLine(indexScript);
            scriptBuilder.AppendLine("GO");
            scriptBuilder.AppendLine();

            try
            {
                await sqlExecutor.ExecuteAsync(connectionForTarget, indexScript, timeoutSeconds: 300);
                createdIndexNames.Add(index.IndexName);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "创建索引 {IndexName} 失败，跳过该索引。", index.IndexName);
            }
        }

        var script = scriptBuilder.ToString();
        var message = createdIndexNames.Count > 0
            ? $"已同步 {createdIndexNames.Count} 个索引：{string.Join(", ", createdIndexNames)}。"
            : "索引同步未成功创建任何索引。";

        return new AutoFixStepOutcome(
            new PartitionSwitchAutoFixExecution("SyncIndexes", createdIndexNames.Count > 0, message, script, 0),
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

    private static string BuildCreateTableScript(string schema, string table, IReadOnlyList<ColumnMetadata> columns)
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

        builder.AppendLine("    );");
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
        public long? IdentitySeed { get; set; }
        public long? IdentityIncrement { get; set; }
        public string? ComputedDefinition { get; set; }
        public bool IsPersisted { get; set; }
        public string? CollationName { get; set; }
        public bool IsRowGuid { get; set; }
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

    private static string BuildCreateIndexScript(string schema, string table, IndexDefinitionForSync index)
    {
        var builder = new StringBuilder();
        var uniqueKeyword = index.IsUnique ? "UNIQUE " : string.Empty;

        builder.Append($"CREATE {uniqueKeyword}NONCLUSTERED INDEX [{index.IndexName}] ON [{schema}].[{table}] (");
        builder.Append(index.KeyColumns);
        builder.Append(")");

        if (!string.IsNullOrWhiteSpace(index.IncludedColumns))
        {
            builder.Append($" INCLUDE ({index.IncludedColumns})");
        }

        if (!string.IsNullOrWhiteSpace(index.FilterDefinition))
        {
            builder.Append($" WHERE {index.FilterDefinition}");
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
}
