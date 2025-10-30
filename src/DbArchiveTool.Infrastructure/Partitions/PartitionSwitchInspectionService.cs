using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.Models;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Partitions;

/// <summary>
/// 默认的分区切换检查实现，验证目标表结构、数据状态等前置条件。
/// </summary>
internal sealed class PartitionSwitchInspectionService : IPartitionSwitchInspectionService
{
    private readonly IDbConnectionFactory connectionFactory;
    private readonly ISqlExecutor sqlExecutor;
    private readonly ILogger<PartitionSwitchInspectionService> logger;

    public PartitionSwitchInspectionService(
        IDbConnectionFactory connectionFactory,
        ISqlExecutor sqlExecutor,
        ILogger<PartitionSwitchInspectionService> logger)
    {
        this.connectionFactory = connectionFactory;
        this.sqlExecutor = sqlExecutor;
        this.logger = logger;
    }

    public async Task<PartitionSwitchInspectionResult> InspectAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        PartitionSwitchInspectionContext context,
        CancellationToken cancellationToken = default)
    {
        if (configuration is null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        var blocking = new List<PartitionSwitchIssue>();
        var warnings = new List<PartitionSwitchIssue>();
        var autoFix = new List<PartitionSwitchAutoFixStep>();

        if (!int.TryParse(context.SourcePartitionKey, NumberStyles.Integer, CultureInfo.InvariantCulture, out var partitionNumber) || partitionNumber <= 0)
        {
            blocking.Add(new PartitionSwitchIssue(
                "InvalidPartitionKey",
                "源分区编号格式不正确，请使用有效的分区编号（正整数）。",
                "请从分区边界列表中选择正确的分区编号。"));
        }

        var targetSchema = context.TargetSchema?.Trim();
        var targetTable = context.TargetTable?.Trim();
        if (string.IsNullOrWhiteSpace(targetTable))
        {
            blocking.Add(new PartitionSwitchIssue(
                "InvalidTargetTable",
                "目标表名称不能为空。",
                "请提供目标表的名称，例如 Archive.SalesHistory 或 [Archive].[SalesHistory]。"));
        }

        await using var sourceConnection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
        await using var targetConnection = context.UseSourceAsTarget
            ? null
            : await connectionFactory.CreateTargetSqlConnectionAsync(dataSourceId, cancellationToken);

        var connectionForTarget = targetConnection ?? sourceConnection;

        var sourceTableInfo = await BuildTableInfoAsync(sourceConnection, context.SourceDatabase, configuration.SchemaName, configuration.TableName, cancellationToken);
        await EnsureAlterPermissionAsync(
            sourceConnection,
            context.SourceDatabase,
            configuration.SchemaName,
            configuration.TableName,
            "SourceMissingPermissions",
            blocking,
            warnings,
            cancellationToken);

        await CheckBlockingLocksAsync(
            sourceConnection,
            context.SourceDatabase,
            configuration.SchemaName,
            configuration.TableName,
            blocking,
            warnings,
            cancellationToken);

        // 加载源表索引定义，后续与目标表进行逐项比对
        var sourceIndexes = await LoadIndexDefinitionsAsync(
            sourceConnection,
            configuration.SchemaName,
            configuration.TableName,
            configuration.PartitionColumn.Name,
            cancellationToken);

        // 加载源表默认约束和检查约束，确保 SWITCH 前约束逻辑完全一致
        var sourceDefaultConstraints = await LoadDefaultConstraintsAsync(
            sourceConnection,
            context.SourceDatabase,
            configuration.SchemaName,
            configuration.TableName,
            cancellationToken);

        var sourceCheckConstraints = await LoadCheckConstraintsAsync(
            sourceConnection,
            context.SourceDatabase,
            configuration.SchemaName,
            configuration.TableName,
            cancellationToken);

        // 捕获源表分区方案、函数、边界等元数据，为后续兼容性校验做准备
        var sourcePartitionMetadata = await LoadPartitionMetadataAsync(
            sourceConnection,
            context.SourceDatabase,
            configuration.SchemaName,
            configuration.TableName,
            cancellationToken);

        IReadOnlyList<TableIndexDefinition> targetIndexes = Array.Empty<TableIndexDefinition>();
        IReadOnlyList<DefaultConstraintDefinition> targetDefaultConstraints = Array.Empty<DefaultConstraintDefinition>();
        IReadOnlyList<CheckConstraintDefinition> targetCheckConstraints = Array.Empty<CheckConstraintDefinition>();
        var targetPartitionMetadata = PartitionMetadataDefinition.NotPartitioned;
        PartitionSwitchTableInfo targetTableInfo;
        bool targetExists = false;

        if (!string.IsNullOrWhiteSpace(targetSchema) && !string.IsNullOrWhiteSpace(targetTable))
        {
            targetExists = await TableExistsAsync(connectionForTarget, targetSchema!, targetTable!, cancellationToken);
            if (!targetExists)
            {
                // 目标表不存在时,不作为阻塞项,而是提供自动创建选项
                warnings.Add(new PartitionSwitchIssue(
                    "TargetTableMissing",
                    $"未找到目标表 {FormatQualifiedName(context.TargetDatabase, targetSchema!, targetTable!)}。",
                    "将通过自动补齐一次性创建目标表并同步所有索引和约束。"));

                targetTableInfo = new PartitionSwitchTableInfo(targetSchema!, targetTable!, 0, Array.Empty<PartitionSwitchColumnInfo>());

                // 一次性添加所有补齐步骤:创建表 + 同步索引 + 同步约束
                autoFix.Add(new PartitionSwitchAutoFixStep(
                    "CreateTargetTable",
                    $"基于源表 {FormatQualifiedName(context.SourceDatabase, configuration.SchemaName, configuration.TableName)} 自动创建目标表 {FormatQualifiedName(context.TargetDatabase, targetSchema!, targetTable!)}。",
                    "自动补齐会复制源表的列结构(包含分区方案)并在目标库中创建空表。"));
                
                autoFix.Add(new PartitionSwitchAutoFixStep(
                    "SyncIndexes",
                    "从源表复制所有索引(聚集索引、主键、唯一索引)到新创建的目标表。",
                    "确保目标表的索引结构与源表完全一致,满足 SWITCH 操作要求。"));
                
                autoFix.Add(new PartitionSwitchAutoFixStep(
                    "SyncConstraints",
                    "从源表复制所有约束(DEFAULT约束、CHECK约束)到新创建的目标表。",
                    "确保目标表的数据完整性规则与源表完全一致。"));
            }
            else
            {
                targetTableInfo = await BuildTableInfoAsync(connectionForTarget, context.TargetDatabase, targetSchema!, targetTable!, cancellationToken);

                await EnsureAlterPermissionAsync(
                    connectionForTarget,
                    context.TargetDatabase,
                    targetSchema!,
                    targetTable!,
                    "TargetMissingPermissions",
                    blocking,
                    warnings,
                    cancellationToken);

                // 目标表存在时，同步加载索引、约束和分区元数据用于结构核对
                targetIndexes = await LoadIndexDefinitionsAsync(
                    connectionForTarget,
                    targetSchema!,
                    targetTable!,
                    configuration.PartitionColumn.Name,
                    cancellationToken);

                targetDefaultConstraints = await LoadDefaultConstraintsAsync(
                    connectionForTarget,
                    context.TargetDatabase,
                    targetSchema!,
                    targetTable!,
                    cancellationToken);

                targetCheckConstraints = await LoadCheckConstraintsAsync(
                    connectionForTarget,
                    context.TargetDatabase,
                    targetSchema!,
                    targetTable!,
                    cancellationToken);

                targetPartitionMetadata = await LoadPartitionMetadataAsync(
                    connectionForTarget,
                    context.TargetDatabase,
                    targetSchema!,
                    targetTable!,
                    cancellationToken);

                // 检查目标表是否为空
                // 累积式归档:如果目标表是分区表,允许非空(SWITCH 会追加到对应分区)
                // 但如果目标表不是分区表,必须为空
                if (targetTableInfo.RowCount > 0)
                {
                    var isTargetPartitioned = targetPartitionMetadata != null && targetPartitionMetadata.IsPartitioned;
                    
                    if (isTargetPartitioned)
                    {
                        // 目标表是分区表:检查目标分区是否为空
                        // SQL Server 要求目标分区必须为空才能执行 SWITCH
                        var targetPartitionRowCount = await LoadPartitionRowCountAsync(
                            connectionForTarget,
                            context.TargetDatabase,
                            targetSchema!,
                            targetTable!,
                            partitionNumber,
                            cancellationToken);

                        if (targetPartitionRowCount > 0)
                        {
                            // 目标分区不为空,提供自动补齐选项(清空目标分区)
                            warnings.Add(new PartitionSwitchIssue(
                                "TargetPartitionNotEmpty",
                                $"目标表 {FormatQualifiedName(context.TargetDatabase, targetSchema!, targetTable!)} 的分区 {partitionNumber} 包含 {targetPartitionRowCount:N0} 行数据。",
                                "SWITCH 操作要求目标分区必须为空。建议勾选'清空目标表残留数据'选项,系统将自动清空该分区的数据。"));
                            
                            // 添加自动补齐步骤:清空目标分区数据
                            autoFix.Add(new PartitionSwitchAutoFixStep(
                                "CleanupResidualData",
                                $"清空目标表 {FormatQualifiedName(context.TargetDatabase, targetSchema!, targetTable!)} 分区 {partitionNumber} 的残留数据（当前 {targetPartitionRowCount:N0} 行）",
                                $"将执行 TRUNCATE TABLE ... WITH (PARTITIONS ({partitionNumber})) 清空目标分区,以满足 SWITCH 要求。请确认该分区的数据可以删除。"));
                        }
                        else
                        {
                            // 目标分区为空,只给出整表数据的提示信息
                            warnings.Add(new PartitionSwitchIssue(
                                "TargetTableNotEmpty",
                                $"目标表 {FormatQualifiedName(context.TargetDatabase, targetSchema!, targetTable!)} 当前包含 {targetTableInfo.RowCount} 行数据（累积式归档模式），但目标分区 {partitionNumber} 为空。",
                                "数据将追加到目标表的对应分区中。"));
                        }
                    }
                    else
                    {
                        // 目标表不是分区表:必须为空,否则无法 SWITCH
                        blocking.Add(new PartitionSwitchIssue(
                            "TargetTableNotEmpty",
                            $"目标表 {FormatQualifiedName(context.TargetDatabase, targetSchema!, targetTable!)} 不是分区表,但包含 {targetTableInfo.RowCount} 行数据。",
                            "普通表必须为空才能接收分区切换的数据。请清空目标表,或将其转换为分区表以支持累积式归档。"));
                    }
                }

                if (await HasDmlTriggerAsync(connectionForTarget, context.TargetDatabase, targetSchema!, targetTable!, cancellationToken))
                {
                    blocking.Add(new PartitionSwitchIssue(
                        "TargetHasTriggers",
                        $"目标表 {FormatQualifiedName(context.TargetDatabase, targetSchema!, targetTable!)} 存在启用的 DML 触发器，无法执行分区切换。",
                        "请先禁用或删除目标表上的 INSERT/UPDATE/DELETE 触发器。"));
                }

                var foreignKeyCount = await CountForeignKeysAsync(connectionForTarget, context.TargetDatabase, targetSchema!, targetTable!, cancellationToken);
                if (foreignKeyCount > 0)
                {
                    blocking.Add(new PartitionSwitchIssue(
                        "TargetHasForeignKeys",
                        $"目标表 {FormatQualifiedName(context.TargetDatabase, targetSchema!, targetTable!)} 上存在 {foreignKeyCount} 个外键约束。",
                        "请移除或禁用这些外键约束后再执行 SWITCH 操作。"));
                }
            }
        }
        else
        {
            targetTableInfo = new PartitionSwitchTableInfo(
                targetSchema ?? configuration.SchemaName,
                targetTable ?? string.Empty,
                0,
                Array.Empty<PartitionSwitchColumnInfo>());
        }

        // 首先校验分区方案/函数/边界是否完全一致，避免结构差异导致 SWITCH 失败
        // 只有目标表存在且已加载分区元数据时才进行分区兼容性检查
        if (targetExists && targetPartitionMetadata != null)
        {
            EvaluatePartitionCompatibility(
                configuration,
                context.SourceDatabase,
                context.TargetDatabase,
                targetSchema,
                targetTable,
                partitionNumber,
                sourcePartitionMetadata,
                targetPartitionMetadata,
                targetExists,
                blocking);
        }
        
        // 检查源表和目标表的索引是否对齐到分区方案
        await EvaluateIndexPartitionAlignmentAsync(
            sourceConnection,
            connectionForTarget,
            context.SourceDatabase,
            context.TargetDatabase,
            configuration.SchemaName,
            configuration.TableName,
            targetSchema,
            targetTable,
            targetExists,
            configuration.PartitionSchemeName,
            blocking,
            autoFix,
            warnings,
            cancellationToken);

        if (targetExists)
        {
            // 目标表存在时才执行列、索引、约束等结构核对
            EvaluateColumnCompatibility(sourceTableInfo, targetTableInfo, blocking);
            EvaluateIndexCompatibility(sourceIndexes, targetIndexes, blocking, autoFix, warnings);
            var hasDefaultMismatch = EvaluateDefaultConstraintCompatibility(sourceDefaultConstraints, targetDefaultConstraints, blocking, autoFix, warnings);
            EvaluateCheckConstraintCompatibility(sourceCheckConstraints, targetCheckConstraints, blocking, autoFix, hasDefaultMismatch, warnings);
        }

        if (!context.UseSourceAsTarget && !string.Equals(context.TargetDatabase, context.SourceDatabase, StringComparison.OrdinalIgnoreCase))
        {
            warnings.Add(new PartitionSwitchIssue(
                "CrossDatabaseSwitch",
                $"本次 SWITCH 将在源库 {context.SourceDatabase} 与目标库 {context.TargetDatabase} 之间执行，请确认目标库已部署相同的分区函数与分区架构。",
                "请在执行前检查目标数据库的分区配置，必要时同步分区架构。"));
        }

        // 判断是否可以继续: 无阻塞项 且 (无自动补齐步骤 或 所有步骤已执行)
        // 如果有未执行的自动补齐步骤,也应该阻止继续,提示用户先执行自动补齐
        var canProceed = blocking.Count == 0 && autoFix.Count == 0;

        var result = new PartitionSwitchInspectionResult(
            canProceed,
            blocking,
            warnings,
            autoFix,
            sourceTableInfo,
            targetTableInfo,
            BuildPlan(blocking, warnings, autoFix));

        return result;
    }

    // 根据检查结果生成补齐计划骨架，方便后续自动补齐与人工处理编排
    private static PartitionSwitchPlan BuildPlan(
        IReadOnlyList<PartitionSwitchIssue> blocking,
        IReadOnlyList<PartitionSwitchIssue> warnings,
        IReadOnlyList<PartitionSwitchAutoFixStep> autoFixSteps)
    {
        var blockerDetails = blocking
            .Select(issue => new PartitionSwitchPlanBlocker(
                issue.Code,
                issue.Message,
                issue.Message,
                issue.Recommendation))
            .ToList();

        var warningDetails = warnings
            .Select(issue => new PartitionSwitchPlanWarning(
                issue.Code,
                issue.Message,
                issue.Message,
                issue.Recommendation))
            .ToList();

        var autoFixDetails = autoFixSteps
            .Select(step => ToPlanAutoFix(step))
            .ToList();

        return new PartitionSwitchPlan(blockerDetails, autoFixDetails, warningDetails);
    }

    // 将单条自动补齐建议转换为计划明细，填充分类与影响面描述
    private static PartitionSwitchPlanAutoFix ToPlanAutoFix(PartitionSwitchAutoFixStep step)
    {
        var (category, impactScope, requiresExclusiveLock) = step.Code switch
        {
            "CreateTargetTable" => (PartitionSwitchAutoFixCategory.CreateTargetTable, "将在目标数据库创建空表结构，保持列定义一致。", true),
            "SyncPartitionObjects" => (PartitionSwitchAutoFixCategory.SyncPartitionObjects, "同步分区函数与方案，确保分区划分一致。", true),
            "SyncIndexes" => (PartitionSwitchAutoFixCategory.SyncIndexes, "重建目标表索引，与源表完全一致。", true),
            "SyncConstraints" => (PartitionSwitchAutoFixCategory.SyncConstraints, "补齐目标表约束，避免切换失败。", true),
            "RefreshStatistics" => (PartitionSwitchAutoFixCategory.RefreshStatistics, "刷新统计信息，降低执行计划风险。", false),
            "CleanupResidualData" => (PartitionSwitchAutoFixCategory.CleanupResidualData, "清空目标表残留数据，腾出切换空间。", true),
            _ => (PartitionSwitchAutoFixCategory.Other, "执行辅助补齐步骤。", false),
        };

        var commands = new List<PartitionSwitchPlanCommand>
        {
            new("-- TODO: 生成自动补齐脚本", step.Description)
        };

        return new PartitionSwitchPlanAutoFix(
            step.Code,
            step.Description,
            category,
            impactScope,
            commands,
            step.Recommendation,
            requiresExclusiveLock);
    }

    private async Task<bool> TableExistsAsync(SqlConnection connection, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COUNT(1)
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @Schema AND t.name = @Table;";

        var count = await sqlExecutor.QuerySingleAsync<int>(
            connection,
            sql,
            new { Schema = schema, Table = table },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();
        return count > 0;
    }

    private async Task<PartitionSwitchTableInfo> BuildTableInfoAsync(SqlConnection connection, string database, string schema, string table, CancellationToken cancellationToken)
    {
        var columns = await LoadColumnsAsync(connection, database, schema, table, cancellationToken);
        var rowCount = await LoadRowCountAsync(connection, database, schema, table, cancellationToken);
        return new PartitionSwitchTableInfo(schema, table, rowCount, columns);
    }

    private async Task<IReadOnlyList<PartitionSwitchColumnInfo>> LoadColumnsAsync(SqlConnection connection, string database, string schema, string table, CancellationToken cancellationToken)
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
    c.is_computed AS IsComputed
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID(@FullName)
ORDER BY c.column_id;";

    var fullName = FormatQualifiedName(database, schema, table);
        var rows = await sqlExecutor.QueryAsync<ColumnRow>(
            connection,
            sql,
            new { FullName = fullName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();

        return rows
            .Select(row => new PartitionSwitchColumnInfo(
                row.ColumnName,
                row.DataType,
                NormalizeMaxLength(row.DataType, row.MaxLength),
                row.Precision,
                row.Scale,
                row.IsNullable,
                row.IsIdentity,
                row.IsComputed))
            .ToList();
    }

    private async Task<long> LoadRowCountAsync(SqlConnection connection, string database, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COALESCE(SUM(p.rows), 0)
FROM sys.partitions p
WHERE p.object_id = OBJECT_ID(@FullName)
  AND p.index_id IN (0, 1);";

        var fullName = FormatQualifiedName(database, schema, table);
        var count = await sqlExecutor.QuerySingleAsync<long>(
            connection,
            sql,
            new { FullName = fullName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();
        return count;
    }

    /// <summary>
    /// 查询指定分区的行数
    /// </summary>
    private async Task<long> LoadPartitionRowCountAsync(
        SqlConnection connection, 
        string database, 
        string schema, 
        string table, 
        int partitionNumber,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COALESCE(SUM(p.rows), 0)
FROM sys.partitions p
WHERE p.object_id = OBJECT_ID(@FullName)
  AND p.index_id IN (0, 1)
  AND p.partition_number = @PartitionNumber;";

        var fullName = FormatQualifiedName(database, schema, table);
        var count = await sqlExecutor.QuerySingleAsync<long>(
            connection,
            sql,
            new { FullName = fullName, PartitionNumber = partitionNumber },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();
        return count;
    }

    private async Task EnsureAlterPermissionAsync(
        SqlConnection connection,
        string database,
        string schema,
        string table,
        string issueCode,
        List<PartitionSwitchIssue> blocking,
        List<PartitionSwitchIssue> warnings,
        CancellationToken cancellationToken)
    {
        try
        {
            var hasPermission = await HasAlterTablePermissionAsync(connection, database, schema, table, cancellationToken);
            if (!hasPermission)
            {
                blocking.Add(new PartitionSwitchIssue(
                    issueCode,
                    $"当前账户对表 {FormatQualifiedName(database, schema, table)} 缺少 ALTER 权限。",
                    "请为执行账户授予 ALTER TABLE 权限或使用具备权限的账号。"));
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "检查表 {Schema}.{Table} 权限失败", schema, table);
            warnings.Add(new PartitionSwitchIssue(
                "PermissionCheckFailed",
                $"未能确认表 {FormatQualifiedName(database, schema, table)} 的权限情况：{ex.Message}",
                "请手动验证执行账号是否拥有 ALTER TABLE 权限。"));
        }
    }

    private async Task CheckBlockingLocksAsync(
        SqlConnection connection,
        string database,
        string schema,
        string table,
        List<PartitionSwitchIssue> blocking,
        List<PartitionSwitchIssue> warnings,
        CancellationToken cancellationToken)
    {
        try
        {
            var hasLocks = await HasBlockingLocksAsync(connection, database, schema, table, cancellationToken);
            if (hasLocks)
            {
                blocking.Add(new PartitionSwitchIssue(
                    "SourceTableLocked",
                    $"源表 {FormatQualifiedName(database, schema, table)} 当前存在其他会话持有的锁。",
                    "请等待相关事务完成或联系数据库管理员释放锁资源。"));
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "检查表 {Schema}.{Table} 锁信息失败", schema, table);
            warnings.Add(new PartitionSwitchIssue(
                "LockCheckFailed",
                $"无法确认表 {FormatQualifiedName(database, schema, table)} 的锁状态：{ex.Message}",
                "如需继续，请手动确认无长事务占用该表。"));
        }
    }

    private async Task<bool> HasAlterTablePermissionAsync(SqlConnection connection, string database, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT CASE WHEN HAS_PERMS_BY_NAME(@ObjectName, 'OBJECT', 'ALTER') = 1 THEN 1 ELSE 0 END;";

        var objectName = BuildPermissionTarget(database, schema, table);

        var hasPermission = await sqlExecutor.QuerySingleAsync<int>(
            connection,
            sql,
            new { ObjectName = objectName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();
        return hasPermission == 1;
    }

    private async Task<bool> HasBlockingLocksAsync(SqlConnection connection, string database, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COUNT(1)
FROM sys.dm_tran_locks l
WHERE l.resource_associated_entity_id = OBJECT_ID(@FullName)
  AND l.request_session_id <> @@SPID
  AND l.request_status IN ('GRANT', 'CONVERT')
  AND l.resource_type IN ('OBJECT', 'PAGE', 'KEY', 'RID', 'HOBT');";

        var fullName = FormatQualifiedName(database, schema, table);
        var count = await sqlExecutor.QuerySingleAsync<int>(
            connection,
            sql,
            new { FullName = fullName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();
        return count > 0;
    }

    private async Task<bool> HasDmlTriggerAsync(SqlConnection connection, string database, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COUNT(1)
FROM sys.triggers t
WHERE t.parent_id = OBJECT_ID(@FullName)
  AND t.is_disabled = 0
  AND t.is_ms_shipped = 0;";

        var fullName = FormatQualifiedName(database, schema, table);
        var count = await sqlExecutor.QuerySingleAsync<int>(
            connection,
            sql,
            new { FullName = fullName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();
        return count > 0;
    }

    private async Task<int> CountForeignKeysAsync(SqlConnection connection, string database, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COUNT(1)
FROM sys.foreign_keys fk
WHERE fk.parent_object_id = OBJECT_ID(@FullName)
   OR fk.referenced_object_id = OBJECT_ID(@FullName);";

        var fullName = FormatQualifiedName(database, schema, table);
        var count = await sqlExecutor.QuerySingleAsync<int>(
            connection,
            sql,
            new { FullName = fullName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();
        return count;
    }

    private async Task<IReadOnlyList<TableIndexDefinition>> LoadIndexDefinitionsAsync(
        SqlConnection connection,
        string schema,
        string table,
        string partitionColumn,
        CancellationToken cancellationToken)
    {
        const string sql = @"SELECT
    i.index_id AS IndexId,
    i.name AS IndexName,
    UPPER(i.type_desc) AS IndexType,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    CASE WHEN kc.type IS NOT NULL AND kc.type <> 'PK' THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS IsUniqueConstraint,
    kc.name AS ConstraintName,
    kc.type AS ConstraintType,
    STUFF((
        SELECT ', ' + '[' + c.name + ']' + ' ' + CASE WHEN ic1.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END
        FROM sys.index_columns ic1
        INNER JOIN sys.columns c ON ic1.object_id = c.object_id AND ic1.column_id = c.column_id
        WHERE ic1.object_id = i.object_id
          AND ic1.index_id = i.index_id
          AND ic1.is_included_column = 0
        ORDER BY ic1.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), 1, 2, '') AS KeyColumns,
    STUFF((
        SELECT ', ' + '[' + c.name + ']'
        FROM sys.index_columns ic2
        INNER JOIN sys.columns c ON ic2.object_id = c.object_id AND ic2.column_id = c.column_id
        WHERE ic2.object_id = i.object_id
          AND ic2.index_id = i.index_id
          AND ic2.is_included_column = 1
        ORDER BY ic2.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), 1, 2, '') AS IncludedColumns,
    i.filter_definition AS FilterDefinition,
    CASE WHEN EXISTS (
        SELECT 1
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id
          AND ic.index_id = i.index_id
          AND ic.is_included_column = 0
          AND c.name = @PartitionColumn
    ) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS ContainsPartitionColumn
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
LEFT JOIN sys.key_constraints kc ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
WHERE SCHEMA_NAME(t.schema_id) = @SchemaName
  AND t.name = @TableName
  AND i.type IN (1, 2)
  AND i.is_disabled = 0
ORDER BY i.index_id;";

        var result = await sqlExecutor.QueryAsync<TableIndexDefinition>(
            connection,
            sql,
            new
            {
                SchemaName = schema,
                TableName = table,
                PartitionColumn = partitionColumn,
            },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();
        return result.ToList();
    }

    private async Task<IReadOnlyList<DefaultConstraintDefinition>> LoadDefaultConstraintsAsync(
        SqlConnection connection,
        string database,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string sql = @"SELECT
    c.name AS ColumnName,
    dc.name AS ConstraintName,
    OBJECT_DEFINITION(dc.object_id) AS Definition
FROM sys.default_constraints dc
INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE dc.parent_object_id = OBJECT_ID(@FullName)
  AND dc.is_ms_shipped = 0;";

        var fullName = FormatQualifiedName(database, schema, table);
        var result = await sqlExecutor.QueryAsync<DefaultConstraintRow>(
            connection,
            sql,
            new { FullName = fullName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();

        return result
            .Select(row => new DefaultConstraintDefinition(
                row.ColumnName,
                row.ConstraintName,
                row.Definition ?? string.Empty))
            .ToList();
    }

    private async Task<IReadOnlyList<CheckConstraintDefinition>> LoadCheckConstraintsAsync(
        SqlConnection connection,
        string database,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string sql = @"SELECT
    cc.name AS ConstraintName,
    OBJECT_DEFINITION(cc.object_id) AS Definition,
    cc.is_disabled AS IsDisabled,
    CASE WHEN cc.parent_column_id = 0 THEN NULL ELSE c.name END AS ColumnName
FROM sys.check_constraints cc
LEFT JOIN sys.columns c ON cc.parent_object_id = c.object_id AND cc.parent_column_id = c.column_id
WHERE cc.parent_object_id = OBJECT_ID(@FullName)
  AND cc.is_ms_shipped = 0;";

        var fullName = FormatQualifiedName(database, schema, table);
        var result = await sqlExecutor.QueryAsync<CheckConstraintRow>(
            connection,
            sql,
            new { FullName = fullName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();

        return result
            .Select(row => new CheckConstraintDefinition(
                row.ConstraintName,
                row.Definition ?? string.Empty,
                row.IsDisabled,
                row.ColumnName))
            .ToList();
    }

    private async Task<PartitionMetadataDefinition> LoadPartitionMetadataAsync(
        SqlConnection connection,
        string database,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        const string metadataSql = @"SELECT TOP 1
    ps.name AS PartitionSchemeName,
    pf.name AS PartitionFunctionName,
    pf.boundary_value_on_right AS BoundaryOnRight,
    c.name AS PartitionColumn
FROM sys.tables t
LEFT JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
LEFT JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
LEFT JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND ic.partition_ordinal > 0
LEFT JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE t.object_id = OBJECT_ID(@FullName);";

        var fullName = FormatQualifiedName(database, schema, table);

        var info = await sqlExecutor.QuerySingleAsync<PartitionInfoRow>(
            connection,
            metadataSql,
            new { FullName = fullName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();

        if (info is null || string.IsNullOrWhiteSpace(info.PartitionSchemeName) || string.IsNullOrWhiteSpace(info.PartitionFunctionName))
        {
            return PartitionMetadataDefinition.NotPartitioned;
        }

        const string boundariesSql = @"WITH PartitionInfo AS (
    SELECT
        p.partition_number,
        TYPE_NAME(pp.system_type_id) AS ValueType,
        ROW_NUMBER() OVER (ORDER BY p.partition_number) AS SortKey,
        prv.value AS BoundaryValue
    FROM sys.partitions p
    INNER JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
    INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
    INNER JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
    OUTER APPLY (
        SELECT prv_inner.value
        FROM sys.partition_range_values prv_inner
        WHERE prv_inner.function_id = pf.function_id
          AND prv_inner.boundary_id = CASE WHEN pf.boundary_value_on_right = 1 THEN p.partition_number - 1 ELSE p.partition_number END
    ) prv
    WHERE p.object_id = OBJECT_ID(@FullName) AND i.index_id IN (0, 1)
)
SELECT SortKey, BoundaryValue, ValueType FROM PartitionInfo ORDER BY SortKey;";

        var boundaryRows = (await sqlExecutor.QueryAsync<PartitionBoundaryRow>(
                connection,
                boundariesSql,
                new { FullName = fullName },
                transaction: null))
            .ToList();

        cancellationToken.ThrowIfCancellationRequested();

        var boundaryValues = new List<string>(boundaryRows.Count);
        foreach (var row in boundaryRows)
        {
            var partitionValue = CreatePartitionValue(row.BoundaryValue, row.ValueType);
            if (partitionValue is null)
            {
                continue;
            }

            boundaryValues.Add(partitionValue.ToInvariantString());
        }

        const string filegroupsSql = @"
SELECT
    dds.destination_id AS PartitionNumber,
    ds.name AS FilegroupName
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id
INNER JOIN sys.data_spaces ds ON dds.data_space_id = ds.data_space_id
WHERE t.object_id = OBJECT_ID(@FullName)
  AND i.index_id IN (0, 1)
ORDER BY dds.destination_id;";

        var filegroupRows = (await sqlExecutor.QueryAsync<PartitionFilegroupRow>(
                connection,
                filegroupsSql,
                new { FullName = fullName },
                transaction: null))
            .ToList();

        cancellationToken.ThrowIfCancellationRequested();

        var filegroupNames = filegroupRows.Select(row => row.FilegroupName).ToList();
        var partitionCount = filegroupNames.Count > 0 ? filegroupNames.Count : boundaryValues.Count + 1;
        var isRangeRight = info.BoundaryOnRight ?? true;

        return new PartitionMetadataDefinition(
            true,
            info.PartitionSchemeName!,
            info.PartitionFunctionName!,
            isRangeRight,
            info.PartitionColumn ?? string.Empty,
            boundaryValues,
            filegroupNames,
            partitionCount);
    }

    private static void EvaluatePartitionCompatibility(
        PartitionConfiguration configuration,
        string sourceDatabase,
        string? targetDatabase,
        string? targetSchema,
        string? targetTable,
        int requestedPartitionNumber,
        PartitionMetadataDefinition sourceMetadata,
        PartitionMetadataDefinition targetMetadata,
        bool targetExists,
        List<PartitionSwitchIssue> blocking)
    {
        var sourceQualifiedName = FormatQualifiedName(sourceDatabase, configuration.SchemaName, configuration.TableName);

        if (!sourceMetadata.IsPartitioned)
        {
            blocking.Add(new PartitionSwitchIssue(
                "SourceNotPartitioned",
                $"源表 {sourceQualifiedName} 未采用分区方案，无法执行 SWITCH 操作。",
                "请先为源表创建与配置一致的分区函数与分区方案。"));
            return;
        }

        if (!string.Equals(sourceMetadata.PartitionSchemeName, configuration.PartitionSchemeName, StringComparison.OrdinalIgnoreCase))
        {
            blocking.Add(new PartitionSwitchIssue(
                "SourcePartitionSchemeMismatch",
                $"源表 {sourceQualifiedName} 的分区方案为 {sourceMetadata.PartitionSchemeName}，与配置记录的 {configuration.PartitionSchemeName} 不一致。",
                "请同步配置或调整源表的分区方案后再执行 SWITCH。"));
        }

        if (!string.Equals(sourceMetadata.PartitionFunctionName, configuration.PartitionFunctionName, StringComparison.OrdinalIgnoreCase))
        {
            blocking.Add(new PartitionSwitchIssue(
                "SourcePartitionFunctionMismatch",
                $"源表 {sourceQualifiedName} 使用的分区函数为 {sourceMetadata.PartitionFunctionName}，与配置记录的 {configuration.PartitionFunctionName} 不一致。",
                "请同步配置或调整源表的分区函数使其保持一致。"));
        }

        if (!string.Equals(sourceMetadata.PartitionColumn, configuration.PartitionColumn.Name, StringComparison.OrdinalIgnoreCase))
        {
            blocking.Add(new PartitionSwitchIssue(
                "SourcePartitionColumnMismatch",
                $"源表 {sourceQualifiedName} 的分区列为 {sourceMetadata.PartitionColumn}，与配置记录的 {configuration.PartitionColumn.Name} 不一致。",
                "请确认配置中的分区列与实际表结构一致。"));
        }

        if (sourceMetadata.IsRangeRight != configuration.IsRangeRight)
        {
            var expected = configuration.IsRangeRight ? "RIGHT" : "LEFT";
            var actual = sourceMetadata.IsRangeRight ? "RIGHT" : "LEFT";
            blocking.Add(new PartitionSwitchIssue(
                "SourcePartitionRangeMismatch",
                $"源表 {sourceQualifiedName} 的分区函数为 RANGE {actual}，与配置记录的 RANGE {expected} 不一致。",
                "请确认分区函数的 Range 边界方向一致。"));
        }

        var configBoundaries = configuration.Boundaries
            .Select(boundary => boundary.Value.ToInvariantString())
            .ToList();

        // 注释掉分区边界严格检查 - 用户可能已经对源表进行过分区拆分/合并操作
        // 只要源表是分区表即可,不需要与初始配置完全一致
        /*
        if (!configBoundaries.SequenceEqual(sourceMetadata.Boundaries, StringComparer.Ordinal))
        {
            blocking.Add(new PartitionSwitchIssue(
                "SourcePartitionBoundariesMismatch",
                $"源表 {sourceQualifiedName} 的分区边界与配置记录不一致。",
                "请同步分区边界配置，确保两端分区数量与边界值完全一致。"));
        }

        var expectedPartitionCount = configBoundaries.Count + 1;
        if (expectedPartitionCount != sourceMetadata.PartitionCount)
        {
            blocking.Add(new PartitionSwitchIssue(
                "SourcePartitionCountMismatch",
                $"源表 {sourceQualifiedName} 实际包含 {sourceMetadata.PartitionCount} 个分区，与配置预期的 {expectedPartitionCount} 个不一致。",
                "请检查分区函数是否缺失边界或存在未同步的分区。"));
        }
        */

        if (requestedPartitionNumber > 0 && requestedPartitionNumber > sourceMetadata.PartitionCount)
        {
            blocking.Add(new PartitionSwitchIssue(
                "SourcePartitionOutOfRange",
                $"源表 {sourceQualifiedName} 仅包含 {sourceMetadata.PartitionCount} 个分区，无法定位到分区号 {requestedPartitionNumber}。",
                "请确认分区编号是否正确，或先同步分区结构。"));
        }

        if (!targetExists)
        {
            return;
        }

        var targetQualifiedName = FormatQualifiedName(targetDatabase ?? string.Empty, targetSchema ?? configuration.SchemaName, targetTable ?? configuration.TableName);

        if (!targetMetadata.IsPartitioned)
        {
            blocking.Add(new PartitionSwitchIssue(
                "TargetNotPartitioned",
                $"目标表 {targetQualifiedName} 未使用分区方案，无法与源表执行 SWITCH。",
                "请在目标表上应用与源表一致的分区函数和分区方案。"));
            return;
        }

        if (!string.Equals(targetMetadata.PartitionSchemeName, sourceMetadata.PartitionSchemeName, StringComparison.OrdinalIgnoreCase))
        {
            blocking.Add(new PartitionSwitchIssue(
                "TargetPartitionSchemeMismatch",
                $"目标表 {targetQualifiedName} 的分区方案为 {targetMetadata.PartitionSchemeName}，与源表的 {sourceMetadata.PartitionSchemeName} 不一致。",
                "请同步目标表的分区方案，使其与源表保持一致。"));
        }

        if (!string.Equals(targetMetadata.PartitionFunctionName, sourceMetadata.PartitionFunctionName, StringComparison.OrdinalIgnoreCase))
        {
            blocking.Add(new PartitionSwitchIssue(
                "TargetPartitionFunctionMismatch",
                $"目标表 {targetQualifiedName} 使用的分区函数为 {targetMetadata.PartitionFunctionName}，与源表的 {sourceMetadata.PartitionFunctionName} 不一致。",
                "请同步目标表的分区函数。"));
        }

        if (!string.Equals(targetMetadata.PartitionColumn, sourceMetadata.PartitionColumn, StringComparison.OrdinalIgnoreCase))
        {
            blocking.Add(new PartitionSwitchIssue(
                "TargetPartitionColumnMismatch",
                $"目标表 {targetQualifiedName} 的分区列为 {targetMetadata.PartitionColumn}，与源表的 {sourceMetadata.PartitionColumn} 不一致。",
                "请调整目标表分区列定义，与源表保持一致。"));
        }

        if (targetMetadata.IsRangeRight != sourceMetadata.IsRangeRight)
        {
            var sourceRange = sourceMetadata.IsRangeRight ? "RIGHT" : "LEFT";
            var targetRange = targetMetadata.IsRangeRight ? "RIGHT" : "LEFT";
            blocking.Add(new PartitionSwitchIssue(
                "TargetPartitionRangeMismatch",
                $"目标表 {targetQualifiedName} 的分区函数为 RANGE {targetRange}，与源表的 RANGE {sourceRange} 不一致。",
                "请确保目标表分区函数的 Range 方向与源表完全一致。"));
        }

        // 边界值一旦不一致，SWITCH 会因分区编号不匹配而失败
        if (!sourceMetadata.Boundaries.SequenceEqual(targetMetadata.Boundaries, StringComparer.Ordinal))
        {
            blocking.Add(new PartitionSwitchIssue(
                "TargetPartitionBoundariesMismatch",
                $"目标表 {targetQualifiedName} 的分区边界与源表不一致。",
                "请同步目标表的分区函数或重新创建目标表以保持分区边界一致。"));
        }

        if (requestedPartitionNumber > 0 && requestedPartitionNumber > targetMetadata.PartitionCount)
        {
            blocking.Add(new PartitionSwitchIssue(
                "TargetPartitionOutOfRange",
                $"目标表 {targetQualifiedName} 仅包含 {targetMetadata.PartitionCount} 个分区，无法接收编号为 {requestedPartitionNumber} 的分区数据。",
                "请扩展目标表的分区结构后再执行 SWITCH。"));
        }
    }

    private static void EvaluateIndexCompatibility(
        IReadOnlyList<TableIndexDefinition> sourceIndexes,
        IReadOnlyList<TableIndexDefinition> targetIndexes,
        List<PartitionSwitchIssue> blocking,
        List<PartitionSwitchAutoFixStep> autoFix,
        List<PartitionSwitchIssue> warnings)
    {
        bool hasIndexMismatch = false;
        
        var sourceClustered = sourceIndexes.FirstOrDefault(index => index.IsClustered);
        var targetClustered = targetIndexes.FirstOrDefault(index => index.IsClustered);

        if (sourceClustered is not null)
        {
            if (targetClustered is null)
            {
                hasIndexMismatch = true;
                // 可自动补齐的问题降级为警告,不阻塞流程
                warnings.Add(new PartitionSwitchIssue(
                    "TargetMissingClusteredIndex",
                    "目标表缺少与源表一致的聚集索引。",
                    "将通过自动补齐创建与源表相同的聚集索引。"));
            }
            else if (!MatchingIndexDefinition(sourceClustered, targetClustered))
            {
                hasIndexMismatch = true;
                blocking.Add(new PartitionSwitchIssue(
                    "ClusteredIndexMismatch",
                    "目标表的聚集索引与源表不一致。",
                    "请同步聚集索引的键列、排序方向及唯一性配置。"));
            }
        }
        else if (targetClustered is not null)
        {
            hasIndexMismatch = true;
            blocking.Add(new PartitionSwitchIssue(
                "ClusteredIndexMismatch",
                "目标表存在聚集索引，但源表未定义聚集索引。",
                "请确认两端的聚集索引策略保持一致后再执行 SWITCH。"));
        }

        var sourcePrimaryKey = sourceIndexes.FirstOrDefault(index => index.IsPrimaryKey);
        var targetPrimaryKey = targetIndexes.FirstOrDefault(index => index.IsPrimaryKey);

        if (sourcePrimaryKey is not null)
        {
            if (targetPrimaryKey is null)
            {
                hasIndexMismatch = true;
                // 可自动补齐的问题降级为警告
                warnings.Add(new PartitionSwitchIssue(
                    "TargetMissingPrimaryKey",
                    "目标表缺少与源表一致的主键约束。",
                    "将通过自动补齐创建与源表相同的主键约束。"));
            }
            else if (!MatchingIndexDefinition(sourcePrimaryKey, targetPrimaryKey))
            {
                hasIndexMismatch = true;
                blocking.Add(new PartitionSwitchIssue(
                    "PrimaryKeyMismatch",
                    "目标表的主键定义与源表不一致。",
                    "请同步主键约束涉及的列、排序方向及唯一性配置。"));
            }
        }
        else if (targetPrimaryKey is not null)
        {
            hasIndexMismatch = true;
            blocking.Add(new PartitionSwitchIssue(
                "PrimaryKeyMismatch",
                "目标表存在主键约束，但源表未定义主键。",
                "请确认两端主键策略保持一致或移除多余约束。"));
        }

        var sourceUniqueConstraints = sourceIndexes
            .Where(index => index.IsUniqueConstraint && !index.IsPrimaryKey)
            .ToList();
        var targetUniqueConstraints = targetIndexes
            .Where(index => index.IsUniqueConstraint && !index.IsPrimaryKey)
            .ToList();

        foreach (var sourceConstraint in sourceUniqueConstraints)
        {
            var match = targetUniqueConstraints.FirstOrDefault(target => MatchingIndexDefinition(sourceConstraint, target));
            if (match is null)
            {
                hasIndexMismatch = true;
                var name = sourceConstraint.ConstraintName ?? sourceConstraint.IndexName;
                // 可自动补齐的问题降级为警告
                warnings.Add(new PartitionSwitchIssue(
                    "UniqueConstraintMismatch",
                    $"目标表缺少与源表唯一约束（{name}）对应的定义。",
                    "将通过自动补齐创建相同列组合的唯一约束。"));
            }
            else
            {
                targetUniqueConstraints.Remove(match);
            }
        }

        var sourceUniqueIndexes = sourceIndexes
            .Where(index => index.IsUnique && !index.IsPrimaryKey && !index.IsUniqueConstraint)
            .ToList();
        var targetUniqueIndexes = targetIndexes
            .Where(index => index.IsUnique && !index.IsPrimaryKey && !index.IsUniqueConstraint)
            .ToList();

        foreach (var sourceIndex in sourceUniqueIndexes)
        {
            var match = targetUniqueIndexes.FirstOrDefault(target => MatchingIndexDefinition(sourceIndex, target));
            if (match is null)
            {
                // 可自动补齐的问题降级为警告
                warnings.Add(new PartitionSwitchIssue(
                    "UniqueIndexMismatch",
                    $"目标表缺少与源表唯一索引 {sourceIndex.IndexName} 对应的定义。",
                    "将通过自动补齐同步唯一索引的键列、INCLUDE列以及筛选条件。"));
            }
            else
            {
                targetUniqueIndexes.Remove(match);
            }
        }
        
        // 检查所有非聚集非唯一索引(普通索引)
        var sourceNonClusteredIndexes = sourceIndexes
            .Where(index => !index.IsClustered && !index.IsUnique && !index.IsPrimaryKey && !index.IsUniqueConstraint)
            .ToList();
        var targetNonClusteredIndexes = targetIndexes
            .Where(index => !index.IsClustered && !index.IsUnique && !index.IsPrimaryKey && !index.IsUniqueConstraint)
            .ToList();

        foreach (var sourceIndex in sourceNonClusteredIndexes)
        {
            var match = targetNonClusteredIndexes.FirstOrDefault(target => MatchingIndexDefinition(sourceIndex, target));
            if (match is null)
            {
                hasIndexMismatch = true;
                // 可自动补齐的问题降级为警告
                warnings.Add(new PartitionSwitchIssue(
                    "NonClusteredIndexMismatch",
                    $"目标表缺少与源表非聚集索引 {sourceIndex.IndexName} 对应的定义。",
                    "将通过自动补齐同步非聚集索引的键列、INCLUDE列以及筛选条件。"));
            }
            else
            {
                targetNonClusteredIndexes.Remove(match);
            }
        }

        // 检查目标表是否有多余的索引
        if (targetNonClusteredIndexes.Any())
        {
            hasIndexMismatch = true;
            var extraIndexNames = string.Join(", ", targetNonClusteredIndexes.Select(idx => idx.IndexName));
            warnings.Add(new PartitionSwitchIssue(
                "ExtraIndexesOnTarget",
                $"目标表存在源表未定义的非聚集索引: {extraIndexNames}。",
                "这些多余的索引不影响SWITCH操作,但建议保持索引一致性。"));
        }
        
        // 如果发现索引不匹配,添加自动补齐步骤
        if (hasIndexMismatch)
        {
            autoFix.Add(new PartitionSwitchAutoFixStep(
                "SyncIndexes",
                "自动从源表复制缺失的索引到目标表。",
                "将创建与源表完全相同的索引结构(包括聚集索引、主键、唯一约束、唯一索引和非聚集索引),确保 SWITCH 操作的兼容性。"));
        }
    }

    private static bool EvaluateDefaultConstraintCompatibility(
        IReadOnlyList<DefaultConstraintDefinition> sourceConstraints,
        IReadOnlyList<DefaultConstraintDefinition> targetConstraints,
        List<PartitionSwitchIssue> blocking,
        List<PartitionSwitchAutoFixStep> autoFix,
        List<PartitionSwitchIssue> warnings)
    {
        bool hasConstraintMismatch = false;
        
        // 以列名映射目标表默认约束，便于快速定位缺失或差异
        var targetByColumn = targetConstraints
            .GroupBy(constraint => constraint.ColumnName, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First(), StringComparer.OrdinalIgnoreCase);

        foreach (var sourceConstraint in sourceConstraints)
        {
            if (!targetByColumn.TryGetValue(sourceConstraint.ColumnName, out var targetConstraint))
            {
                hasConstraintMismatch = true;
                // 可自动补齐的问题降级为警告
                warnings.Add(new PartitionSwitchIssue(
                    "TargetMissingDefaultConstraint",
                    $"目标表列 {sourceConstraint.ColumnName} 缺少默认约束{FormatConstraintName(sourceConstraint.ConstraintName)}。",
                    "将通过自动补齐创建与源表一致的 DEFAULT 约束。"));
                continue;
            }

            if (!string.Equals(
                    NormalizeConstraintDefinition(sourceConstraint.Definition),
                    NormalizeConstraintDefinition(targetConstraint.Definition),
                    StringComparison.Ordinal))
            {
                hasConstraintMismatch = true;
                blocking.Add(new PartitionSwitchIssue(
                    "DefaultConstraintMismatch",
                    $"目标表列 {sourceConstraint.ColumnName} 的默认约束定义与源表不一致。",
                    "请同步 DEFAULT 约束的表达式或移除差异后重试。"));
            }
        }

        var sourceColumns = new HashSet<string>(sourceConstraints.Select(constraint => constraint.ColumnName), StringComparer.OrdinalIgnoreCase);
        foreach (var targetConstraint in targetConstraints)
        {
            if (!sourceColumns.Contains(targetConstraint.ColumnName))
            {
                hasConstraintMismatch = true;
                blocking.Add(new PartitionSwitchIssue(
                    "ExtraDefaultConstraintOnTarget",
                    $"目标表列 {targetConstraint.ColumnName} 存在源表未定义的默认约束{FormatConstraintName(targetConstraint.ConstraintName)}。",
                    "请移除或禁用多余的 DEFAULT 约束以保持结构一致。"));
            }
        }
        
        // 约束自动补齐会在检查约束评估后统一添加
        // hasConstraintMismatch会被检查约束方法使用
        return hasConstraintMismatch;
    }

    private static void EvaluateCheckConstraintCompatibility(
        IReadOnlyList<CheckConstraintDefinition> sourceConstraints,
        IReadOnlyList<CheckConstraintDefinition> targetConstraints,
        List<PartitionSwitchIssue> blocking,
        List<PartitionSwitchAutoFixStep> autoFix,
        bool hasDefaultMismatch,
        List<PartitionSwitchIssue> warnings)
    {
        bool hasCheckMismatch = false;
        
        // 通过规范化定义将检查约束成组，比对逻辑表达式是否一致
        var targetByDefinition = targetConstraints
            .GroupBy(constraint => NormalizeConstraintDefinition(constraint.Definition))
            .ToDictionary(group => group.Key, group => new Queue<CheckConstraintDefinition>(group), StringComparer.Ordinal);

        foreach (var sourceConstraint in sourceConstraints)
        {
            var key = NormalizeConstraintDefinition(sourceConstraint.Definition);
            if (!targetByDefinition.TryGetValue(key, out var candidates) || candidates.Count == 0)
            {
                hasCheckMismatch = true;
                var message = sourceConstraint.ColumnName is null
                    ? $"目标表缺少与源表检查约束 {sourceConstraint.ConstraintName} 对应的逻辑。"
                    : $"目标表缺少作用于列 {sourceConstraint.ColumnName} 的检查约束 {sourceConstraint.ConstraintName}。";

                // 可自动补齐的问题降级为警告
                warnings.Add(new PartitionSwitchIssue(
                    "TargetMissingCheckConstraint",
                    message,
                    "将通过自动补齐创建相同表达式的 CHECK 约束。"));
                continue;
            }

            var targetConstraint = candidates.Dequeue();
            if (targetConstraint.IsDisabled != sourceConstraint.IsDisabled)
            {
                hasCheckMismatch = true;
                blocking.Add(new PartitionSwitchIssue(
                    "CheckConstraintMismatch",
                    $"检查约束 {sourceConstraint.ConstraintName} 的启用状态在源表和目标表之间不一致。",
                    "请确认双方检查约束均处于相同启用状态。"));
            }
        }

        foreach (var remaining in targetByDefinition.Values.SelectMany(queue => queue))
        {
            hasCheckMismatch = true;
            var message = remaining.ColumnName is null
                ? $"目标表存在源表未定义的检查约束 {remaining.ConstraintName}。"
                : $"目标表列 {remaining.ColumnName} 存在源表未定义的检查约束 {remaining.ConstraintName}。";

            blocking.Add(new PartitionSwitchIssue(
                "ExtraCheckConstraintOnTarget",
                message,
                "请移除多余的 CHECK 约束或在源表上保持一致。"));
        }
        
        // 如果发现默认约束或检查约束不匹配,添加自动补齐步骤
        if (hasCheckMismatch || hasDefaultMismatch)
        {
            autoFix.Add(new PartitionSwitchAutoFixStep(
                "SyncConstraints",
                "自动从源表复制缺失的DEFAULT约束和CHECK约束到目标表。",
                "将创建与源表完全相同的约束定义,确保数据完整性规则一致。"));
        }
    }

    private static bool MatchingIndexDefinition(TableIndexDefinition source, TableIndexDefinition target)
    {
        if (!string.Equals(NormalizeKeyColumns(source.KeyColumns), NormalizeKeyColumns(target.KeyColumns), StringComparison.Ordinal))
        {
            return false;
        }

        if (!string.Equals(NormalizeIncludeColumns(source.IncludedColumns), NormalizeIncludeColumns(target.IncludedColumns), StringComparison.Ordinal))
        {
            return false;
        }

        if (!string.Equals(NormalizeFilterDefinition(source.FilterDefinition), NormalizeFilterDefinition(target.FilterDefinition), StringComparison.Ordinal))
        {
            return false;
        }

        if (source.IsUnique != target.IsUnique)
        {
            return false;
        }

        if (source.IsPrimaryKey != target.IsPrimaryKey)
        {
            return false;
        }

        if (source.IsUniqueConstraint != target.IsUniqueConstraint)
        {
            return false;
        }

        if (!string.Equals(source.IndexType, target.IndexType, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return true;
    }

    private static string NormalizeKeyColumns(string? keyColumns)
    {
        if (string.IsNullOrWhiteSpace(keyColumns))
        {
            return string.Empty;
        }

        var normalized = keyColumns
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(part =>
            {
                var tokens = part.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (tokens.Length == 0)
                {
                    return string.Empty;
                }

                var columnName = tokens[0].Trim('[', ']').ToUpperInvariant();
                var direction = tokens.Length > 1 ? tokens[1].ToUpperInvariant() : "ASC";
                return $"{columnName} {direction}";
            });

        return string.Join(',', normalized);
    }

    private static string NormalizeIncludeColumns(string? includedColumns)
    {
        if (string.IsNullOrWhiteSpace(includedColumns))
        {
            return string.Empty;
        }

        var normalized = includedColumns
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(column => column.Trim().Trim('[', ']').ToUpperInvariant())
            .OrderBy(column => column, StringComparer.Ordinal)
            .ToArray();

        return string.Join(',', normalized);
    }

    private static string NormalizeFilterDefinition(string? filterDefinition)
    {
        return string.IsNullOrWhiteSpace(filterDefinition)
            ? string.Empty
            : filterDefinition.Trim().ToUpperInvariant();
    }

    private static string NormalizeConstraintDefinition(string? definition)
    {
        if (string.IsNullOrWhiteSpace(definition))
        {
            return string.Empty;
        }

        var normalized = definition.Trim();

        while (normalized.StartsWith("(", StringComparison.Ordinal) && normalized.EndsWith(")", StringComparison.Ordinal) && normalized.Length >= 2)
        {
            normalized = normalized.Substring(1, normalized.Length - 2).Trim();
        }

        normalized = normalized
            .Replace(" ", string.Empty, StringComparison.Ordinal)
            .Replace("\r", string.Empty, StringComparison.Ordinal)
            .Replace("\n", string.Empty, StringComparison.Ordinal);

        return normalized.ToUpperInvariant();
    }

    private static string FormatConstraintName(string? constraintName)
        => string.IsNullOrWhiteSpace(constraintName)
            ? string.Empty
            : $" [{constraintName}]";

    private static PartitionValue? CreatePartitionValue(object? rawValue, string valueType)
    {
        if (rawValue is null || rawValue is DBNull)
        {
            return null;
        }

        var normalizedType = valueType.ToLowerInvariant();
        return normalizedType switch
        {
            "int" => PartitionValue.FromInt(Convert.ToInt32(rawValue, CultureInfo.InvariantCulture)),
            "bigint" => PartitionValue.FromBigInt(Convert.ToInt64(rawValue, CultureInfo.InvariantCulture)),
            "date" => PartitionValue.FromDate(DateOnly.FromDateTime(Convert.ToDateTime(rawValue, CultureInfo.InvariantCulture))),
            "datetime" => PartitionValue.FromDateTime(Convert.ToDateTime(rawValue, CultureInfo.InvariantCulture)),
            "datetime2" => PartitionValue.FromDateTime2(Convert.ToDateTime(rawValue, CultureInfo.InvariantCulture)),
            "smalldatetime" => PartitionValue.FromDateTime(Convert.ToDateTime(rawValue, CultureInfo.InvariantCulture)),
            "uniqueidentifier" => PartitionValue.FromGuid((Guid)rawValue),
            _ => PartitionValue.FromString(Convert.ToString(rawValue, CultureInfo.InvariantCulture) ?? string.Empty),
        };
    }

    private static void EvaluateColumnCompatibility(
        PartitionSwitchTableInfo source,
        PartitionSwitchTableInfo target,
        List<PartitionSwitchIssue> blocking)
    {
        if (source.Columns.Count != target.Columns.Count)
        {
            blocking.Add(new PartitionSwitchIssue(
                "ColumnCountMismatch",
                $"源表与目标表的列数量不一致（源 {source.Columns.Count} 列，目标 {target.Columns.Count} 列）。",
                "请确保目标表与源表使用完全一致的列定义。"));
            return;
        }

        for (var index = 0; index < source.Columns.Count; index++)
        {
            var sourceColumn = source.Columns[index];
            var targetColumn = target.Columns[index];

            if (!string.Equals(sourceColumn.Name, targetColumn.Name, StringComparison.OrdinalIgnoreCase))
            {
                blocking.Add(new PartitionSwitchIssue(
                    "ColumnNameMismatch",
                    $"第 {index + 1} 列名称不一致：源为 {sourceColumn.Name}，目标为 {targetColumn.Name}。",
                    "请调整目标表列名称顺序，使其与源表完全一致。"));
                break;
            }

            if (!string.Equals(sourceColumn.DataType, targetColumn.DataType, StringComparison.OrdinalIgnoreCase) ||
                Normalize(sourceColumn.MaxLength) != Normalize(targetColumn.MaxLength) ||
                Normalize(sourceColumn.Precision) != Normalize(targetColumn.Precision) ||
                Normalize(sourceColumn.Scale) != Normalize(targetColumn.Scale))
            {
                blocking.Add(new PartitionSwitchIssue(
                    "ColumnTypeMismatch",
                    $"列 {sourceColumn.Name} 的数据类型或长度与目标表不一致。",
                    "请保持两端的数据类型、精度、标度一致，否则无法执行 SWITCH。"));
                break;
            }

            if (sourceColumn.IsNullable != targetColumn.IsNullable)
            {
                blocking.Add(new PartitionSwitchIssue(
                    "ColumnNullabilityMismatch",
                    $"列 {sourceColumn.Name} 的可空性不同。",
                    "请确保目标表与源表的可空性完全一致。"));
                break;
            }

            if (sourceColumn.IsIdentity != targetColumn.IsIdentity)
            {
                blocking.Add(new PartitionSwitchIssue(
                    "IdentityMismatch",
                    $"列 {sourceColumn.Name} 的标识列属性不一致。",
                    "请确认目标表是否需要包含相同的 IDENTITY 定义。"));
                break;
            }

            if (sourceColumn.IsComputed != targetColumn.IsComputed)
            {
                blocking.Add(new PartitionSwitchIssue(
                    "ComputedColumnMismatch",
                    $"列 {sourceColumn.Name} 的计算列属性不一致。",
                    "请确保目标表计算列定义与源表一致。"));
                break;
            }
        }
    }

    private static int? NormalizeMaxLength(string dataType, short maxLength)
    {
        // 对于 nvarchar/nchar 等类型，max_length 为字节长度，需要换算为字符数。
        return dataType.Equals("nvarchar", StringComparison.OrdinalIgnoreCase)
            || dataType.Equals("nchar", StringComparison.OrdinalIgnoreCase)
            ? maxLength / 2
            : maxLength;
    }

    private static int? Normalize(int? value) => value == 0 ? null : value;
    private static byte? Normalize(byte? value) => value == 0 ? null : value;

    private static string FormatQualifiedName(string database, string schema, string table)
        => string.IsNullOrWhiteSpace(database)
            ? $"[{schema}].[{table}]"
            : $"[{database}].[{schema}].[{table}]";

    private static string BuildPermissionTarget(string database, string schema, string table)
        => string.IsNullOrWhiteSpace(database)
            ? $"{schema}.{table}"
            : $"{database}.{schema}.{table}";

    private sealed class ColumnRow
    {
        public int ColumnId { get; set; }
        public string ColumnName { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public short MaxLength { get; set; }
        public byte Precision { get; set; }
        public int Scale { get; set; }
        public bool IsNullable { get; set; }
        public bool IsIdentity { get; set; }
        public bool IsComputed { get; set; }
    }

    private sealed record DefaultConstraintRow(string ColumnName, string? ConstraintName, string? Definition);

    private sealed record CheckConstraintRow(string ConstraintName, string? Definition, bool IsDisabled, string? ColumnName);

    private sealed record DefaultConstraintDefinition(string ColumnName, string? ConstraintName, string Definition);

    private sealed record CheckConstraintDefinition(string ConstraintName, string Definition, bool IsDisabled, string? ColumnName);

    private sealed record PartitionMetadataDefinition(
        bool IsPartitioned,
        string PartitionSchemeName,
        string PartitionFunctionName,
        bool IsRangeRight,
        string PartitionColumn,
        IReadOnlyList<string> Boundaries,
        IReadOnlyList<string> Filegroups,
        int PartitionCount)
    {
        public static PartitionMetadataDefinition NotPartitioned { get; } = new(false, string.Empty, string.Empty, true, string.Empty, Array.Empty<string>(), Array.Empty<string>(), 0);
    }

    private sealed class PartitionInfoRow
    {
        public string? PartitionSchemeName { get; set; }
        public string? PartitionFunctionName { get; set; }
        public bool? BoundaryOnRight { get; set; }
        public string? PartitionColumn { get; set; }
    }

    private sealed class PartitionBoundaryRow
    {
        public int SortKey { get; set; }
        public object? BoundaryValue { get; set; }
        public string ValueType { get; set; } = string.Empty;
    }

    private sealed class PartitionFilegroupRow
    {
        public int PartitionNumber { get; set; }
        public string FilegroupName { get; set; } = string.Empty;
    }
    
    /// <summary>
    /// 检查源表和目标表的索引是否对齐到分区方案
    /// </summary>
    private async Task EvaluateIndexPartitionAlignmentAsync(
        SqlConnection sourceConnection,
        SqlConnection targetConnection,
        string sourceDatabase,
        string? targetDatabase,
        string sourceSchema,
        string sourceTable,
        string? targetSchema,
        string? targetTable,
        bool targetExists,
        string partitionSchemeName,
        List<PartitionSwitchIssue> blocking,
        List<PartitionSwitchAutoFixStep> autoFix,
        List<PartitionSwitchIssue> warnings,
        CancellationToken cancellationToken)
    {
        // 检查源表索引是否对齐
        var sourceUnalignedIndexes = await GetUnalignedIndexesAsync(
            sourceConnection,
            sourceDatabase,
            sourceSchema,
            sourceTable,
            partitionSchemeName,
            cancellationToken);

        if (sourceUnalignedIndexes.Any())
        {
            var indexList = string.Join(", ", sourceUnalignedIndexes.Select(idx => idx.IndexName));
            warnings.Add(new PartitionSwitchIssue(
                "SourceIndexNotAligned",
                $"源表 [{sourceSchema}].[{sourceTable}] 的以下索引未对齐到分区方案 {partitionSchemeName}: {indexList}",
                "可以使用\"对齐源表索引\"自动补齐功能,系统将自动删除并重建这些索引。"));

            autoFix.Add(new PartitionSwitchAutoFixStep(
                "AlignSourceIndexes",
                $"对齐源表索引(共{sourceUnalignedIndexes.Count}个未对齐索引)",
                $"将删除并重建源表的未对齐索引,使其包含分区列并对齐到分区方案 {partitionSchemeName}。"));
        }

        // 如果目标表存在,也检查目标表索引对齐
        if (targetExists && !string.IsNullOrWhiteSpace(targetTable))
        {
            var actualTargetSchema = targetSchema ?? sourceSchema;
            var targetUnalignedIndexes = await GetUnalignedIndexesAsync(
                targetConnection,
                targetDatabase ?? sourceDatabase,
                actualTargetSchema,
                targetTable,
                partitionSchemeName,
                cancellationToken);

            if (targetUnalignedIndexes.Any())
            {
                var indexList = string.Join(", ", targetUnalignedIndexes.Select(idx => idx.IndexName));
                warnings.Add(new PartitionSwitchIssue(
                    "TargetIndexNotAligned",
                    $"目标表 [{actualTargetSchema}].[{targetTable}] 的以下索引未对齐到分区方案 {partitionSchemeName}: {indexList}",
                    "可以使用\"对齐目标表索引\"自动补齐功能,系统将自动删除并重建这些索引。"));

                autoFix.Add(new PartitionSwitchAutoFixStep(
                    "AlignTargetIndexes",
                    $"对齐目标表索引(共{targetUnalignedIndexes.Count}个未对齐索引)",
                    $"将删除并重建目标表的未对齐索引,使其包含分区列并对齐到分区方案 {partitionSchemeName}。"));
            }
        }
    }

    /// <summary>
    /// 获取表中未对齐到分区方案的索引列表
    /// </summary>
    private async Task<List<UnalignedIndexInfo>> GetUnalignedIndexesAsync(
        SqlConnection connection,
        string database,
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
    CASE WHEN ds.type = 'PS' THEN 1 ELSE 0 END AS IsPartitioned,
    ds.name AS DataSpaceName,
    c.name AS PartitionColumnName
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
LEFT JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND ic.partition_ordinal > 0
LEFT JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE s.name = @Schema
    AND t.name = @Table
    AND i.type > 0  -- 排除堆
    AND (
        -- 索引不在分区方案上
        ds.type != 'PS'
        -- 或者索引在分区方案上但不是目标分区方案
        OR (ds.type = 'PS' AND ds.name != @PartitionSchemeName)
        -- 或者索引在分区方案上但没有分区列
        OR (ds.type = 'PS' AND ic.partition_ordinal IS NULL)
    )
ORDER BY i.index_id;";

        var parameters = new[]
        {
            new SqlParameter("@Schema", schema),
            new SqlParameter("@Table", table),
            new SqlParameter("@PartitionSchemeName", partitionSchemeName)
        };

        var result = new List<UnalignedIndexInfo>();
        
        try
        {
            if (!string.IsNullOrWhiteSpace(database))
            {
                await connection.ChangeDatabaseAsync(database, cancellationToken);
            }

            await using var command = new SqlCommand(sql, connection);
            command.Parameters.AddRange(parameters);
            
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                result.Add(new UnalignedIndexInfo
                {
                    IndexName = reader.GetString(0),
                    IndexType = reader.GetString(1),
                    IsUnique = reader.GetBoolean(2),
                    IsPrimaryKey = reader.GetBoolean(3),
                    IsPartitioned = reader.GetInt32(4) == 1,
                    DataSpaceName = reader.IsDBNull(5) ? null : reader.GetString(5),
                    PartitionColumnName = reader.IsDBNull(6) ? null : reader.GetString(6)
                });
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "检查表 [{Schema}].[{Table}] 索引对齐状态时发生异常", schema, table);
        }

        return result;
    }

    private sealed class UnalignedIndexInfo
    {
        public string IndexName { get; set; } = string.Empty;
        public string IndexType { get; set; } = string.Empty;
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsPartitioned { get; set; }
        public string? DataSpaceName { get; set; }
        public string? PartitionColumnName { get; set; }
    }
}
