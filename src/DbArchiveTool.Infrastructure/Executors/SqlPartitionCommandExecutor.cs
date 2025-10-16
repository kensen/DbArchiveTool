using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.Models;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 负责渲染 SQL 模板并调用数据库执行 DDL 的执行器。
/// </summary>
internal sealed class SqlPartitionCommandExecutor
{
    private readonly ISqlTemplateProvider templateProvider;
    private readonly ISqlExecutor sqlExecutor;
    private readonly IDbConnectionFactory connectionFactory;
    private readonly ILogger<SqlPartitionCommandExecutor> logger;

    public SqlPartitionCommandExecutor(
        ISqlTemplateProvider templateProvider,
        ISqlExecutor sqlExecutor,
        IDbConnectionFactory connectionFactory,
        ILogger<SqlPartitionCommandExecutor> logger)
    {
        this.templateProvider = templateProvider;
        this.sqlExecutor = sqlExecutor;
        this.connectionFactory = connectionFactory;
        this.logger = logger;
    }

    public string RenderSplitScript(PartitionConfiguration configuration, IReadOnlyList<PartitionValue> newBoundaries)
    {
        var template = templateProvider.GetTemplate("SplitRange.sql");
        var builder = new StringBuilder();
        var filegroup = ResolveDefaultFilegroup(configuration);

        builder.AppendLine("-- 分区拆分脚本");
        builder.AppendLine($"-- 目标表: {configuration.SchemaName}.{configuration.TableName}");
        builder.AppendLine($"-- 分区函数: {configuration.PartitionFunctionName}");
        builder.AppendLine($"-- 分区方案: {configuration.PartitionSchemeName}");
        builder.AppendLine($"-- 新增边界数量: {newBoundaries.Count}");
        builder.AppendLine($"-- 生成时间: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        builder.AppendLine();

        foreach (var boundary in newBoundaries.Select((value, index) => new { value, index }))
        {
            builder.AppendLine($"-- 边界 #{boundary.index + 1}: {boundary.value.ToLiteral()}");
            builder.AppendLine(template
                .Replace("{PartitionScheme}", configuration.PartitionSchemeName)
                .Replace("{FilegroupName}", filegroup)
                .Replace("{PartitionFunction}", configuration.PartitionFunctionName)
                .Replace("{BoundaryLiteral}", boundary.value.ToLiteral()));
            builder.AppendLine();
        }

        logger.LogDebug("Split script generated for {Schema}.{Table} with {Count} boundaries", configuration.SchemaName, configuration.TableName, newBoundaries.Count);
        return builder.ToString();
    }

    public string RenderMergeScript(PartitionConfiguration configuration, string boundaryKey)
    {
        var boundary = configuration.Boundaries.FirstOrDefault(x => x.SortKey.Equals(boundaryKey, StringComparison.Ordinal));
        if (boundary is null)
        {
            throw new InvalidOperationException("未找到指定的分区边界，无法生成 MERGE 脚本。");
        }

        var template = templateProvider.GetTemplate("MergeRange.sql");
        var script = template
            .Replace("{PartitionFunction}", configuration.PartitionFunctionName)
            .Replace("{BoundaryLiteral}", boundary.Value.ToLiteral());

        logger.LogDebug("Merge script generated for {Schema}.{Table} at boundary {Boundary}", configuration.SchemaName, configuration.TableName, boundaryKey);
        return script;
    }

    public string RenderSwitchOutScript(PartitionConfiguration configuration, SwitchPayload payload)
    {
        if (string.IsNullOrWhiteSpace(payload.TargetSchema))
        {
            throw new InvalidOperationException("目标架构不能为空。");
        }

        var template = templateProvider.GetTemplate("SwitchOut.sql");
        var script = template
            .Replace("{SourceSchema}", configuration.SchemaName)
            .Replace("{SourceTable}", configuration.TableName)
            .Replace("{TargetSchema}", payload.TargetSchema)
            .Replace("{TargetTable}", payload.TargetTable)
            .Replace("{SourcePartitionNumber}", payload.SourcePartitionKey)
            .Replace("{TargetPartitionNumber}", payload.SourcePartitionKey);

        logger.LogDebug("Switch script generated for {Schema}.{Table} to {TargetSchema}.{TargetTable}", configuration.SchemaName, configuration.TableName, payload.TargetSchema, payload.TargetTable);
        return script;
    }

    /// <summary>
    /// 如果指定文件组不存在则创建。
    /// </summary>
    /// <returns><see cref="PartitionConversionResult"/>，包含是否执行转换及索引操作明细。</returns>
    /// <param name="dataSourceId">数据源标识</param>
    /// <param name="databaseName">数据库名称</param>
    /// <param name="filegroupName">文件组名称</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>操作结果（true=新建，false=已存在）</returns>
    public async Task<bool> CreateFilegroupIfNeededAsync(
        Guid dataSourceId,
        string databaseName,
        string filegroupName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);

            // 检查文件组是否已存在
            var checkSql = @"
                SELECT COUNT(1) 
                FROM sys.filegroups 
                WHERE name = @FilegroupName";

            var exists = await sqlExecutor.QuerySingleAsync<int>(
                connection,
                checkSql,
                new { FilegroupName = filegroupName });

            if (exists > 0)
            {
                logger.LogDebug("文件组 {Filegroup} 已存在，跳过创建。", filegroupName);
                return false;
            }

            // 创建文件组
            var template = templateProvider.GetTemplate("CreateFilegroup.sql");
            var createSql = template
                .Replace("{DatabaseName}", databaseName)
                .Replace("{FilegroupName}", $"[{filegroupName}]");

            await sqlExecutor.ExecuteAsync(connection, createSql, timeoutSeconds: 60);

            logger.LogInformation("成功创建文件组：{Filegroup}", filegroupName);
            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "创建文件组 {Filegroup} 失败。", filegroupName);
            throw;
        }
    }

    /// <summary>
    /// 添加数据文件到指定文件组。
    /// </summary>
    public async Task AddDataFileAsync(
        Guid dataSourceId,
        string databaseName,
        string filegroupName,
        string fileName,
        string filePath,
        int initialSizeMB = 100,
        int growthSizeMB = 50,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);

            var sanitizedFileName = fileName.Replace("'", "''", StringComparison.Ordinal);
            var sanitizedFilePath = filePath.Replace("'", "''", StringComparison.Ordinal);

            var template = templateProvider.GetTemplate("AddDataFile.sql");
            var sql = template
                .Replace("{DatabaseName}", databaseName)
                .Replace("{FileName}", sanitizedFileName)
                .Replace("{FilePath}", sanitizedFilePath)
                .Replace("{InitialSizeMB}", initialSizeMB.ToString())
                .Replace("{GrowthSizeMB}", growthSizeMB.ToString())
                .Replace("{FilegroupName}", $"[{filegroupName}]");

            await sqlExecutor.ExecuteAsync(connection, sql, timeoutSeconds: 120);

            logger.LogInformation(
                "成功添加数据文件：{FileName} ({FilePath}) 到文件组 {Filegroup}",
                fileName, filePath, filegroupName);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "添加数据文件 {FileName} 到文件组 {Filegroup} 失败。", fileName, filegroupName);
            throw;
        }
    }

    /// <summary>
    /// 确保为单文件组策略创建数据文件。
    /// </summary>
    /// <param name="dataSourceId">数据源标识。</param>
    /// <param name="databaseName">数据库名称。</param>
    /// <param name="storageSettings">分区存储设置。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>返回 true 表示已新建数据文件。</returns>
    public async Task<bool> CreateDataFileIfNeededAsync(
        Guid dataSourceId,
        string databaseName,
        PartitionStorageSettings storageSettings,
        CancellationToken cancellationToken = default)
    {
        if (storageSettings.Mode != PartitionStorageMode.DedicatedFilegroupSingleFile)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(storageSettings.DataFileDirectory) ||
            string.IsNullOrWhiteSpace(storageSettings.DataFileName))
        {
            throw new InvalidOperationException("分区配置缺少数据文件目录或文件名，无法创建文件。");
        }

        var initialSize = storageSettings.InitialSizeMb ?? throw new InvalidOperationException("分区配置缺少数据文件初始大小。");
        var autoGrowth = storageSettings.AutoGrowthMb ?? throw new InvalidOperationException("分区配置缺少数据文件自动增长设置。");

        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);

            const string checkSql = @"
                SELECT COUNT(1)
                FROM sys.database_files
                WHERE name = @FileName";

            var exists = await sqlExecutor.QuerySingleAsync<int>(
                connection,
                checkSql,
                new { FileName = storageSettings.DataFileName });

            if (exists > 0)
            {
                logger.LogDebug("数据文件 {FileName} 已存在，跳过创建。", storageSettings.DataFileName);
                return false;
            }

            var fullPath = Path.Combine(storageSettings.DataFileDirectory, storageSettings.DataFileName);
            var sanitizedFileName = storageSettings.DataFileName.Replace("'", "''", StringComparison.Ordinal);
            var sanitizedFilePath = fullPath.Replace("'", "''", StringComparison.Ordinal);

            var template = templateProvider.GetTemplate("AddDataFile.sql");
            var sql = template
                .Replace("{DatabaseName}", databaseName)
                .Replace("{FileName}", sanitizedFileName)
                .Replace("{FilePath}", sanitizedFilePath)
                .Replace("{InitialSizeMB}", initialSize.ToString(CultureInfo.InvariantCulture))
                .Replace("{GrowthSizeMB}", autoGrowth.ToString(CultureInfo.InvariantCulture))
                .Replace("{FilegroupName}", $"[{storageSettings.FilegroupName}]");

            await sqlExecutor.ExecuteAsync(connection, sql, timeoutSeconds: 180);

            logger.LogInformation(
                "成功创建数据文件：{FileName} ({FilePath})，归属文件组 {Filegroup}",
                storageSettings.DataFileName,
                fullPath,
                storageSettings.FilegroupName);

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "创建数据文件 {FileName} 到文件组 {Filegroup} 失败。", storageSettings.DataFileName, storageSettings.FilegroupName);
            throw;
        }
    }

    /// <summary>
    /// 检查分区函数是否存在。
    /// </summary>
    public async Task<bool> CheckPartitionFunctionExistsAsync(
        Guid dataSourceId,
        string partitionFunctionName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);

            var checkSql = @"
                SELECT COUNT(1) 
                FROM sys.partition_functions 
                WHERE name = @FunctionName";

            var exists = await sqlExecutor.QuerySingleAsync<int>(
                connection,
                checkSql,
                new { FunctionName = partitionFunctionName });

            return exists > 0;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "检查分区函数 {Function} 是否存在失败。", partitionFunctionName);
            throw;
        }
    }

    /// <summary>
    /// 检查分区方案是否存在。
    /// </summary>
    public async Task<bool> CheckPartitionSchemeExistsAsync(
        Guid dataSourceId,
        string partitionSchemeName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);

            var checkSql = @"
                SELECT COUNT(1) 
                FROM sys.partition_schemes 
                WHERE name = @SchemeName";

            var exists = await sqlExecutor.QuerySingleAsync<int>(
                connection,
                checkSql,
                new { SchemeName = partitionSchemeName });

            return exists > 0;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "检查分区方案 {Scheme} 是否存在失败。", partitionSchemeName);
            throw;
        }
    }

    /// <summary>
    /// 创建分区函数（带初始边界值）。
    /// </summary>
    public async Task<bool> CreatePartitionFunctionAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        IReadOnlyList<PartitionValue>? initialBoundaries = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);

            // 检查是否已存在
            if (await CheckPartitionFunctionExistsAsync(dataSourceId, configuration.PartitionFunctionName, cancellationToken))
            {
                logger.LogInformation("分区函数 {Function} 已存在，跳过创建。", configuration.PartitionFunctionName);
                return false;
            }

            var template = templateProvider.GetTemplate("CreatePartitionFunction.sql");
            
            // 确定数据类型和范围类型（从 PartitionColumn.ValueKind 推导 SQL 数据类型）
            var dataType = configuration.PartitionColumn.ValueKind switch
            {
                Domain.Partitions.PartitionValueKind.Int => "int",
                Domain.Partitions.PartitionValueKind.BigInt => "bigint",
                Domain.Partitions.PartitionValueKind.Date => "date",
                Domain.Partitions.PartitionValueKind.DateTime => "datetime",
                Domain.Partitions.PartitionValueKind.DateTime2 => "datetime2",
                Domain.Partitions.PartitionValueKind.Guid => "uniqueidentifier",
                Domain.Partitions.PartitionValueKind.String => "nvarchar(50)",
                _ => "datetime2" // 默认使用 datetime2
            };
            var rangeType = configuration.IsRangeRight ? "RIGHT" : "LEFT";
            
            // 构建初始边界值列表（如果提供）
            var boundariesStr = initialBoundaries != null && initialBoundaries.Count > 0
                ? string.Join(", ", initialBoundaries.Select(b => b.ToLiteral()))
                : string.Empty;

            var sql = template
                .Replace("{PartitionFunction}", configuration.PartitionFunctionName)
                .Replace("{DataType}", dataType)
                .Replace("{RangeType}", rangeType)
                .Replace("{InitialBoundaries}", boundariesStr);

            await sqlExecutor.ExecuteAsync(connection, sql, timeoutSeconds: 60);

            logger.LogInformation(
                "成功创建分区函数：{Function}，数据类型：{DataType}，范围：{Range}，初始边界数：{Count}",
                configuration.PartitionFunctionName, dataType, rangeType, initialBoundaries?.Count ?? 0);

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "创建分区函数 {Function} 失败。", configuration.PartitionFunctionName);
            throw;
        }
    }

    /// <summary>
    /// 创建分区方案（映射分区函数到文件组）。
    /// </summary>
    public async Task<bool> CreatePartitionSchemeAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);

            // 检查是否已存在
            if (await CheckPartitionSchemeExistsAsync(dataSourceId, configuration.PartitionSchemeName, cancellationToken))
            {
                logger.LogInformation("分区方案 {Scheme} 已存在，跳过创建。", configuration.PartitionSchemeName);
                return false;
            }

            var template = templateProvider.GetTemplate("CreatePartitionScheme.sql");
            
            // 构建文件组映射（ALL TO 或指定映射）
            string filegroupMapping;
            if (configuration.FilegroupMappings.Count > 0)
            {
                // 使用用户定义的映射
                var mappings = configuration.FilegroupMappings
                    .OrderBy(m => m.BoundaryKey)
                    .Select(m => $"[{m.FilegroupName}]");
                filegroupMapping = $"TO ({string.Join(", ", mappings)})";
            }
            else
            {
                // 默认全部映射到存储配置指定的文件组
                var defaultFilegroup = ResolveDefaultFilegroup(configuration);
                filegroupMapping = $"ALL TO ([{defaultFilegroup}])";
            }

            var sql = template
                .Replace("{PartitionScheme}", configuration.PartitionSchemeName)
                .Replace("{PartitionFunction}", configuration.PartitionFunctionName)
                .Replace("{FilegroupMapping}", filegroupMapping);

            await sqlExecutor.ExecuteAsync(connection, sql, timeoutSeconds: 60);

            logger.LogInformation(
                "成功创建分区方案：{Scheme}，映射到分区函数：{Function}",
                configuration.PartitionSchemeName, configuration.PartitionFunctionName);

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "创建分区方案 {Scheme} 失败。", configuration.PartitionSchemeName);
            throw;
        }
    }

    /// <summary>
    /// 检查表是否已经是分区表。
    /// </summary>
    public async Task<bool> CheckTableIsPartitionedAsync(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);

            var checkSql = @"
                SELECT COUNT(1)
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                WHERE SCHEMA_NAME(t.schema_id) = @SchemaName
                  AND t.name = @TableName
                  AND i.index_id IN (0, 1)"; // 堆或聚集索引

            var isPartitioned = await sqlExecutor.QuerySingleAsync<int>(
                connection,
                checkSql,
                new { SchemaName = schemaName, TableName = tableName });

            return isPartitioned > 0;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "检查表 {Schema}.{Table} 是否分区失败。", schemaName, tableName);
            throw;
        }
    }

    /// <summary>
    /// 将普通表转换为分区表（保存所有索引定义，删除所有索引，修改分区列，然后在分区方案上重建所有索引）。
    /// <para>执行顺序（严格遵循 SQL Server 分区最佳实践）：</para>
    /// <para>1. 查询并保存所有索引定义</para>
    /// <para>2. 删除所有非聚集索引（保留聚集索引）</para>
    /// <para>3. 删除聚集索引（此时表上没有任何索引）</para>
    /// <para>3.5. 修改分区列为 NOT NULL（如果需要，此时所有索引已删除，不会报依赖错误）</para>
    /// <para>4. 重建聚集索引到分区方案（此时表变为分区表）</para>
    /// <para>5. 重建所有非聚集索引</para>
    /// </summary>
    public async Task<PartitionConversionResult> ConvertToPartitionedTableAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        PartitionIndexInspection indexInspection,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // 检查是否已经是分区表
            if (await CheckTableIsPartitionedAsync(dataSourceId, configuration.SchemaName, configuration.TableName, cancellationToken))
            {
                logger.LogInformation("表 {Schema}.{Table} 已经是分区表，跳过转换。", configuration.SchemaName, configuration.TableName);
                return PartitionConversionResult.Skipped();
            }

            // 双保险：确保分区函数与方案存在
            if (!await CheckPartitionFunctionExistsAsync(dataSourceId, configuration.PartitionFunctionName, cancellationToken))
            {
                logger.LogInformation("分区函数 {Function} 不存在，自动创建（转换前校验）。", configuration.PartitionFunctionName);
                await CreatePartitionFunctionAsync(dataSourceId, configuration, null, cancellationToken);
            }

            if (!await CheckPartitionSchemeExistsAsync(dataSourceId, configuration.PartitionSchemeName, cancellationToken))
            {
                logger.LogInformation("分区方案 {Scheme} 不存在，自动创建（转换前校验）。", configuration.PartitionSchemeName);
                await CreatePartitionSchemeAsync(dataSourceId, configuration, cancellationToken);
            }

            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken);

            logger.LogInformation(
                "开始将表 {Schema}.{Table} 转换为分区表（遵循 SQL Server 分区最佳实践）...",
                configuration.SchemaName, configuration.TableName);

            await sqlExecutor.ExecuteAsync(connection, "SET XACT_ABORT ON;", transaction: transaction, timeoutSeconds: 5);

            // ============== 步骤 1: 查询并保存所有索引定义 ==============
            logger.LogInformation("步骤 1/4: 查询表的所有索引定义...");
            var queryIndexesSql = templateProvider.GetTemplate("QueryTableIndexes.sql");
            var indexes = (await sqlExecutor.QueryAsync<Models.TableIndexDefinition>(
                connection,
                queryIndexesSql,
                new
                {
                    SchemaName = configuration.SchemaName,
                    TableName = configuration.TableName,
                    PartitionColumn = configuration.PartitionColumn.Name
                },
                transaction: transaction)).ToList();

            if (!indexes.Any())
            {
                throw new InvalidOperationException($"表 {configuration.SchemaName}.{configuration.TableName} 没有任何索引，无法转换为分区表。");
            }

            logger.LogInformation(
                "发现 {Count} 个索引：{Indexes}",
                indexes.Count,
                string.Join(", ", indexes.Select(i => $"{i.IndexName} ({i.GetDescription()})")));

            var indicesToAlign = new HashSet<string>(
                indexInspection.IndexesMissingPartitionColumn.Select(i => i.IndexName),
                StringComparer.OrdinalIgnoreCase);

            if (!indexInspection.HasClusteredIndex)
            {
                throw new PartitionConversionException(
                    $"表 {configuration.SchemaName}.{configuration.TableName} 未检测到聚集索引，无法自动对齐分区列。请先创建包含分区列 [{configuration.PartitionColumn.Name}] 的聚集索引。");
            }

            if (indexInspection.HasExternalForeignKeys && indicesToAlign.Count > 0)
            {
                var fkNames = indexInspection.ExternalForeignKeys.Count > 0
                    ? string.Join("、", indexInspection.ExternalForeignKeys)
                    : "存在外部外键引用";

                throw new PartitionConversionException(
                    $"表 {configuration.SchemaName}.{configuration.TableName} 的主键或唯一约束存在外部外键引用（{fkNames}），无法自动补齐分区列，请手动调整索引结构后重试。");
            }

            var autoAlignmentChanges = new List<IndexAlignmentChange>();
            var partitionColumnAltered = false;

            // 检查分区列可空性，但稍后在删除所有索引后再执行 ALTER COLUMN
            PartitionColumnMetadata? columnMetadata = null;
            bool needsAlterColumn = false;

            if (configuration.RequirePartitionColumnNotNull)
            {
                columnMetadata = await GetPartitionColumnMetadataAsync(
                    connection,
                    transaction,
                    configuration.SchemaName,
                    configuration.TableName,
                    configuration.PartitionColumn.Name,
                    cancellationToken);

                if (columnMetadata is null)
                {
                    throw new PartitionConversionException(
                        $"未能获取表 {configuration.SchemaName}.{configuration.TableName} 的分区列 {configuration.PartitionColumn.Name} 元数据，无法确认可空性。");
                }

                if (columnMetadata.IsNullable)
                {
                    var nullableCount = await sqlExecutor.QuerySingleAsync<int>(
                        connection,
                        $"SELECT COUNT(1) FROM [{configuration.SchemaName}].[{configuration.TableName}] WHERE [{configuration.PartitionColumn.Name}] IS NULL",
                        transaction: transaction);

                    if (nullableCount > 0)
                    {
                        throw new PartitionConversionException(
                            $"分区列 {configuration.PartitionColumn.Name} 仍存在 {nullableCount} 条 NULL 数据，无法自动转换为 NOT NULL。请清理数据或关闭去可空选项后重试。");
                    }

                    // 标记需要修改列，但稍后在删除索引后再执行
                    needsAlterColumn = true;
                    logger.LogInformation(
                        "分区列 {Schema}.{Table}.{Column} 当前为 NULL，将在删除索引后转换为 NOT NULL。",
                        configuration.SchemaName,
                        configuration.TableName,
                        configuration.PartitionColumn.Name);
                }
            }

            if (indicesToAlign.Count > 0)
            {
                foreach (var definition in indexes.Where(i => indicesToAlign.Contains(i.IndexName)))
                {
                    ApplyPartitionColumn(definition, configuration.PartitionColumn.Name, autoAlignmentChanges);
                }
            }

            var clusteredIndex = indexes.FirstOrDefault(i => i.IsClustered);
            if (clusteredIndex is null)
            {
                throw new PartitionConversionException(
                    $"表 {configuration.SchemaName}.{configuration.TableName} 未检测到聚集索引，无法自动对齐分区列。");
            }

            if (!clusteredIndex.ContainsPartitionColumn)
            {
                ApplyPartitionColumn(clusteredIndex, configuration.PartitionColumn.Name, autoAlignmentChanges);
            }

            var nonClusteredIndexes = indexes.Where(i => !i.IsClustered).OrderByDescending(i => i.IndexId).ToList();

            var droppedIndexes = new List<string>();
            var recreatedIndexes = new List<string>();

            // ============== 步骤 2: 删除所有非聚集索引（保留聚集索引） ==============
            logger.LogInformation("步骤 2/4: 删除所有非聚集索引（保留聚集索引 {ClusteredIndex}）...", clusteredIndex.IndexName);
            foreach (var index in nonClusteredIndexes)
            {
                var dropSql = index.GetDropSql(configuration.SchemaName, configuration.TableName);
                logger.LogInformation("  删除非聚集索引：{IndexName} ({Type})", index.IndexName, index.GetDescription());
                logger.LogDebug("  执行 SQL: {Sql}", dropSql);

                await sqlExecutor.ExecuteAsync(connection, dropSql, transaction: transaction, timeoutSeconds: 300);
                logger.LogInformation("  ✓ 已删除索引 {IndexName}", index.IndexName);
                droppedIndexes.Add(index.IndexName);
            }

            logger.LogInformation("已删除 {Count} 个非聚集索引。", nonClusteredIndexes.Count);

            // ============== 步骤 3: 删除聚集索引 ==============
            logger.LogInformation("步骤 3/5: 删除聚集索引（为 ALTER COLUMN 做准备）...");
            var clusteredDropSql = clusteredIndex.GetDropSql(configuration.SchemaName, configuration.TableName);
            logger.LogInformation("  删除聚集索引：{IndexName} ({Type})", clusteredIndex.IndexName, clusteredIndex.GetDescription());
            logger.LogDebug("  执行 SQL: {Sql}", clusteredDropSql);

            await sqlExecutor.ExecuteAsync(connection, clusteredDropSql, transaction: transaction, timeoutSeconds: 300);
            logger.LogInformation("  ✓ 已删除聚集索引 {IndexName}", clusteredIndex.IndexName);
            droppedIndexes.Add(clusteredIndex.IndexName);

            // ============== 步骤 3.5: 修改分区列为 NOT NULL（如果需要，此时所有索引已删除） ==============
            if (needsAlterColumn && columnMetadata is not null)
            {
                logger.LogInformation("步骤 3.5/5: 修改分区列为 NOT NULL（所有索引已删除）...");
                var alterSql = $"ALTER TABLE [{configuration.SchemaName}].[{configuration.TableName}] ALTER COLUMN [{configuration.PartitionColumn.Name}] {BuildColumnTypeDefinition(columnMetadata)} NOT NULL";
                logger.LogDebug("  执行 SQL: {Sql}", alterSql);
                
                await sqlExecutor.ExecuteAsync(connection, alterSql, transaction: transaction, timeoutSeconds: 300);
                logger.LogInformation(
                    "  ✓ 已将分区列 {Schema}.{Table}.{Column} 转换为 NOT NULL。",
                    configuration.SchemaName,
                    configuration.TableName,
                    configuration.PartitionColumn.Name);
                partitionColumnAltered = true;
            }

            // ============== 步骤 4: 重建聚集索引到分区方案（关键步骤：表变为分区表） ==============
            logger.LogInformation("步骤 4/5: 重建聚集索引到分区方案（表将变为分区表）...");
            var clusteredCreateSql = clusteredIndex.GetCreateSql(
                configuration.SchemaName,
                configuration.TableName,
                configuration.PartitionSchemeName,
                configuration.PartitionColumn.Name);

            logger.LogInformation("  重建聚集索引到分区方案：{IndexName} ON {Scheme}({Column})",
                clusteredIndex.IndexName, configuration.PartitionSchemeName, configuration.PartitionColumn.Name);
            logger.LogDebug("  执行 SQL: {Sql}", clusteredCreateSql);

            await sqlExecutor.ExecuteAsync(connection, clusteredCreateSql, transaction: transaction, timeoutSeconds: 1800);
            logger.LogInformation("  ✓ 已重建聚集索引 {IndexName}，表已变为分区表", clusteredIndex.IndexName);
            recreatedIndexes.Add(clusteredIndex.IndexName);

            // ============== 步骤 5: 重建所有非聚集索引 ==============
            logger.LogInformation("步骤 5/5: 重建所有非聚集索引（按 IndexId 正序）...");
            var indexesToCreate = nonClusteredIndexes.OrderBy(i => i.IndexId).ToList();
            foreach (var index in indexesToCreate)
            {
                var createSql = index.GetCreateSql(
                    configuration.SchemaName,
                    configuration.TableName,
                    configuration.PartitionSchemeName,
                    configuration.PartitionColumn.Name);

                logger.LogInformation("  重建非聚集索引：{IndexName} ({Type})", index.IndexName, index.GetDescription());
                logger.LogDebug("  执行 SQL: {Sql}", createSql);

                await sqlExecutor.ExecuteAsync(connection, createSql, transaction: transaction, timeoutSeconds: 1800);
                logger.LogInformation("  ✓ 已重建索引 {IndexName}", index.IndexName);
                recreatedIndexes.Add(index.IndexName);
            }

            logger.LogInformation(
                "✓ 成功将表 {Schema}.{Table} 转换为分区表，重建了 {TotalCount} 个索引（1个聚集 + {NonClusteredCount} 个非聚集）。",
                configuration.SchemaName, configuration.TableName,
                indexes.Count, nonClusteredIndexes.Count);

            await transaction.CommitAsync(cancellationToken);
            return PartitionConversionResult.Success(droppedIndexes, recreatedIndexes, autoAlignmentChanges, partitionColumnAltered);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "❌ 将表 {Schema}.{Table} 转换为分区表失败。", configuration.SchemaName, configuration.TableName);
            throw;
        }
    }

    /// <summary>
    /// 在事务中执行分区拆分脚本（执行前会检查并创建分区函数和方案，并将表转换为分区表）。
    /// </summary>
    public async Task<SqlExecutionResult> ExecuteSplitWithTransactionAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        IReadOnlyList<PartitionValue> newBoundaries,
        PartitionIndexInspection indexInspection,
        CancellationToken cancellationToken = default)
    {
        if (newBoundaries.Count == 0)
        {
            logger.LogInformation(
                "No new partition boundaries detected for {Schema}.{Table}, skip split execution.",
                configuration.SchemaName,
                configuration.TableName);

            return SqlExecutionResult.Success(
                0,
                0,
                "数据库分区边界已与草稿配置一致，无需拆分。");
        }

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var affectedPartitions = 0;

        try
        {
            // 前置检查：确保分区函数和方案存在
            var functionExists = await CheckPartitionFunctionExistsAsync(dataSourceId, configuration.PartitionFunctionName, cancellationToken);
            var schemeExists = await CheckPartitionSchemeExistsAsync(dataSourceId, configuration.PartitionSchemeName, cancellationToken);

            if (!functionExists)
            {
                logger.LogWarning("分区函数 {Function} 不存在，准备创建（不带初始边界）。", configuration.PartitionFunctionName);
                await CreatePartitionFunctionAsync(dataSourceId, configuration, null, cancellationToken);
            }

            if (!schemeExists)
            {
                logger.LogWarning("分区方案 {Scheme} 不存在，准备创建。", configuration.PartitionSchemeName);
                await CreatePartitionSchemeAsync(dataSourceId, configuration, cancellationToken);
            }

            // 检查并转换表为分区表
            var conversionResult = await ConvertToPartitionedTableAsync(dataSourceId, configuration, indexInspection, cancellationToken);
            if (conversionResult.Converted)
            {
                var droppedSummary = conversionResult.DroppedIndexNames.Count > 0
                    ? string.Join(", ", conversionResult.DroppedIndexNames)
                    : "无";
                var recreatedSummary = conversionResult.RecreatedIndexNames.Count > 0
                    ? string.Join(", ", conversionResult.RecreatedIndexNames)
                    : "无";
                var alignmentSummary = conversionResult.AutoAlignedIndexes.Count > 0
                    ? string.Join(", ", conversionResult.AutoAlignedIndexes.Select(a => $"{a.IndexName}({a.OriginalKeyColumns} -> {a.UpdatedKeyColumns})"))
                    : "无";
                var columnSummary = conversionResult.PartitionColumnAlteredToNotNull
                    ? "分区列已转换为 NOT NULL"
                    : "分区列未调整";

                logger.LogInformation(
                    "表 {Schema}.{Table} 已转换为分区表。已删除索引：{Dropped}；已重建索引：{Recreated}；自动对齐索引：{Aligned}；列调整：{Column}",
                    configuration.SchemaName,
                    configuration.TableName,
                    droppedSummary,
                    recreatedSummary,
                    alignmentSummary,
                    columnSummary);
            }

            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken);

            try
            {
                // 启用 XACT_ABORT
                await sqlExecutor.ExecuteAsync(connection, "SET XACT_ABORT ON;", transaction: transaction, timeoutSeconds: 5);

                var script = RenderSplitScript(configuration, newBoundaries);

                // 执行分区拆分
                affectedPartitions = await sqlExecutor.ExecuteAsync(
                    connection,
                    script,
                    transaction: transaction,
                    timeoutSeconds: 600); // 10分钟超时

                await transaction.CommitAsync(cancellationToken);

                stopwatch.Stop();

                logger.LogInformation(
                    "成功执行分区拆分：{Schema}.{Table}，新增 {Count} 个边界，耗时 {Elapsed}",
                    configuration.SchemaName, configuration.TableName, newBoundaries.Count, stopwatch.Elapsed);

                return SqlExecutionResult.Success(
                    affectedPartitions,
                    stopwatch.ElapsedMilliseconds,
                    $"成功拆分 {newBoundaries.Count} 个分区边界");
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);
                throw;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            logger.LogError(ex, "执行分区拆分失败：{Schema}.{Table}", configuration.SchemaName, configuration.TableName);

            return SqlExecutionResult.Failure(
                ex.Message,
                stopwatch.ElapsedMilliseconds,
                ex.ToString());
        }
    }

    /// <summary>
    /// 执行 MERGE 操作（移除分区边界）。
    /// </summary>
    public async Task<SqlExecutionResult> ExecuteMergeWithTransactionAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        string boundaryKey,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken);

            try
            {
                await sqlExecutor.ExecuteAsync(connection, "SET XACT_ABORT ON;", transaction: transaction, timeoutSeconds: 5);

                var script = RenderMergeScript(configuration, boundaryKey);

                await sqlExecutor.ExecuteAsync(connection, script, transaction: transaction, timeoutSeconds: 300);

                await transaction.CommitAsync(cancellationToken);

                stopwatch.Stop();

                logger.LogInformation(
                    "成功执行分区合并：{Schema}.{Table}，移除边界 {Boundary}，耗时 {Elapsed}",
                    configuration.SchemaName, configuration.TableName, boundaryKey, stopwatch.Elapsed);

                return SqlExecutionResult.Success(
                    1,
                    stopwatch.ElapsedMilliseconds,
                    $"成功移除分区边界：{boundaryKey}");
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);
                throw;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            logger.LogError(ex, "执行分区合并失败：{Schema}.{Table}", configuration.SchemaName, configuration.TableName);

            return SqlExecutionResult.Failure(
                ex.Message,
                stopwatch.ElapsedMilliseconds,
                ex.ToString());
        }
    }

    /// <summary>
    /// 执行分区切换操作（带事务保护）。
    /// </summary>
    public async Task<SqlExecutionResult> ExecuteSwitchWithTransactionAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        SwitchPayload payload,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken);

            try
            {
                await sqlExecutor.ExecuteAsync(connection, "SET XACT_ABORT ON;", transaction: transaction, timeoutSeconds: 5);

                var script = RenderSwitchOutScript(configuration, payload);

                await sqlExecutor.ExecuteAsync(connection, script, transaction: transaction, timeoutSeconds: 300);

                await transaction.CommitAsync(cancellationToken);

                stopwatch.Stop();

                logger.LogInformation(
                    "成功执行分区切换：{Schema}.{Table} 分区 {Partition} -> {TargetSchema}.{TargetTable}，耗时 {Elapsed}",
                    configuration.SchemaName, configuration.TableName, payload.SourcePartitionKey, payload.TargetSchema, payload.TargetTable, stopwatch.Elapsed);

                return SqlExecutionResult.Success(
                    1,
                    stopwatch.ElapsedMilliseconds,
                    $"成功切换分区 {payload.SourcePartitionKey} 至 {payload.TargetSchema}.{payload.TargetTable}");
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);
                throw;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            logger.LogError(
                ex,
                "执行分区切换失败：{Schema}.{Table} -> {TargetSchema}.{TargetTable}",
                configuration.SchemaName,
                configuration.TableName,
                payload.TargetSchema,
                payload.TargetTable);

            return SqlExecutionResult.Failure(
                ex.Message,
                stopwatch.ElapsedMilliseconds,
                ex.ToString());
        }
    }

    private static void ApplyPartitionColumn(TableIndexDefinition definition, string partitionColumn, IList<IndexAlignmentChange> alignmentChanges)
    {
        var originalKeyColumns = definition.KeyColumns ?? string.Empty;

        var keyTokens = string.IsNullOrWhiteSpace(originalKeyColumns)
            ? new List<string>()
            : originalKeyColumns
                .Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(token => token.Trim())
                .ToList();

        var hasPartitionColumn = keyTokens.Any(token => token.StartsWith($"[{partitionColumn}]", StringComparison.OrdinalIgnoreCase));

        if (!hasPartitionColumn)
        {
            keyTokens.Add($"[{partitionColumn}] ASC");
            var updated = string.Join(", ", keyTokens);
            definition.KeyColumns = updated;
            definition.ContainsPartitionColumn = true;
            alignmentChanges.Add(new IndexAlignmentChange(definition.IndexName, originalKeyColumns, updated));
        }
        else if (!definition.ContainsPartitionColumn)
        {
            definition.ContainsPartitionColumn = true;
        }
    }

    private async Task<PartitionColumnMetadata?> GetPartitionColumnMetadataAsync(
        IDbConnection connection,
        IDbTransaction transaction,
        string schemaName,
        string tableName,
        string columnName,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT TOP 1
    t.name AS DataType,
    c.max_length AS MaxLength,
    c.precision AS Precision,
    c.scale AS Scale,
    c.is_nullable AS IsNullable,
    c.collation_name AS CollationName
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id AND c.system_type_id = t.system_type_id
WHERE c.object_id = OBJECT_ID(@FullName) AND c.name = @ColumnName";

        var result = await sqlExecutor.QuerySingleAsync<PartitionColumnMetadata>(
            connection,
            sql,
            new
            {
                FullName = $"[{schemaName}].[{tableName}]",
                ColumnName = columnName
            },
            transaction);

        return result;
    }

    private static string BuildColumnTypeDefinition(PartitionColumnMetadata metadata)
    {
        var dataType = metadata.DataType.ToLowerInvariant();
        string typeDeclaration = dataType switch
        {
            "nvarchar" => metadata.MaxLength == -1
                ? "NVARCHAR(MAX)"
                : $"NVARCHAR({Math.Max(1, metadata.MaxLength / 2)})",
            "nchar" => metadata.MaxLength == -1
                ? "NCHAR(MAX)"
                : $"NCHAR({Math.Max(1, metadata.MaxLength / 2)})",
            "varchar" => metadata.MaxLength == -1
                ? "VARCHAR(MAX)"
                : $"VARCHAR({Math.Max(1, Convert.ToInt32(metadata.MaxLength))})",
            "char" => metadata.MaxLength == -1
                ? "CHAR(MAX)"
                : $"CHAR({Math.Max(1, Convert.ToInt32(metadata.MaxLength))})",
            "varbinary" => metadata.MaxLength == -1
                ? "VARBINARY(MAX)"
                : $"VARBINARY({Math.Max(1, Convert.ToInt32(metadata.MaxLength))})",
            "binary" => metadata.MaxLength == -1
                ? "BINARY(MAX)"
                : $"BINARY({Math.Max(1, Convert.ToInt32(metadata.MaxLength))})",
            "decimal" or "numeric" => metadata.Scale > 0
                ? $"{metadata.DataType.ToUpperInvariant()}({metadata.Precision},{metadata.Scale})"
                : $"{metadata.DataType.ToUpperInvariant()}({metadata.Precision})",
            "datetime2" or "datetimeoffset" or "time" => metadata.Scale > 0
                ? $"{metadata.DataType.ToUpperInvariant()}({metadata.Scale})"
                : metadata.DataType.ToUpperInvariant(),
            "float" or "real" or "bigint" or "int" or "smallint" or "tinyint" or "bit" or "datetime" or "date" or "smalldatetime" or "money" or "smallmoney" or "uniqueidentifier"
                => metadata.DataType.ToUpperInvariant(),
            _ => metadata.DataType.ToUpperInvariant()
        };

        if (!string.IsNullOrWhiteSpace(metadata.CollationName) &&
            (dataType.Contains("char") || dataType.Contains("text")))
        {
            typeDeclaration += $" COLLATE {metadata.CollationName}";
        }

        return typeDeclaration;
    }

    private static string ResolveDefaultFilegroup(PartitionConfiguration configuration)
    {
        if (configuration.StorageSettings.Mode == PartitionStorageMode.DedicatedFilegroupSingleFile &&
            !string.IsNullOrWhiteSpace(configuration.StorageSettings.FilegroupName))
        {
            return configuration.StorageSettings.FilegroupName;
        }

        return configuration.FilegroupStrategy.PrimaryFilegroup;
    }
}

internal sealed record PartitionConversionResult(
    bool Converted,
    IReadOnlyList<string> DroppedIndexNames,
    IReadOnlyList<string> RecreatedIndexNames,
    IReadOnlyList<IndexAlignmentChange> AutoAlignedIndexes,
    bool PartitionColumnAlteredToNotNull)
{
    public static PartitionConversionResult Skipped() =>
        new(false, Array.Empty<string>(), Array.Empty<string>(), Array.Empty<IndexAlignmentChange>(), false);

    public static PartitionConversionResult Success(
        IReadOnlyList<string> droppedIndexNames,
        IReadOnlyList<string> recreatedIndexNames,
        IReadOnlyList<IndexAlignmentChange> autoAlignedIndexes,
        bool partitionColumnAlteredToNotNull) =>
        new(true, droppedIndexNames, recreatedIndexNames, autoAlignedIndexes, partitionColumnAlteredToNotNull);
}

internal sealed record IndexAlignmentChange(
    string IndexName,
    string OriginalKeyColumns,
    string UpdatedKeyColumns);

internal sealed class PartitionColumnMetadata
{
    public string DataType { get; set; } = string.Empty;
    public short MaxLength { get; set; }
    public byte Precision { get; set; }
    public byte Scale { get; set; }
    public bool IsNullable { get; set; }
    public string? CollationName { get; set; }
}

/// <summary>
/// SQL 执行结果。
/// </summary>
public sealed class SqlExecutionResult
{
    /// <summary>是否成功。</summary>
    public bool IsSuccess { get; private init; }

    /// <summary>受影响的行数或分区数。</summary>
    public int AffectedCount { get; private init; }

    /// <summary>执行耗时（毫秒）。</summary>
    public long ElapsedMilliseconds { get; private init; }

    /// <summary>执行结果消息。</summary>
    public string Message { get; private init; } = string.Empty;

    /// <summary>错误详情（如果失败）。</summary>
    public string? ErrorDetail { get; private init; }

    public static SqlExecutionResult Success(int affectedCount, long elapsedMs, string message)
        => new()
        {
            IsSuccess = true,
            AffectedCount = affectedCount,
            ElapsedMilliseconds = elapsedMs,
            Message = message
        };

    public static SqlExecutionResult Failure(string message, long elapsedMs, string? errorDetail = null)
        => new()
        {
            IsSuccess = false,
            AffectedCount = 0,
            ElapsedMilliseconds = elapsedMs,
            Message = message,
            ErrorDetail = errorDetail
        };
}
