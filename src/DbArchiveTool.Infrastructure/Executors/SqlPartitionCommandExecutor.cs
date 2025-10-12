using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
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
        var filegroup = configuration.FilegroupStrategy.PrimaryFilegroup;

        foreach (var boundary in newBoundaries)
        {
            builder.AppendLine(template
                .Replace("{PartitionScheme}", configuration.PartitionSchemeName)
                .Replace("{FilegroupName}", filegroup)
                .Replace("{PartitionFunction}", configuration.PartitionFunctionName)
                .Replace("{BoundaryLiteral}", boundary.ToLiteral()));
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
        var template = templateProvider.GetTemplate("SwitchOut.sql");
        var script = template
            .Replace("{Schema}", configuration.SchemaName)
            .Replace("{SourceTable}", configuration.TableName)
            .Replace("{TargetTable}", payload.TargetTable)
            .Replace("{SourcePartitionNumber}", payload.SourcePartitionKey)
            .Replace("{TargetPartitionNumber}", payload.SourcePartitionKey);

        logger.LogDebug("Switch script generated for {Schema}.{Table} to {Target}", configuration.SchemaName, configuration.TableName, payload.TargetTable);
        return script;
    }

    /// <summary>
    /// 如果指定文件组不存在则创建。
    /// </summary>
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
                .Replace("{FilegroupName}", filegroupName);

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

            var template = templateProvider.GetTemplate("AddDataFile.sql");
            var sql = template
                .Replace("{DatabaseName}", databaseName)
                .Replace("{FileName}", fileName)
                .Replace("{FilePath}", filePath)
                .Replace("{InitialSizeMB}", initialSizeMB.ToString())
                .Replace("{GrowthSizeMB}", growthSizeMB.ToString())
                .Replace("{FilegroupName}", filegroupName);

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
    /// 在事务中执行分区拆分脚本。
    /// </summary>
    public async Task<SqlExecutionResult> ExecuteSplitWithTransactionAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        IReadOnlyList<PartitionValue> newBoundaries,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var affectedPartitions = 0;

        try
        {
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
