using System.Data;
using System.Diagnostics;
using DbArchiveTool.Shared.Archive;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 基于 SqlBulkCopy 的批量数据传输执行器
/// 使用 Dapper 读取数据 + SqlBulkCopy 写入,兼顾性能与代码风格一致性
/// </summary>
public class SqlBulkCopyExecutor
{
    private readonly ILogger<SqlBulkCopyExecutor> _logger;

    public SqlBulkCopyExecutor(ILogger<SqlBulkCopyExecutor> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// 执行批量数据复制
    /// </summary>
    /// <param name="sourceConnectionString">源数据库连接字符串</param>
    /// <param name="targetConnectionString">目标数据库连接字符串</param>
    /// <param name="sourceQuery">源数据查询 SQL</param>
    /// <param name="targetTable">目标表名 (格式: [schema].[table])</param>
    /// <param name="options">批量复制选项</param>
    /// <param name="progress">进度回调</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>批量复制结果</returns>
    public async Task<BulkCopyResult> ExecuteAsync(
        string sourceConnectionString,
        string targetConnectionString,
        string sourceQuery,
        string targetTable,
        BulkCopyOptions options,
        IProgress<BulkCopyProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sourceConnectionString))
        {
            throw new ArgumentException("源连接字符串不能为空", nameof(sourceConnectionString));
        }

        if (string.IsNullOrWhiteSpace(targetConnectionString))
        {
            throw new ArgumentException("目标连接字符串不能为空", nameof(targetConnectionString));
        }

        if (string.IsNullOrWhiteSpace(sourceQuery))
        {
            throw new ArgumentException("源查询 SQL 不能为空", nameof(sourceQuery));
        }

        if (string.IsNullOrWhiteSpace(targetTable))
        {
            throw new ArgumentException("目标表名不能为空", nameof(targetTable));
        }

        var startTimeUtc = DateTime.UtcNow;
        var stopwatch = Stopwatch.StartNew();
        var totalRowsCopied = 0L;

        try
        {
            _logger.LogInformation(
                "开始 SqlBulkCopy 传输: 目标表={TargetTable}, 批次大小={BatchSize}",
                targetTable, options.BatchSize);

            using var sourceConnection = new SqlConnection(sourceConnectionString);
            using var targetConnection = new SqlConnection(targetConnectionString);

            await sourceConnection.OpenAsync(cancellationToken);
            await targetConnection.OpenAsync(cancellationToken);

            _logger.LogDebug("源和目标数据库连接已建立");

            // 1. 使用 Dapper 风格流式读取源数据
            using var command = sourceConnection.CreateCommand();
            command.CommandText = sourceQuery;
            command.CommandTimeout = options.TimeoutSeconds;

            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            _logger.LogDebug("开始读取源数据: {SourceQuery}", sourceQuery);

            // 2. 配置 SqlBulkCopy
            using var bulkCopy = new SqlBulkCopy(
                targetConnection,
                SqlBulkCopyOptions.Default | SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction,
                null)
            {
                DestinationTableName = targetTable,
                BatchSize = options.BatchSize,
                BulkCopyTimeout = options.TimeoutSeconds,
                EnableStreaming = true // 流式传输,减少内存占用
            };

            _logger.LogDebug(
                "SqlBulkCopy 已配置: DestinationTable={DestinationTable}, BatchSize={BatchSize}, EnableStreaming=true",
                targetTable, options.BatchSize);

            // 3. 自动映射列(列名相同则自动对应)
            // 如果源表和目标表列名一致,SqlBulkCopy 会自动映射
            // 如需自定义映射可手动添加: bulkCopy.ColumnMappings.Add("SourceCol", "TargetCol");

            // 4. 注册进度回调
            var lastProgressTime = DateTime.UtcNow;
            bulkCopy.SqlRowsCopied += (sender, e) =>
            {
                totalRowsCopied = e.RowsCopied;

                if (progress != null)
                {
                    var elapsed = DateTime.UtcNow - startTimeUtc;
                    var currentThroughput = elapsed.TotalSeconds > 0
                        ? totalRowsCopied / elapsed.TotalSeconds
                        : 0;

                    var percentComplete = CalculatePercentage(totalRowsCopied, options.EstimatedTotalRows);

                    TimeSpan? estimatedTimeRemaining = null;
                    if (options.EstimatedTotalRows.HasValue && currentThroughput > 0)
                    {
                        var remainingRows = options.EstimatedTotalRows.Value - totalRowsCopied;
                        estimatedTimeRemaining = TimeSpan.FromSeconds(remainingRows / currentThroughput);
                    }

                    progress.Report(new BulkCopyProgress
                    {
                        RowsCopied = totalRowsCopied,
                        PercentComplete = percentComplete,
                        StartTimeUtc = startTimeUtc,
                        CurrentThroughput = currentThroughput,
                        EstimatedTimeRemaining = estimatedTimeRemaining
                    });
                }

                var now = DateTime.UtcNow;
                if ((now - lastProgressTime).TotalSeconds >= 5) // 每5秒记录一次日志
                {
                    _logger.LogInformation(
                        "SqlBulkCopy 进度: 已复制 {RowsCopied:N0} 行",
                        totalRowsCopied);
                    lastProgressTime = now;
                }
            };
            bulkCopy.NotifyAfter = options.NotifyAfterRows;

            // 5. 执行批量复制
            await bulkCopy.WriteToServerAsync(reader, cancellationToken);

            // ⚠️ 关键修复: SqlRowsCopied 事件在数据量 < NotifyAfter 时不触发
            // 必须在 WriteToServerAsync 完成后使用 RowsCopied 属性获取实际行数
            totalRowsCopied = bulkCopy.RowsCopied;

            stopwatch.Stop();
            var duration = stopwatch.Elapsed;
            var throughput = duration.TotalSeconds > 0
                ? totalRowsCopied / duration.TotalSeconds
                : 0;

            _logger.LogInformation(
                "SqlBulkCopy 完成: 总行数={TotalRows:N0}, 耗时={Duration}, 吞吐量={Throughput:N0} 行/秒",
                totalRowsCopied, duration, throughput);

            return new BulkCopyResult
            {
                Succeeded = true,
                RowsCopied = totalRowsCopied,
                Duration = duration,
                ThroughputRowsPerSecond = throughput,
                StartTimeUtc = startTimeUtc,
                EndTimeUtc = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();

            _logger.LogError(
                ex,
                "SqlBulkCopy 失败: 已复制 {RowsCopied:N0} 行, 耗时={Duration}",
                totalRowsCopied, stopwatch.Elapsed);

            return new BulkCopyResult
            {
                Succeeded = false,
                RowsCopied = totalRowsCopied,
                Duration = stopwatch.Elapsed,
                ThroughputRowsPerSecond = stopwatch.Elapsed.TotalSeconds > 0
                    ? totalRowsCopied / stopwatch.Elapsed.TotalSeconds
                    : 0,
                ErrorMessage = ex.Message,
                StartTimeUtc = startTimeUtc,
                EndTimeUtc = DateTime.UtcNow
            };
        }
    }

    /// <summary>
    /// 计算完成百分比
    /// </summary>
    private static double CalculatePercentage(long current, long? total)
    {
        if (!total.HasValue || total.Value == 0)
        {
            return 0;
        }

        return Math.Min(100.0, (double)current / total.Value * 100);
    }
}
