using DbArchiveTool.Application.Archives;
using DbArchiveTool.Domain.ArchiveConfigurations;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Shared.Archive;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 优化的分区归档执行器
/// 使用 PARTITION SWITCH + BCP/BulkCopy 策略,将生产表锁定时间降低到 < 1 秒
/// </summary>
public sealed class OptimizedPartitionArchiveExecutor
{
    private readonly IPartitionMetadataService _partitionMetadataService;
    private readonly SqlBulkCopyExecutor _bulkCopyExecutor;
    private readonly BcpExecutor _bcpExecutor;
    private readonly ILogger<OptimizedPartitionArchiveExecutor> _logger;

    /// <summary>
    /// 初始化优化的分区归档执行器
    /// </summary>
    public OptimizedPartitionArchiveExecutor(
        IPartitionMetadataService partitionMetadataService,
        SqlBulkCopyExecutor bulkCopyExecutor,
        BcpExecutor bcpExecutor,
        ILogger<OptimizedPartitionArchiveExecutor> logger)
    {
        _partitionMetadataService = partitionMetadataService;
        _bulkCopyExecutor = bulkCopyExecutor;
        _bcpExecutor = bcpExecutor;
        _logger = logger;
    }

    /// <summary>
    /// 执行优化的分区归档
    /// </summary>
    /// <param name="config">归档配置</param>
    /// <param name="dataSource">数据源配置</param>
    /// <param name="targetConnectionString">目标数据库连接字符串</param>
    /// <param name="partitionNumber">要归档的分区号</param>
    /// <param name="progressCallback">进度回调</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>归档结果</returns>
    public async Task<ArchiveResult> ExecuteAsync(
        ArchiveConfiguration config,
        ArchiveDataSource dataSource,
        string targetConnectionString,
        int partitionNumber,
        Action<ArchiveProgress>? progressCallback = null,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var stagingTableName = $"{config.SourceTableName}_Archive_Staging_{Guid.NewGuid():N}";

        try
        {
            _logger.LogInformation(
                "开始优化分区归档: {Schema}.{Table} 分区 {Partition}",
                config.SourceSchemaName,
                config.SourceTableName,
                partitionNumber);

            // 步骤 1: 验证分区表结构
            ReportProgress(progressCallback, "验证分区表结构", 0, 0);
            var sourceConnectionString = BuildConnectionString(dataSource, false);
            
            var partitionInfo = await _partitionMetadataService.GetPartitionInfoAsync(
                sourceConnectionString,
                config.SourceSchemaName,
                config.SourceTableName,
                cancellationToken);

            if (string.IsNullOrEmpty(partitionInfo.PartitionFunction))
            {
                throw new InvalidOperationException(
                    $"表 {config.SourceSchemaName}.{config.SourceTableName} 不是分区表");
            }

            // 获取该分区的行数
            var rowCount = await GetPartitionRowCountAsync(
                sourceConnectionString,
                config.SourceSchemaName,
                config.SourceTableName,
                partitionNumber,
                cancellationToken);

            _logger.LogInformation("分区 {Partition} 包含 {RowCount} 行数据", partitionNumber, rowCount);

            // 步骤 2: 创建临时归档表(与源表结构相同)
            ReportProgress(progressCallback, "创建临时归档表", 10, rowCount);
            await CreateStagingTableAsync(
                sourceConnectionString,
                config.SourceSchemaName,
                config.SourceTableName,
                stagingTableName,
                cancellationToken);

            // 步骤 3: 执行 PARTITION SWITCH (< 1 秒)
            ReportProgress(progressCallback, "执行分区切换", 20, rowCount);
            var switchStartTime = DateTime.UtcNow;
            
            await SwitchPartitionAsync(
                sourceConnectionString,
                config.SourceSchemaName,
                config.SourceTableName,
                stagingTableName,
                partitionNumber,
                cancellationToken);

            var switchDuration = DateTime.UtcNow - switchStartTime;
            _logger.LogInformation(
                "分区切换完成,耗时 {Duration} 毫秒",
                switchDuration.TotalMilliseconds);

            // 步骤 4: 使用 BCP 或 BulkCopy 将数据传输到目标
            ReportProgress(progressCallback, "传输数据到目标数据库", 30, rowCount);
            
            long transferredRows;
            if (config.ArchiveMethod == ArchiveMethod.Bcp)
            {
                transferredRows = await TransferViaBcpAsync(
                    sourceConnectionString,
                    targetConnectionString,
                    config.SourceSchemaName,
                    stagingTableName,
                    config.SourceTableName,
                    progressCallback,
                    rowCount,
                    cancellationToken);
            }
            else
            {
                transferredRows = await TransferViaBulkCopyAsync(
                    sourceConnectionString,
                    targetConnectionString,
                    config.SourceSchemaName,
                    stagingTableName,
                    config.SourceTableName,
                    progressCallback,
                    rowCount,
                    cancellationToken);
            }

            // 步骤 5: 清理临时表
            ReportProgress(progressCallback, "清理临时表", 90, rowCount);
            await DropStagingTableAsync(
                sourceConnectionString,
                config.SourceSchemaName,
                stagingTableName,
                cancellationToken);

            ReportProgress(progressCallback, "归档完成", 100, rowCount);

            var duration = DateTime.UtcNow - startTime;
            _logger.LogInformation(
                "优化分区归档完成: 传输 {Rows} 行,总耗时 {Duration} 秒",
                transferredRows,
                duration.TotalSeconds);

            return new ArchiveResult
            {
                Success = true,
                RowsArchived = transferredRows,
                StartTime = startTime,
                EndTime = DateTime.UtcNow,
                Duration = duration,
                Message = $"成功归档分区 {partitionNumber},传输 {transferredRows} 行数据"
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "优化分区归档失败");

            // 确保清理临时表
            try
            {
                var sourceConnectionString = BuildConnectionString(dataSource, false);
                await DropStagingTableAsync(
                    sourceConnectionString,
                    config.SourceSchemaName,
                    stagingTableName,
                    cancellationToken);
            }
            catch (Exception cleanupEx)
            {
                _logger.LogWarning(cleanupEx, "清理临时表失败");
            }

            return new ArchiveResult
            {
                Success = false,
                RowsArchived = 0,
                StartTime = startTime,
                EndTime = DateTime.UtcNow,
                Duration = DateTime.UtcNow - startTime,
                Message = $"归档失败: {ex.Message}",
                ErrorDetails = ex.ToString()
            };
        }
    }

    /// <summary>
    /// 获取分区行数
    /// </summary>
    private async Task<long> GetPartitionRowCountAsync(
        string connectionString,
        string schemaName,
        string tableName,
        int partitionNumber,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            SELECT p.rows
            FROM sys.partitions p
            INNER JOIN sys.tables t ON p.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @SchemaName
              AND t.name = @TableName
              AND p.partition_number = @PartitionNumber
              AND p.index_id <= 1";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@SchemaName", schemaName);
        command.Parameters.AddWithValue("@TableName", tableName);
        command.Parameters.AddWithValue("@PartitionNumber", partitionNumber);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result != null && result != DBNull.Value ? Convert.ToInt64(result) : 0;
    }

    /// <summary>
    /// 创建临时归档表
    /// </summary>
    private async Task CreateStagingTableAsync(
        string connectionString,
        string schemaName,
        string sourceTableName,
        string stagingTableName,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        // 使用 SELECT INTO 创建与源表结构相同的空表
        var sql = $@"
            SELECT TOP 0 *
            INTO [{schemaName}].[{stagingTableName}]
            FROM [{schemaName}].[{sourceTableName}]";

        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation("创建临时表 {Schema}.{Table}", schemaName, stagingTableName);
    }

    /// <summary>
    /// 执行分区切换
    /// </summary>
    private async Task SwitchPartitionAsync(
        string connectionString,
        string schemaName,
        string sourceTableName,
        string stagingTableName,
        int partitionNumber,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        // PARTITION SWITCH 将指定分区的数据瞬间切换到临时表
        var sql = $@"
            ALTER TABLE [{schemaName}].[{sourceTableName}]
            SWITCH PARTITION {partitionNumber}
            TO [{schemaName}].[{stagingTableName}]";

        await using var command = new SqlCommand(sql, connection);
        command.CommandTimeout = 300; // 5 分钟超时
        await command.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation(
            "分区切换完成: {Schema}.{Table} 分区 {Partition} -> {Staging}",
            schemaName,
            sourceTableName,
            partitionNumber,
            stagingTableName);
    }

    /// <summary>
    /// 通过 BCP 传输数据
    /// </summary>
    private async Task<long> TransferViaBcpAsync(
        string sourceConnectionString,
        string targetConnectionString,
        string schemaName,
        string stagingTableName,
        string targetTableName,
        Action<ArchiveProgress>? progressCallback,
        long totalRows,
        CancellationToken cancellationToken)
    {
        var options = new BcpOptions
        {
            BatchSize = 10000,
            UseNativeFormat = true
        };

        var sourceQuery = $"SELECT * FROM [{schemaName}].[{stagingTableName}]";
        var targetTable = $"[{schemaName}].[{targetTableName}]";

        var result = await _bcpExecutor.ExecuteAsync(
            sourceConnectionString,
            targetConnectionString,
            sourceQuery,
            targetTable,
            options,
            new Progress<BulkCopyProgress>(p =>
            {
                // BCP 阶段占 30-90%
                var overallProgress = 30 + (int)(p.PercentComplete * 0.6);
                ReportProgress(progressCallback, "传输数据", overallProgress, totalRows);
            }),
            cancellationToken);

        if (!result.Succeeded)
        {
            throw new InvalidOperationException($"BCP 传输失败: {result.ErrorMessage}");
        }

        return result.RowsCopied;
    }

    /// <summary>
    /// 通过 BulkCopy 传输数据
    /// </summary>
    private async Task<long> TransferViaBulkCopyAsync(
        string sourceConnectionString,
        string targetConnectionString,
        string schemaName,
        string stagingTableName,
        string targetTableName,
        Action<ArchiveProgress>? progressCallback,
        long totalRows,
        CancellationToken cancellationToken)
    {
        var options = new BulkCopyOptions
        {
            BatchSize = 10000,
            NotifyAfterRows = 1000,
            EstimatedTotalRows = totalRows,
            TimeoutSeconds = 3600
        };

        var sourceQuery = $"SELECT * FROM [{schemaName}].[{stagingTableName}]";
        var targetTable = $"[{schemaName}].[{targetTableName}]";

        var result = await _bulkCopyExecutor.ExecuteAsync(
            sourceConnectionString,
            targetConnectionString,
            sourceQuery,
            targetTable,
            options,
            new Progress<BulkCopyProgress>(p =>
            {
                // BulkCopy 阶段占 30-90%
                var overallProgress = 30 + (int)(p.PercentComplete * 0.6);
                ReportProgress(progressCallback, "批量复制数据", overallProgress, totalRows);
            }),
            cancellationToken);

        if (!result.Succeeded)
        {
            throw new InvalidOperationException($"BulkCopy 失败: {result.ErrorMessage}");
        }

        return result.RowsCopied;
    }

    /// <summary>
    /// 删除临时表
    /// </summary>
    private async Task DropStagingTableAsync(
        string connectionString,
        string schemaName,
        string stagingTableName,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"DROP TABLE IF EXISTS [{schemaName}].[{stagingTableName}]";

        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation("删除临时表 {Schema}.{Table}", schemaName, stagingTableName);
    }

    /// <summary>
    /// 构建数据库连接字符串
    /// </summary>
    private string BuildConnectionString(ArchiveDataSource dataSource, bool useTarget)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = useTarget && !dataSource.UseSourceAsTarget
                ? $"{dataSource.TargetServerAddress},{dataSource.TargetServerPort}"
                : $"{dataSource.ServerAddress},{dataSource.ServerPort}",
            InitialCatalog = useTarget && !dataSource.UseSourceAsTarget
                ? dataSource.TargetDatabaseName!
                : dataSource.DatabaseName
        };

        if (useTarget && !dataSource.UseSourceAsTarget)
        {
            if (dataSource.TargetUseIntegratedSecurity)
            {
                builder.IntegratedSecurity = true;
            }
            else
            {
                builder.IntegratedSecurity = false;
                builder.UserID = dataSource.TargetUserName;
                builder.Password = dataSource.TargetPassword;
            }
        }
        else
        {
            if (dataSource.UseIntegratedSecurity)
            {
                builder.IntegratedSecurity = true;
            }
            else
            {
                builder.IntegratedSecurity = false;
                builder.UserID = dataSource.UserName;
                builder.Password = dataSource.Password;
            }
        }

        builder.TrustServerCertificate = true;
        builder.ConnectTimeout = 30;

        return builder.ConnectionString;
    }

    /// <summary>
    /// 报告进度
    /// </summary>
    private void ReportProgress(
        Action<ArchiveProgress>? callback,
        string message,
        int percentage,
        long totalRows)
    {
        if (callback == null)
            return;

        callback(new ArchiveProgress
        {
            Message = message,
            ProgressPercentage = percentage,
            RowsProcessed = (long)(totalRows * percentage / 100.0)
        });
    }
}

/// <summary>
/// 归档结果
/// </summary>
public sealed class ArchiveResult
{
    /// <summary>
    /// 是否成功
    /// </summary>
    public bool Success { get; init; }

    /// <summary>
    /// 归档的行数
    /// </summary>
    public long RowsArchived { get; init; }

    /// <summary>
    /// 开始时间
    /// </summary>
    public DateTime StartTime { get; init; }

    /// <summary>
    /// 结束时间
    /// </summary>
    public DateTime EndTime { get; init; }

    /// <summary>
    /// 总耗时
    /// </summary>
    public TimeSpan Duration { get; init; }

    /// <summary>
    /// 结果消息
    /// </summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>
    /// 错误详情
    /// </summary>
    public string? ErrorDetails { get; init; }
}

/// <summary>
/// 归档进度
/// </summary>
public sealed class ArchiveProgress
{
    /// <summary>
    /// 进度消息
    /// </summary>
    public string Message { get; init; } = string.Empty;

    /// <summary>
    /// 进度百分比 (0-100)
    /// </summary>
    public int ProgressPercentage { get; init; }

    /// <summary>
    /// 已处理的行数
    /// </summary>
    public long RowsProcessed { get; init; }
}
