using DbArchiveTool.Application.Archives;
using DbArchiveTool.Domain.ArchiveConfigurations;
using DbArchiveTool.Domain.DataSources;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 归档执行器适配器
/// 将 OptimizedPartitionArchiveExecutor 适配为 IArchiveExecutor 接口
/// </summary>
public sealed class ArchiveExecutorAdapter : IArchiveExecutor
{
    private readonly OptimizedPartitionArchiveExecutor _partitionExecutor;
    private readonly ILogger<ArchiveExecutorAdapter> _logger;

    public ArchiveExecutorAdapter(
        OptimizedPartitionArchiveExecutor partitionExecutor,
        ILogger<ArchiveExecutorAdapter> logger)
    {
        _partitionExecutor = partitionExecutor;
        _logger = logger;
    }

    /// <summary>
    /// 执行归档
    /// </summary>
    public async Task<ArchiveExecutionResult> ExecuteAsync(
        ArchiveConfiguration config,
        ArchiveDataSource dataSource,
        string targetConnectionString,
        int? partitionNumber = null,
        Action<ArchiveProgressInfo>? progressCallback = null,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;

        try
        {
            // 验证分区表归档必须指定分区号
            if (config.IsPartitionedTable && !partitionNumber.HasValue)
            {
                return new ArchiveExecutionResult
                {
                    Success = false,
                    ConfigurationId = config.Id,
                    ConfigurationName = config.Name,
                    SourceSchemaName = config.SourceSchemaName,
                    SourceTableName = config.SourceTableName,
                    ArchiveMethod = config.ArchiveMethod,
                    Message = "分区表归档必须指定分区号",
                    StartTimeUtc = startTime,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            // 普通表归档暂不支持
            if (!config.IsPartitionedTable)
            {
                return new ArchiveExecutionResult
                {
                    Success = false,
                    ConfigurationId = config.Id,
                    ConfigurationName = config.Name,
                    SourceSchemaName = config.SourceSchemaName,
                    SourceTableName = config.SourceTableName,
                    ArchiveMethod = config.ArchiveMethod,
                    Message = "普通表归档功能暂未实现",
                    StartTimeUtc = startTime,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            // 执行优化的分区归档
            var result = await _partitionExecutor.ExecuteAsync(
                config,
                dataSource,
                targetConnectionString,
                partitionNumber!.Value,
                progress =>
                {
                    progressCallback?.Invoke(new ArchiveProgressInfo
                    {
                        Message = progress.Message,
                        ProgressPercentage = progress.ProgressPercentage,
                        RowsProcessed = progress.RowsProcessed
                    });
                },
                cancellationToken);

            // 转换结果
            return new ArchiveExecutionResult
            {
                Success = result.Success,
                ConfigurationId = config.Id,
                ConfigurationName = config.Name,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                PartitionNumber = partitionNumber,
                ArchiveMethod = config.ArchiveMethod,
                RowsArchived = result.RowsArchived,
                StartTimeUtc = result.StartTime,
                EndTimeUtc = result.EndTime,
                Message = result.Message,
                ErrorDetails = result.ErrorDetails
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "归档执行失败: ConfigId={ConfigId}", config.Id);

            return new ArchiveExecutionResult
            {
                Success = false,
                ConfigurationId = config.Id,
                ConfigurationName = config.Name,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                PartitionNumber = partitionNumber,
                ArchiveMethod = config.ArchiveMethod,
                Message = $"归档执行失败: {ex.Message}",
                ErrorDetails = ex.ToString(),
                StartTimeUtc = startTime,
                EndTimeUtc = DateTime.UtcNow
            };
        }
    }
}
