using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.Archives;

/// <summary>
/// 归档任务服务实现(供Hangfire调用)
/// </summary>
internal sealed class ArchiveJobService : IArchiveJobService
{
    private readonly ArchiveOrchestrationService _orchestrationService;
    private readonly ILogger<ArchiveJobService> _logger;

    public ArchiveJobService(
        ArchiveOrchestrationService orchestrationService,
        ILogger<ArchiveJobService> logger)
    {
        _orchestrationService = orchestrationService;
        _logger = logger;
    }

    /// <summary>
    /// 执行单个归档任务
    /// </summary>
    public async Task<ArchiveExecutionResult> ExecuteArchiveJobAsync(Guid configurationId)
    {
        _logger.LogInformation("Hangfire 归档任务开始: {ConfigId}", configurationId);

        try
        {
            var result = await _orchestrationService.ExecuteArchiveAsync(
                configurationId,
                partitionNumber: null,
                progressCallback: null,
                CancellationToken.None);

            if (result.Success)
            {
                _logger.LogInformation(
                    "Hangfire 归档任务成功: {ConfigId}, 归档 {Rows} 行, 耗时 {Duration}",
                    configurationId,
                    result.RowsArchived,
                    result.Duration);
            }
            else
            {
                _logger.LogWarning(
                    "Hangfire 归档任务失败: {ConfigId}, 原因: {Message}",
                    configurationId,
                    result.Message);
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Hangfire 归档任务异常: {ConfigId}", configurationId);
            
            return new ArchiveExecutionResult
            {
                Success = false,
                ConfigurationId = configurationId,
                Message = $"归档任务执行异常: {ex.Message}",
                ErrorDetails = ex.ToString(),
                StartTimeUtc = DateTime.UtcNow,
                EndTimeUtc = DateTime.UtcNow
            };
        }
    }

    /// <summary>
    /// 批量执行归档任务
    /// </summary>
    public async Task<BatchArchiveExecutionResult> ExecuteBatchArchiveJobAsync(List<Guid> configurationIds)
    {
        _logger.LogInformation("Hangfire 批量归档任务开始: {Count} 个配置", configurationIds.Count);

        try
        {
            var result = await _orchestrationService.ExecuteBatchArchiveAsync(
                configurationIds,
                CancellationToken.None);

            _logger.LogInformation(
                "Hangfire 批量归档任务完成: 成功 {Success}/{Total}, 归档 {TotalRows} 行, 耗时 {Duration}",
                result.SuccessCount,
                result.TotalTasks,
                result.TotalRowsArchived,
                result.Duration);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Hangfire 批量归档任务异常");

            return new BatchArchiveExecutionResult
            {
                TotalTasks = configurationIds.Count,
                SuccessCount = 0,
                FailureCount = configurationIds.Count,
                TotalRowsArchived = 0,
                Results = Array.Empty<ArchiveExecutionResult>(),
                StartTimeUtc = DateTime.UtcNow,
                EndTimeUtc = DateTime.UtcNow,
                Duration = TimeSpan.Zero
            };
        }
    }

    /// <summary>
    /// 执行所有启用的归档任务
    /// </summary>
    public async Task<BatchArchiveExecutionResult> ExecuteAllEnabledArchiveJobsAsync()
    {
        _logger.LogInformation("Hangfire 定时归档任务开始: 执行所有启用的归档配置");

        try
        {
            // 获取所有归档配置(由于取消了定时任务功能,此方法已废弃)
            var configs = await _orchestrationService.GetArchiveConfigurationsAsync(
                dataSourceId: null,
                CancellationToken.None);

            var configIds = configs.Select(c => c.Id).ToList();

            if (configIds.Count == 0)
            {
                _logger.LogInformation("没有启用定时归档的配置,跳过执行");

                return new BatchArchiveExecutionResult
                {
                    TotalTasks = 0,
                    SuccessCount = 0,
                    FailureCount = 0,
                    TotalRowsArchived = 0,
                    Results = Array.Empty<ArchiveExecutionResult>(),
                    StartTimeUtc = DateTime.UtcNow,
                    EndTimeUtc = DateTime.UtcNow,
                    Duration = TimeSpan.Zero
                };
            }

            _logger.LogInformation("找到 {Count} 个启用的归档配置", configIds.Count);

            // 批量执行
            return await ExecuteBatchArchiveJobAsync(configIds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Hangfire 定时归档任务异常");

            return new BatchArchiveExecutionResult
            {
                TotalTasks = 0,
                SuccessCount = 0,
                FailureCount = 0,
                TotalRowsArchived = 0,
                Results = Array.Empty<ArchiveExecutionResult>(),
                StartTimeUtc = DateTime.UtcNow,
                EndTimeUtc = DateTime.UtcNow,
                Duration = TimeSpan.Zero
            };
        }
    }
}
