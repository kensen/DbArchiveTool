using DbArchiveTool.Shared.Results;
using DbArchiveTool.Web.Models;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 定时归档任务状态管理服务（Scoped生命周期，用于缓存和状态同步）
/// </summary>
public class ScheduledArchiveJobState
{
    private readonly ScheduledArchiveJobApiClient _apiClient;
    private readonly ILogger<ScheduledArchiveJobState> _logger;

    /// <summary>
    /// 当前加载的任务列表缓存（按数据源分组）
    /// </summary>
    private Dictionary<Guid, List<ScheduledArchiveJobDto>> _cachedJobsByDataSource = new();

    /// <summary>
    /// 缓存的统计信息（按数据源）
    /// </summary>
    private Dictionary<Guid, ScheduledArchiveJobStatisticsDto> _cachedStatistics = new();

    /// <summary>
    /// 缓存过期时间（秒）
    /// </summary>
    private const int CacheExpirationSeconds = 30;

    /// <summary>
    /// 缓存时间戳
    /// </summary>
    private Dictionary<Guid, DateTime> _cacheTimestamps = new();

    /// <summary>
    /// 状态变更事件：当任务列表更新时触发（用于 UI 自动刷新）
    /// </summary>
    public event Action? OnStateChanged;

    public ScheduledArchiveJobState(
        ScheduledArchiveJobApiClient apiClient,
        ILogger<ScheduledArchiveJobState> logger)
    {
        _apiClient = apiClient;
        _logger = logger;
    }

    /// <summary>
    /// 获取指定数据源的任务列表（优先返回缓存）
    /// </summary>
    public async Task<Result<List<ScheduledArchiveJobDto>>> GetJobsAsync(
        Guid dataSourceId,
        bool forceRefresh = false)
    {
        // 检查缓存是否有效
        if (!forceRefresh && IsCacheValid(dataSourceId))
        {
            _logger.LogDebug("从缓存返回数据源 {DataSourceId} 的任务列表", dataSourceId);
            return Result<List<ScheduledArchiveJobDto>>.Success(
                _cachedJobsByDataSource.GetValueOrDefault(dataSourceId, new List<ScheduledArchiveJobDto>()));
        }

        // 从 API 刷新
        _logger.LogInformation("从 API 刷新数据源 {DataSourceId} 的任务列表", dataSourceId);
        var result = await _apiClient.GetListAsync(dataSourceId: dataSourceId, pageSize: 1000);

        if (result.IsSuccess && result.Value != null)
        {
            _cachedJobsByDataSource[dataSourceId] = result.Value.Items.ToList();
            _cacheTimestamps[dataSourceId] = DateTime.UtcNow;
            NotifyStateChanged();
        }

        return result.IsSuccess
            ? Result<List<ScheduledArchiveJobDto>>.Success(result.Value!.Items.ToList())
            : Result<List<ScheduledArchiveJobDto>>.Failure(result.Error!);
    }

    /// <summary>
    /// 获取指定数据源的统计信息（优先返回缓存）
    /// </summary>
    public async Task<Result<ScheduledArchiveJobStatisticsDto>> GetStatisticsAsync(
        Guid dataSourceId,
        bool forceRefresh = false)
    {
        // 检查缓存是否有效
        if (!forceRefresh && IsCacheValid(dataSourceId) && _cachedStatistics.ContainsKey(dataSourceId))
        {
            _logger.LogDebug("从缓存返回数据源 {DataSourceId} 的统计信息", dataSourceId);
            return Result<ScheduledArchiveJobStatisticsDto>.Success(_cachedStatistics[dataSourceId]);
        }

        // 从 API 刷新
        _logger.LogInformation("从 API 刷新数据源 {DataSourceId} 的统计信息", dataSourceId);
        var result = await _apiClient.GetStatisticsAsync(dataSourceId: dataSourceId);

        if (result.IsSuccess && result.Value != null)
        {
            _cachedStatistics[dataSourceId] = result.Value;
        }

        return result;
    }

    /// <summary>
    /// 添加或更新任务到缓存（用于创建/编辑后的本地更新，避免立即刷新 API）
    /// </summary>
    public void UpsertJobInCache(Guid dataSourceId, ScheduledArchiveJobDto job)
    {
        if (!_cachedJobsByDataSource.ContainsKey(dataSourceId))
        {
            _cachedJobsByDataSource[dataSourceId] = new List<ScheduledArchiveJobDto>();
        }

        var jobs = _cachedJobsByDataSource[dataSourceId];
        var existingIndex = jobs.FindIndex(j => j.Id == job.Id);

        if (existingIndex >= 0)
        {
            // 更新现有任务
            jobs[existingIndex] = job;
            _logger.LogDebug("更新缓存中的任务 {JobId}", job.Id);
        }
        else
        {
            // 添加新任务
            jobs.Add(job);
            _logger.LogDebug("添加新任务 {JobId} 到缓存", job.Id);
        }

        NotifyStateChanged();
    }

    /// <summary>
    /// 从缓存中移除任务（用于删除后的本地更新）
    /// </summary>
    public void RemoveJobFromCache(Guid dataSourceId, Guid jobId)
    {
        if (_cachedJobsByDataSource.ContainsKey(dataSourceId))
        {
            var jobs = _cachedJobsByDataSource[dataSourceId];
            var removed = jobs.RemoveAll(j => j.Id == jobId);

            if (removed > 0)
            {
                _logger.LogDebug("从缓存中移除任务 {JobId}", jobId);
                NotifyStateChanged();
            }
        }
    }

    /// <summary>
    /// 更新单个任务的启用状态（用于快速本地更新，无需重载列表）
    /// </summary>
    public void UpdateJobEnabledState(Guid dataSourceId, Guid jobId, bool isEnabled)
    {
        if (_cachedJobsByDataSource.ContainsKey(dataSourceId))
        {
            var jobs = _cachedJobsByDataSource[dataSourceId];
            var job = jobs.FirstOrDefault(j => j.Id == jobId);

            if (job != null)
            {
                // 通过 record 的 with 表达式更新
                var updatedJob = job with { IsEnabled = isEnabled };
                var index = jobs.IndexOf(job);
                jobs[index] = updatedJob;

                _logger.LogDebug("更新任务 {JobId} 启用状态为 {IsEnabled}", jobId, isEnabled);
                NotifyStateChanged();
            }
        }
    }

    /// <summary>
    /// 清除指定数据源的缓存
    /// </summary>
    public void ClearCache(Guid dataSourceId)
    {
        _cachedJobsByDataSource.Remove(dataSourceId);
        _cachedStatistics.Remove(dataSourceId);
        _cacheTimestamps.Remove(dataSourceId);
        _logger.LogDebug("清除数据源 {DataSourceId} 的缓存", dataSourceId);
        NotifyStateChanged();
    }

    /// <summary>
    /// 清除所有缓存
    /// </summary>
    public void ClearAllCache()
    {
        _cachedJobsByDataSource.Clear();
        _cachedStatistics.Clear();
        _cacheTimestamps.Clear();
        _logger.LogInformation("清除所有定时归档任务缓存");
        NotifyStateChanged();
    }

    /// <summary>
    /// 检查缓存是否有效
    /// </summary>
    private bool IsCacheValid(Guid dataSourceId)
    {
        if (!_cacheTimestamps.ContainsKey(dataSourceId))
        {
            return false;
        }

        var cacheAge = (DateTime.UtcNow - _cacheTimestamps[dataSourceId]).TotalSeconds;
        return cacheAge < CacheExpirationSeconds;
    }

    /// <summary>
    /// 通知状态已变更（触发 UI 更新）
    /// </summary>
    private void NotifyStateChanged()
    {
        OnStateChanged?.Invoke();
    }

    /// <summary>
    /// 获取所有缓存的任务数量（用于调试）
    /// </summary>
    public int GetCachedJobCount()
    {
        return _cachedJobsByDataSource.Values.Sum(jobs => jobs.Count);
    }
}
