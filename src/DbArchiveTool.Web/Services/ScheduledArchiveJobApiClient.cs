using DbArchiveTool.Shared.Results;
using DbArchiveTool.Web.Models;
using System.Net.Http.Json;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 定时归档任务 API 客户端
/// 提供定时归档任务的 CRUD 操作、启用/禁用、立即执行、统计查询等功能
/// </summary>
public class ScheduledArchiveJobApiClient
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<ScheduledArchiveJobApiClient> _logger;
    private const string BaseUrl = "/api/v1/scheduled-archive-jobs";

    public ScheduledArchiveJobApiClient(
        HttpClient httpClient,
        ILogger<ScheduledArchiveJobApiClient> logger)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// 获取指定数据源的定时归档任务列表
    /// </summary>
    /// <param name="dataSourceId">数据源ID，null表示查询所有数据源</param>
    /// <param name="isEnabled">是否仅查询启用任务，null表示查询全部</param>
    /// <param name="pageIndex">页码，从1开始</param>
    /// <param name="pageSize">每页条数</param>
    /// <returns>分页的任务列表</returns>
    public async Task<Result<DbArchiveTool.Shared.Results.PagedResult<ScheduledArchiveJobDto>>> GetListAsync(
        Guid? dataSourceId = null,
        bool? isEnabled = null,
        int pageIndex = 1,
        int pageSize = 20)
    {
        try
        {
            var queryParams = new List<string>();

            if (dataSourceId.HasValue)
            {
                queryParams.Add($"dataSourceId={dataSourceId.Value}");
            }

            if (isEnabled.HasValue)
            {
                queryParams.Add($"isEnabled={isEnabled.Value}");
            }

            var url = queryParams.Any() 
                ? $"{BaseUrl}?{string.Join("&", queryParams)}"
                : BaseUrl;
                
            var response = await _httpClient.GetAsync(url);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("获取定时归档任务列表失败: {StatusCode}, {Error}", response.StatusCode, error);
                return Result<DbArchiveTool.Shared.Results.PagedResult<ScheduledArchiveJobDto>>.Failure($"获取任务列表失败: {error}");
            }

            // 后端返回的是 IEnumerable<ScheduledArchiveJobListItemDto>,需要转换为 ScheduledArchiveJobDto 并构造分页结果
            var jobs = await response.Content.ReadFromJsonAsync<List<ScheduledArchiveJobDto>>();
            if (jobs == null)
            {
                return Result<DbArchiveTool.Shared.Results.PagedResult<ScheduledArchiveJobDto>>.Failure("返回数据为空");
            }

            // 在客户端进行分页
            var totalCount = jobs.Count;
            var pagedItems = jobs
                .Skip((pageIndex - 1) * pageSize)
                .Take(pageSize)
                .ToList();

            var pagedResult = new DbArchiveTool.Shared.Results.PagedResult<ScheduledArchiveJobDto>(
                pagedItems,
                totalCount,
                pageIndex,
                pageSize);

            return Result<DbArchiveTool.Shared.Results.PagedResult<ScheduledArchiveJobDto>>.Success(pagedResult);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取定时归档任务列表时发生异常");
            return Result<DbArchiveTool.Shared.Results.PagedResult<ScheduledArchiveJobDto>>.Failure($"获取任务列表失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 根据ID获取定时归档任务详情
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <returns>任务详情</returns>
    public async Task<Result<ScheduledArchiveJobDto>> GetByIdAsync(Guid jobId)
    {
        try
        {
            var url = $"{BaseUrl}/{jobId}";
            var response = await _httpClient.GetAsync(url);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("获取定时归档任务详情失败: {JobId}, {StatusCode}, {Error}", jobId, response.StatusCode, error);
                return Result<ScheduledArchiveJobDto>.Failure($"获取任务详情失败: {error}");
            }

            var job = await response.Content.ReadFromJsonAsync<ScheduledArchiveJobDto>();
            if (job == null)
            {
                return Result<ScheduledArchiveJobDto>.Failure("任务不存在");
            }
            
            return Result<ScheduledArchiveJobDto>.Success(job);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取定时归档任务详情时发生异常: {JobId}", jobId);
            return Result<ScheduledArchiveJobDto>.Failure($"获取任务详情失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 创建定时归档任务
    /// </summary>
    /// <param name="request">创建任务请求</param>
    /// <returns>创建成功的任务ID</returns>
    public async Task<Result<Guid>> CreateAsync(CreateScheduledArchiveJobRequest request)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync(BaseUrl, request);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("创建定时归档任务失败: {StatusCode}, {Error}", response.StatusCode, error);
                return Result<Guid>.Failure($"创建任务失败: {error}");
            }

            // 后端返回的是 ScheduledArchiveJobDto 对象，需要提取 Id
            var dto = await response.Content.ReadFromJsonAsync<ScheduledArchiveJobDto>();
            if (dto == null)
            {
                _logger.LogError("创建定时归档任务成功但无法解析返回结果");
                return Result<Guid>.Failure("创建任务成功但无法获取任务ID");
            }

            _logger.LogInformation("成功创建定时归档任务: {JobId}, 任务名称: {Name}", dto.Id, request.Name);
            return Result<Guid>.Success(dto.Id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建定时归档任务时发生异常: {Name}", request.Name);
            return Result<Guid>.Failure($"创建任务失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 更新定时归档任务
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <param name="request">更新任务请求</param>
    /// <returns>更新结果</returns>
    public async Task<Result> UpdateAsync(Guid jobId, UpdateScheduledArchiveJobRequest request)
    {
        try
        {
            var url = $"{BaseUrl}/{jobId}";
            var response = await _httpClient.PutAsJsonAsync(url, request);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("更新定时归档任务失败: {JobId}, {StatusCode}, {Error}", jobId, response.StatusCode, error);
                return Result.Failure($"更新任务失败: {error}");
            }

            _logger.LogInformation("成功更新定时归档任务: {JobId}", jobId);
            return Result.Success();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "更新定时归档任务时发生异常: {JobId}", jobId);
            return Result.Failure($"更新任务失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 启用定时归档任务
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <returns>启用结果</returns>
    public async Task<Result> EnableAsync(Guid jobId)
    {
        try
        {
            var url = $"{BaseUrl}/{jobId}/enable";
            var response = await _httpClient.PostAsync(url, null);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("启用定时归档任务失败: {JobId}, {StatusCode}, {Error}", jobId, response.StatusCode, error);
                return Result.Failure($"启用任务失败: {error}");
            }

            _logger.LogInformation("成功启用定时归档任务: {JobId}", jobId);
            return Result.Success();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "启用定时归档任务时发生异常: {JobId}", jobId);
            return Result.Failure($"启用任务失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 禁用定时归档任务
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <returns>禁用结果</returns>
    public async Task<Result> DisableAsync(Guid jobId)
    {
        try
        {
            var url = $"{BaseUrl}/{jobId}/disable";
            var response = await _httpClient.PostAsync(url, null);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("禁用定时归档任务失败: {JobId}, {StatusCode}, {Error}", jobId, response.StatusCode, error);
                return Result.Failure($"禁用任务失败: {error}");
            }

            _logger.LogInformation("成功禁用定时归档任务: {JobId}", jobId);
            return Result.Success();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "禁用定时归档任务时发生异常: {JobId}", jobId);
            return Result.Failure($"禁用任务失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 立即执行定时归档任务（手动触发一次）
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <returns>执行结果</returns>
    public async Task<Result> ExecuteAsync(Guid jobId)
    {
        try
        {
            var url = $"{BaseUrl}/{jobId}/execute";
            var response = await _httpClient.PostAsync(url, null);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("立即执行定时归档任务失败: {JobId}, {StatusCode}, {Error}", jobId, response.StatusCode, error);
                return Result.Failure($"执行任务失败: {error}");
            }

            _logger.LogInformation("成功触发立即执行定时归档任务: {JobId}", jobId);
            return Result.Success();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "立即执行定时归档任务时发生异常: {JobId}", jobId);
            return Result.Failure($"执行任务失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 删除定时归档任务（软删除）
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <returns>删除结果</returns>
    public async Task<Result> DeleteAsync(Guid jobId)
    {
        try
        {
            var url = $"{BaseUrl}/{jobId}";
            var response = await _httpClient.DeleteAsync(url);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("删除定时归档任务失败: {JobId}, {StatusCode}, {Error}", jobId, response.StatusCode, error);
                return Result.Failure($"删除任务失败: {error}");
            }

            _logger.LogInformation("成功删除定时归档任务: {JobId}", jobId);
            return Result.Success();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "删除定时归档任务时发生异常: {JobId}", jobId);
            return Result.Failure($"删除任务失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 获取定时归档任务统计信息
    /// </summary>
    /// <param name="jobId">任务ID，null表示查询全局统计</param>
    /// <param name="dataSourceId">数据源ID，用于筛选特定数据源的统计</param>
    /// <returns>统计信息</returns>
    public async Task<Result<ScheduledArchiveJobStatisticsDto>> GetStatisticsAsync(
        Guid? jobId = null,
        Guid? dataSourceId = null)
    {
        try
        {
            // 后端只支持单个任务的统计查询,不支持按数据源汇总
            // 如果只传入 dataSourceId 而没有 jobId,返回空统计
            if (!jobId.HasValue)
            {
                _logger.LogWarning("GetStatisticsAsync 需要 jobId 参数,后端不支持按 dataSourceId 汇总统计");
                
                // 返回空统计对象
                var emptyStats = new ScheduledArchiveJobStatisticsDto();
                
                return Result<ScheduledArchiveJobStatisticsDto>.Success(emptyStats);
            }

            var url = $"{BaseUrl}/{jobId.Value}/statistics";
            var response = await _httpClient.GetAsync(url);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("获取定时归档任务统计失败: {StatusCode}, {Error}", response.StatusCode, error);
                return Result<ScheduledArchiveJobStatisticsDto>.Failure($"获取统计信息失败: {error}");
            }

            var statistics = await response.Content.ReadFromJsonAsync<ScheduledArchiveJobStatisticsDto>();
            return Result<ScheduledArchiveJobStatisticsDto>.Success(statistics!);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取定时归档任务统计时发生异常");
            return Result<ScheduledArchiveJobStatisticsDto>.Failure($"获取统计信息失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 获取任务执行历史记录
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <param name="pageIndex">页码</param>
    /// <param name="pageSize">每页条数</param>
    /// <returns>执行历史分页列表</returns>
    public async Task<Result<DbArchiveTool.Shared.Results.PagedResult<JobExecutionHistoryDto>>> GetExecutionHistoryAsync(
        Guid jobId,
        int pageIndex = 1,
        int pageSize = 20)
    {
        try
        {
            var url = $"{BaseUrl}/{jobId}/executions?pageIndex={pageIndex}&pageSize={pageSize}";
            var response = await _httpClient.GetAsync(url);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("获取任务执行历史失败: {JobId}, {StatusCode}, {Error}", jobId, response.StatusCode, error);
                return Result<DbArchiveTool.Shared.Results.PagedResult<JobExecutionHistoryDto>>.Failure($"获取执行历史失败: {error}");
            }

            var history = await response.Content.ReadFromJsonAsync<DbArchiveTool.Shared.Results.PagedResult<JobExecutionHistoryDto>>();
            return Result<DbArchiveTool.Shared.Results.PagedResult<JobExecutionHistoryDto>>.Success(history!);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取任务执行历史时发生异常: {JobId}", jobId);
            return Result<DbArchiveTool.Shared.Results.PagedResult<JobExecutionHistoryDto>>.Failure($"获取执行历史失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 检查目标表是否存在
    /// </summary>
    /// <param name="dataSourceId">数据源ID</param>
    /// <param name="targetSchemaName">目标架构名</param>
    /// <param name="targetTableName">目标表名</param>
    /// <returns>检查结果</returns>
    public async Task<Result<TargetTableCheckResult>> CheckTargetTableAsync(
        Guid dataSourceId,
        string targetSchemaName,
        string targetTableName)
    {
        try
        {
            var request = new
            {
                DataSourceId = dataSourceId,
                TargetSchemaName = targetSchemaName,
                TargetTableName = targetTableName
            };

            var response = await _httpClient.PostAsJsonAsync($"{BaseUrl}/check-target-table", request);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("检查目标表失败: {StatusCode}, {Error}", response.StatusCode, error);
                return Result<TargetTableCheckResult>.Failure($"检查目标表失败: {error}");
            }

            var result = await response.Content.ReadFromJsonAsync<TargetTableCheckResult>();
            return Result<TargetTableCheckResult>.Success(result!);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "检查目标表时发生异常");
            return Result<TargetTableCheckResult>.Failure($"检查目标表失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 创建目标表
    /// </summary>
    /// <param name="dataSourceId">数据源ID</param>
    /// <param name="sourceSchemaName">源架构名</param>
    /// <param name="sourceTableName">源表名</param>
    /// <param name="targetSchemaName">目标架构名</param>
    /// <param name="targetTableName">目标表名</param>
    /// <returns>创建结果</returns>
    public async Task<Result<TargetTableCreationResult>> CreateTargetTableAsync(
        Guid dataSourceId,
        string sourceSchemaName,
        string sourceTableName,
        string targetSchemaName,
        string targetTableName)
    {
        try
        {
            var request = new
            {
                DataSourceId = dataSourceId,
                SourceSchemaName = sourceSchemaName,
                SourceTableName = sourceTableName,
                TargetSchemaName = targetSchemaName,
                TargetTableName = targetTableName
            };

            var response = await _httpClient.PostAsJsonAsync($"{BaseUrl}/create-target-table", request);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("创建目标表失败: {StatusCode}, {Error}", response.StatusCode, error);
                return Result<TargetTableCreationResult>.Failure($"创建目标表失败: {error}");
            }

            var result = await response.Content.ReadFromJsonAsync<TargetTableCreationResult>();
            return Result<TargetTableCreationResult>.Success(result!);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建目标表时发生异常");
            return Result<TargetTableCreationResult>.Failure($"创建目标表失败: {ex.Message}");
        }
    }

    /// <summary>
    /// 验证目标表结构
    /// </summary>
    /// <param name="dataSourceId">数据源ID</param>
    /// <param name="sourceSchemaName">源架构名</param>
    /// <param name="sourceTableName">源表名</param>
    /// <param name="targetSchemaName">目标架构名</param>
    /// <param name="targetTableName">目标表名</param>
    /// <returns>验证结果</returns>
    public async Task<Result<TargetTableValidationResult>> ValidateTargetTableAsync(
        Guid dataSourceId,
        string sourceSchemaName,
        string sourceTableName,
        string targetSchemaName,
        string targetTableName)
    {
        try
        {
            var request = new
            {
                DataSourceId = dataSourceId,
                SourceSchemaName = sourceSchemaName,
                SourceTableName = sourceTableName,
                TargetSchemaName = targetSchemaName,
                TargetTableName = targetTableName
            };

            var response = await _httpClient.PostAsJsonAsync($"{BaseUrl}/validate-target-table", request);

            if (!response.IsSuccessStatusCode)
            {
                var error = await response.Content.ReadAsStringAsync();
                _logger.LogError("验证目标表结构失败: {StatusCode}, {Error}", response.StatusCode, error);
                return Result<TargetTableValidationResult>.Failure($"验证目标表结构失败: {error}");
            }

            var result = await response.Content.ReadFromJsonAsync<TargetTableValidationResult>();
            return Result<TargetTableValidationResult>.Success(result!);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "验证目标表结构时发生异常");
            return Result<TargetTableValidationResult>.Failure($"验证目标表结构失败: {ex.Message}");
        }
    }
}

/// <summary>
/// 目标表检查结果
/// </summary>
public class TargetTableCheckResult
{
    public bool Exists { get; set; }
    public string TargetSchemaName { get; set; } = string.Empty;
    public string TargetTableName { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
}

/// <summary>
/// 目标表创建结果
/// </summary>
public class TargetTableCreationResult
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public string TargetSchemaName { get; set; } = string.Empty;
    public string TargetTableName { get; set; } = string.Empty;
    public int ColumnCount { get; set; }
    public string? Script { get; set; }
}

/// <summary>
/// 目标表验证结果
/// </summary>
public class TargetTableValidationResult
{
    public bool TargetTableExists { get; set; }
    public bool IsCompatible { get; set; }
    public string Message { get; set; } = string.Empty;
    public int SourceColumnCount { get; set; }
    public int? TargetColumnCount { get; set; }
    public List<string> MissingColumns { get; set; } = new();
    public List<string> TypeMismatchColumns { get; set; } = new();
    public List<string> LengthInsufficientColumns { get; set; } = new();
    public List<string> PrecisionInsufficientColumns { get; set; } = new();
}
