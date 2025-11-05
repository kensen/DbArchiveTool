using System.Net.Http.Json;
using System.Text.Json;
using DbArchiveTool.Shared.Archive;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 归档配置 API 客户端
/// </summary>
public class ArchiveConfigurationApiClient
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<ArchiveConfigurationApiClient> _logger;

    public ArchiveConfigurationApiClient(
        HttpClient httpClient,
        ILogger<ArchiveConfigurationApiClient> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    /// <summary>
    /// 获取所有归档配置
    /// </summary>
    public async Task<List<ArchiveConfigurationListItemModel>> GetAllAsync(
        Guid? dataSourceId = null,
        bool? isEnabled = null)
    {
        try
        {
            var queryParams = new List<string>();
            if (dataSourceId.HasValue)
                queryParams.Add($"dataSourceId={dataSourceId.Value}");
            if (isEnabled.HasValue)
                queryParams.Add($"isEnabled={isEnabled.Value}");

            var query = queryParams.Count > 0 ? "?" + string.Join("&", queryParams) : "";
            var response = await _httpClient.GetFromJsonAsync<List<ArchiveConfigurationListItemModel>>(
                $"/api/v1/archive-configurations{query}");

            return response ?? new List<ArchiveConfigurationListItemModel>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取归档配置列表失败");
            throw;
        }
    }

    /// <summary>
    /// 根据ID获取归档配置详情
    /// </summary>
    public async Task<ArchiveConfigurationDetailModel?> GetByIdAsync(Guid id)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<ArchiveConfigurationDetailModel>(
                $"/api/v1/archive-configurations/{id}");
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取归档配置详情失败: {Id}", id);
            throw;
        }
    }

    /// <summary>
    /// 创建归档配置
    /// </summary>
    public async Task<ArchiveConfigurationDetailModel> CreateAsync(CreateArchiveConfigurationModel model)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync("/api/v1/archive-configurations", model);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadFromJsonAsync<ArchiveConfigurationDetailModel>()
                ?? throw new Exception("返回数据为空");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建归档配置失败");
            throw;
        }
    }

    /// <summary>
    /// 更新归档配置
    /// </summary>
    public async Task UpdateAsync(Guid id, UpdateArchiveConfigurationModel model)
    {
        try
        {
            var response = await _httpClient.PutAsJsonAsync($"/api/v1/archive-configurations/{id}", model);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "更新归档配置失败: {Id}", id);
            throw;
        }
    }

    /// <summary>
    /// 删除归档配置
    /// </summary>
    public async Task DeleteAsync(Guid id)
    {
        try
        {
            var response = await _httpClient.DeleteAsync($"/api/v1/archive-configurations/{id}");
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "删除归档配置失败: {Id}", id);
            throw;
        }
    }

    /// <summary>
    /// 启用归档配置
    /// </summary>
    public async Task EnableAsync(Guid id)
    {
        try
        {
            var response = await _httpClient.PostAsync($"/api/v1/archive-configurations/{id}/enable", null);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "启用归档配置失败: {Id}", id);
            throw;
        }
    }

    /// <summary>
    /// 禁用归档配置
    /// </summary>
    public async Task DisableAsync(Guid id)
    {
        try
        {
            var response = await _httpClient.PostAsync($"/api/v1/archive-configurations/{id}/disable", null);
            response.EnsureSuccessStatusCode();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "禁用归档配置失败: {Id}", id);
            throw;
        }
    }
}

/// <summary>
/// 归档配置列表项模型
/// </summary>
public class ArchiveConfigurationListItemModel
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public Guid DataSourceId { get; set; }
    public string SourceSchemaName { get; set; } = string.Empty;
    public string SourceTableName { get; set; } = string.Empty;
    public bool IsPartitionedTable { get; set; }
    public ArchiveMethod ArchiveMethod { get; set; }
    public bool IsEnabled { get; set; }
    public DateTime? LastExecutionTimeUtc { get; set; }
    public string? LastExecutionStatus { get; set; }
    public long? LastArchivedRowCount { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

/// <summary>
/// 归档配置详情模型
/// </summary>
public class ArchiveConfigurationDetailModel
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public Guid DataSourceId { get; set; }
    public string SourceSchemaName { get; set; } = string.Empty;
    public string SourceTableName { get; set; } = string.Empty;
    public bool IsPartitionedTable { get; set; }
    public Guid? PartitionConfigurationId { get; set; }
    public string? ArchiveFilterColumn { get; set; }
    public string? ArchiveFilterCondition { get; set; }
    public ArchiveMethod ArchiveMethod { get; set; }
    public bool DeleteSourceDataAfterArchive { get; set; }
    public int BatchSize { get; set; }
    public bool IsEnabled { get; set; }
    public DateTime? LastExecutionTimeUtc { get; set; }
    public string? LastExecutionStatus { get; set; }
    public long? LastArchivedRowCount { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public string CreatedBy { get; set; } = string.Empty;
    public DateTime UpdatedAtUtc { get; set; }
    public string UpdatedBy { get; set; } = string.Empty;
}

/// <summary>
/// 创建归档配置模型
/// </summary>
public class CreateArchiveConfigurationModel
{
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public Guid DataSourceId { get; set; }
    public string SourceSchemaName { get; set; } = "dbo";
    public string SourceTableName { get; set; } = string.Empty;
    public bool IsPartitionedTable { get; set; }
    public Guid? PartitionConfigurationId { get; set; }
    public string? ArchiveFilterColumn { get; set; }
    public string? ArchiveFilterCondition { get; set; }
    public ArchiveMethod ArchiveMethod { get; set; }
    public bool DeleteSourceDataAfterArchive { get; set; } = true;
    public int BatchSize { get; set; } = 10000;
}

/// <summary>
/// 更新归档配置模型
/// </summary>
public class UpdateArchiveConfigurationModel
{
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public Guid DataSourceId { get; set; }
    public string SourceSchemaName { get; set; } = "dbo";
    public string SourceTableName { get; set; } = string.Empty;
    public bool IsPartitionedTable { get; set; }
    public Guid? PartitionConfigurationId { get; set; }
    public string? ArchiveFilterColumn { get; set; }
    public string? ArchiveFilterCondition { get; set; }
    public ArchiveMethod ArchiveMethod { get; set; }
    public bool DeleteSourceDataAfterArchive { get; set; } = true;
    public int BatchSize { get; set; } = 10000;
}
