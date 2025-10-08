using System.Net.Http.Json;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 分区信息API客户端
/// </summary>
public class PartitionInfoApiClient
{
    private readonly HttpClient _httpClient;

    public PartitionInfoApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    /// <summary>
    /// 获取数据源的所有分区表
    /// </summary>
    public async Task<List<PartitionTableDto>> GetPartitionTablesAsync(Guid dataSourceId)
    {
        var response = await _httpClient.GetAsync($"api/v1/data-sources/{dataSourceId}/partitions/tables");
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<List<PartitionTableDto>>() ?? new List<PartitionTableDto>();
    }

    /// <summary>
    /// 获取分区表的边界值明细
    /// </summary>
    public async Task<List<PartitionDetailDto>> GetPartitionDetailsAsync(
        Guid dataSourceId,
        string schemaName,
        string tableName)
    {
        var response = await _httpClient.GetAsync($"api/v1/data-sources/{dataSourceId}/partitions/tables/{schemaName}/{tableName}/details");
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<List<PartitionDetailDto>>() ?? new List<PartitionDetailDto>();
    }

    /// <summary>
    /// 获取数据源详细信息
    /// </summary>
    public async Task<DataSourceDto?> GetDataSourceAsync(Guid dataSourceId)
    {
        var response = await _httpClient.GetAsync($"api/v1/archive-data-sources/{dataSourceId}");
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<DataSourceDto>();
    }

    /// <summary>
    /// 更新数据源目标服务器配置
    /// </summary>
    public async Task<bool> UpdateTargetServerConfigAsync(Guid dataSourceId, UpdateTargetServerConfigRequest request)
    {
        var response = await _httpClient.PutAsJsonAsync($"api/v1/archive-data-sources/{dataSourceId}/target-server", request);
        return response.IsSuccessStatusCode;
    }

    /// <summary>
    /// 测试数据库连接
    /// </summary>
    public async Task<bool> TestConnectionAsync(TestConnectionRequest request)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync("api/v1/archive-data-sources/test-connection", request);
            if (response.IsSuccessStatusCode)
            {
                var result = await response.Content.ReadFromJsonAsync<bool>();
                return result;
            }
            return false;
        }
        catch
        {
            return false;
        }
    }
}

/// <summary>
/// 分区表信息DTO
/// </summary>
public class PartitionTableDto
{
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string PartitionFunction { get; set; } = "";
    public string PartitionScheme { get; set; } = "";
    public string PartitionColumn { get; set; } = "";
    public string DataType { get; set; } = "";
    public bool IsRangeRight { get; set; }
    public int TotalPartitions { get; set; }
}

/// <summary>
/// 分区明细信息DTO
/// </summary>
public class PartitionDetailDto
{
    public int PartitionNumber { get; set; }
    public string BoundaryValue { get; set; } = "";
    public string RangeType { get; set; } = "";
    public string FilegroupName { get; set; } = "";
    public long RowCount { get; set; }
    public decimal TotalSpaceMB { get; set; }
    public string DataCompression { get; set; } = "NONE";
}

/// <summary>
/// 数据源信息DTO
/// </summary>
public class DataSourceDto
{
    public Guid Id { get; set; }
    public string? Name { get; set; }
    public string? Description { get; set; }
    public string? ServerAddress { get; set; }
    public int ServerPort { get; set; }
    public string? DatabaseName { get; set; }
    public bool UseIntegratedSecurity { get; set; }
    public string? UserName { get; set; }
    public string? Password { get; set; }
    public bool IsEnabled { get; set; }
    public string? DisplayConnection { get; set; }

    // 目标服务器配置
    public bool UseSourceAsTarget { get; set; } = true;
    public string? TargetServerAddress { get; set; }
    public int TargetServerPort { get; set; } = 1433;
    public string? TargetDatabaseName { get; set; }
    public bool TargetUseIntegratedSecurity { get; set; }
    public string? TargetUserName { get; set; }
    public string? TargetPassword { get; set; }
}

/// <summary>
/// 更新目标服务器配置请求
/// </summary>
public class UpdateTargetServerConfigRequest
{
    /// <summary>是否使用源服务器作为目标服务器</summary>
    public bool UseSourceAsTarget { get; set; } = true;
    /// <summary>目标服务器地址</summary>
    public string? TargetServerAddress { get; set; }
    /// <summary>目标服务器端口</summary>
    public int TargetServerPort { get; set; } = 1433;
    /// <summary>目标数据库名称</summary>
    public string? TargetDatabaseName { get; set; }
    /// <summary>目标服务器是否使用集成身份验证</summary>
    public bool TargetUseIntegratedSecurity { get; set; } = true;
    /// <summary>目标服务器用户名</summary>
    public string? TargetUserName { get; set; }
    /// <summary>目标服务器密码</summary>
    public string? TargetPassword { get; set; }
}

/// <summary>
/// 测试连接请求
/// </summary>
public class TestConnectionRequest
{
    /// <summary>服务器地址</summary>
    public string ServerAddress { get; set; } = "";
    /// <summary>服务器端口</summary>
    public int ServerPort { get; set; } = 1433;
    /// <summary>数据库名称</summary>
    public string DatabaseName { get; set; } = "";
    /// <summary>是否使用集成身份验证</summary>
    public bool UseIntegratedSecurity { get; set; } = true;
    /// <summary>用户名</summary>
    public string? UserName { get; set; }
    /// <summary>密码</summary>
    public string? Password { get; set; }
}
