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
}
