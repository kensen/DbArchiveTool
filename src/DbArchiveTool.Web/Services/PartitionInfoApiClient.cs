using System;
using System.Net.Http.Json;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 分区信息 API 客户端。
/// </summary>
public class PartitionInfoApiClient
{
    private readonly HttpClient httpClient;

    public PartitionInfoApiClient(HttpClient httpClient)
    {
        this.httpClient = httpClient;
    }

    /// <summary>获取数据源下的分区表。</summary>
    public async Task<List<PartitionTableDto>> GetPartitionTablesAsync(Guid dataSourceId)
    {
        var response = await httpClient.GetAsync($"api/v1/data-sources/{dataSourceId}/partitions/tables");
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<List<PartitionTableDto>>() ?? new List<PartitionTableDto>();
    }

    /// <summary>获取数据源下的全部用户表。</summary>
    public async Task<List<DatabaseTableDto>> GetDatabaseTablesAsync(Guid dataSourceId)
    {
        var response = await httpClient.GetAsync($"api/v1/data-sources/{dataSourceId}/tables");
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<List<DatabaseTableDto>>() ?? new List<DatabaseTableDto>();
    }

    /// <summary>获取指定表的分区详情。</summary>
    public async Task<List<PartitionDetailDto>> GetPartitionDetailsAsync(Guid dataSourceId, string schemaName, string tableName)
    {
        var response = await httpClient.GetAsync($"api/v1/data-sources/{dataSourceId}/partitions/tables/{schemaName}/{tableName}/details");
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<List<PartitionDetailDto>>() ?? new List<PartitionDetailDto>();
    }

    /// <summary>获取数据源基本信息。</summary>
    public async Task<DataSourceDto?> GetDataSourceAsync(Guid dataSourceId)
    {
        var response = await httpClient.GetAsync($"api/v1/archive-data-sources/{dataSourceId}");
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<DataSourceDto>();
    }

    /// <summary>获取目标服务器数据库列表。</summary>
    public async Task<List<TargetDatabaseDto>> GetTargetDatabasesAsync(Guid dataSourceId)
    {
        var response = await httpClient.GetAsync($"api/v1/data-sources/{dataSourceId}/partitions/target-databases");
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<List<TargetDatabaseDto>>() ?? new List<TargetDatabaseDto>();
    }

    /// <summary>获取指定表的列信息。</summary>
    public async Task<List<PartitionTableColumnDto>> GetTableColumnsAsync(Guid dataSourceId, string schemaName, string tableName)
    {
        var url = $"api/v1/data-sources/{dataSourceId}/tables/{Uri.EscapeDataString(schemaName)}/{Uri.EscapeDataString(tableName)}/columns";
        var response = await httpClient.GetAsync(url);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<List<PartitionTableColumnDto>>() ?? new List<PartitionTableColumnDto>();
    }

    /// <summary>获取列统计信息。</summary>
    public async Task<PartitionColumnStatisticsDto?> GetColumnStatisticsAsync(Guid dataSourceId, string schemaName, string tableName, string columnName)
    {
        var url =
            $"api/v1/data-sources/{dataSourceId}/tables/{Uri.EscapeDataString(schemaName)}/{Uri.EscapeDataString(tableName)}/columns/{Uri.EscapeDataString(columnName)}/statistics";
        var response = await httpClient.GetAsync(url);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<PartitionColumnStatisticsDto>();
    }

    /// <summary>获取默认数据文件目录。</summary>
    public async Task<string?> GetDefaultFilePathAsync(Guid dataSourceId)
    {
        var response = await httpClient.GetAsync($"api/v1/data-sources/{dataSourceId}/partitions/default-file-path");
        response.EnsureSuccessStatusCode();
        var payload = await response.Content.ReadFromJsonAsync<DefaultFilePathResponse>();
        return payload?.Path;
    }

    /// <summary>更新目标服务器配置。</summary>
    public async Task<bool> UpdateTargetServerConfigAsync(Guid dataSourceId, UpdateTargetServerConfigRequest request)
    {
        var response = await httpClient.PutAsJsonAsync($"api/v1/archive-data-sources/{dataSourceId}/target-server", request);
        return response.IsSuccessStatusCode;
    }

    /// <summary>测试连接。</summary>
    public async Task<bool> TestConnectionAsync(TestConnectionRequest request)
    {
        try
        {
            var response = await httpClient.PostAsJsonAsync("api/v1/archive-data-sources/test-connection", request);
            if (!response.IsSuccessStatusCode)
            {
                return false;
            }

            return await response.Content.ReadFromJsonAsync<bool>();
        }
        catch
        {
            return false;
        }
    }
}

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

public class PartitionTableColumnDto
{
    public string ColumnName { get; set; } = "";
    public string DataType { get; set; } = "";
    public bool IsNullable { get; set; }
    public int MaxLength { get; set; }
    public int Precision { get; set; }
    public int Scale { get; set; }
    public string DisplayType { get; set; } = "";
}

public class PartitionColumnStatisticsDto
{
    public string? MinValue { get; set; }
    public string? MaxValue { get; set; }
    public long? TotalRows { get; set; }
    public long? DistinctRows { get; set; }
}

public class TargetDatabaseDto
{
    public string Name { get; set; } = "";
    public int DatabaseId { get; set; }
    public bool IsCurrent { get; set; }
}

public class DatabaseTableDto
{
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
}

public class DefaultFilePathResponse
{
    public string? Path { get; set; }
}

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

    public bool UseSourceAsTarget { get; set; } = true;
    public string? TargetServerAddress { get; set; }
    public int TargetServerPort { get; set; } = 1433;
    public string? TargetDatabaseName { get; set; }
    public bool TargetUseIntegratedSecurity { get; set; }
    public string? TargetUserName { get; set; }
    public string? TargetPassword { get; set; }
}

public class UpdateTargetServerConfigRequest
{
    public bool UseSourceAsTarget { get; set; } = true;
    public string? TargetServerAddress { get; set; }
    public int TargetServerPort { get; set; } = 1433;
    public string? TargetDatabaseName { get; set; }
    public bool TargetUseIntegratedSecurity { get; set; } = true;
    public string? TargetUserName { get; set; }
    public string? TargetPassword { get; set; }
}

public class TestConnectionRequest
{
    public string ServerAddress { get; set; } = "";
    public int ServerPort { get; set; } = 1433;
    public string DatabaseName { get; set; } = "";
    public bool UseIntegratedSecurity { get; set; } = true;
    public string? UserName { get; set; }
    public string? Password { get; set; }
}

