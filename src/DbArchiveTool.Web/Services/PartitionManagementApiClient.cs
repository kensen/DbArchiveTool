using DbArchiveTool.Application.Partitions;
using System.Net.Http.Json;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 调用 API 获取分区概览与安全信息。
/// </summary>
public sealed class PartitionManagementApiClient
{
    private readonly HttpClient httpClient;

    public PartitionManagementApiClient(HttpClient httpClient)
    {
        this.httpClient = httpClient;
    }

    public async Task<PartitionOverviewDto?> GetOverviewAsync(Guid dataSourceId, string schema, string table, CancellationToken cancellationToken = default)
    {
        var url = $"api/v1/archive-data-sources/{dataSourceId}/partitions/overview?schema={schema}&table={table}";
        return await httpClient.GetFromJsonAsync<PartitionOverviewDto>(url, cancellationToken);
    }

    public async Task<PartitionBoundarySafetyDto?> GetSafetyAsync(Guid dataSourceId, string boundaryKey, string schema, string table, CancellationToken cancellationToken = default)
    {
        var url = $"api/v1/archive-data-sources/{dataSourceId}/partitions/{boundaryKey}/safety?schema={schema}&table={table}";
        return await httpClient.GetFromJsonAsync<PartitionBoundarySafetyDto>(url, cancellationToken);
    }
}
