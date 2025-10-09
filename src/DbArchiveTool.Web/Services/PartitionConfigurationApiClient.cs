using System;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 调用分区配置 API 的客户端。
/// </summary>
public sealed class PartitionConfigurationApiClient
{
    private readonly HttpClient httpClient;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() } // 不使用 camelCase，与后端保持一致
    };

    public PartitionConfigurationApiClient(HttpClient httpClient)
    {
        this.httpClient = httpClient;
    }

    public async Task<Result<Guid>> CreateAsync(CreatePartitionConfigurationRequestModel request)
    {
        var response = await httpClient.PostAsJsonAsync("api/v1/partition-configurations", request, JsonOptions);
        if (!response.IsSuccessStatusCode)
        {
            return await ReadFailureAsync<Result<Guid>>(response)
                   ?? Result<Guid>.Failure($"创建分区配置失败，HTTP {response.StatusCode}。");
        }

        return await ReadSuccessAsync<Result<Guid>>(response)
               ?? Result<Guid>.Failure("创建分区配置成功，但解析响应失败。");
    }

    public async Task<Result> ReplaceValuesAsync(Guid configurationId, ReplacePartitionValuesRequestModel request)
    {
        var response = await httpClient.PostAsJsonAsync($"api/v1/partition-configurations/{configurationId}/values", request, JsonOptions);
        if (!response.IsSuccessStatusCode)
        {
            return await ReadFailureAsync<Result>(response)
                   ?? Result.Failure($"更新分区值失败，HTTP {response.StatusCode}。");
        }

        return await ReadSuccessAsync<Result>(response)
               ?? Result.Failure("更新分区值成功，但解析响应失败。");
    }

    private static async Task<T?> ReadSuccessAsync<T>(HttpResponseMessage response)
    {
        try
        {
            return await response.Content.ReadFromJsonAsync<T>(JsonOptions);
        }
        catch
        {
            return default;
        }
    }

    private static async Task<T?> ReadFailureAsync<T>(HttpResponseMessage response)
    {
        try
        {
            return await response.Content.ReadFromJsonAsync<T>(JsonOptions);
        }
        catch
        {
            return default;
        }
    }
}

