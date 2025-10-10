using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 调用后端分区执行任务接口的 API 客户端。
/// </summary>
public sealed class PartitionExecutionApiClient
{
    private readonly HttpClient httpClient;

    public PartitionExecutionApiClient(HttpClient httpClient)
    {
        this.httpClient = httpClient;
    }

    public async Task<Guid?> StartAsync(StartPartitionExecutionRequestModel request, CancellationToken cancellationToken = default)
    {
        var response = await httpClient.PostAsJsonAsync("api/v1/partition-executions", request, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            return null;
        }

        var payload = await response.Content.ReadFromJsonAsync<StartExecutionResponse>(cancellationToken: cancellationToken);
        return payload?.TaskId;
    }

    public Task<PartitionExecutionTaskDetailModel?> GetAsync(Guid taskId, CancellationToken cancellationToken = default)
        => httpClient.GetFromJsonAsync<PartitionExecutionTaskDetailModel>($"api/v1/partition-executions/{taskId}", cancellationToken);

    public Task<List<PartitionExecutionTaskSummaryModel>?> ListAsync(Guid? dataSourceId, int maxCount, CancellationToken cancellationToken = default)
    {
        var query = dataSourceId.HasValue ? $"?dataSourceId={dataSourceId.Value}&maxCount={maxCount}" : $"?maxCount={maxCount}";
        return httpClient.GetFromJsonAsync<List<PartitionExecutionTaskSummaryModel>>($"api/v1/partition-executions{query}", cancellationToken);
    }

    public Task<List<PartitionExecutionLogModel>?> GetLogsAsync(Guid taskId, DateTime? sinceUtc, int take, CancellationToken cancellationToken = default)
    {
        var query = $"?take={take}";
        if (sinceUtc.HasValue)
        {
            query += $"&sinceUtc={sinceUtc.Value:O}";
        }

        return httpClient.GetFromJsonAsync<List<PartitionExecutionLogModel>>($"api/v1/partition-executions/{taskId}/logs{query}", cancellationToken);
    }

    private sealed record StartExecutionResponse(Guid TaskId);
}

/// <summary>发起执行的请求模型。</summary>
public sealed record StartPartitionExecutionRequestModel(
    Guid PartitionConfigurationId,
    Guid DataSourceId,
    string RequestedBy,
    bool BackupConfirmed,
    string? BackupReference,
    string? Notes,
    bool ForceWhenWarnings,
    int Priority = 0);

public class PartitionExecutionTaskSummaryModel
{
    public Guid Id { get; set; }
    public Guid PartitionConfigurationId { get; set; }
    public Guid DataSourceId { get; set; }
    public string Status { get; set; } = string.Empty;
    public string Phase { get; set; } = string.Empty;
    public double Progress { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }
    public string RequestedBy { get; set; } = string.Empty;
    public string? FailureReason { get; set; }
    public string? BackupReference { get; set; }
}

public sealed class PartitionExecutionTaskDetailModel : PartitionExecutionTaskSummaryModel
{
    public string? SummaryJson { get; set; }
    public string? Notes { get; set; }
}

public sealed class PartitionExecutionLogModel
{
    public Guid Id { get; set; }
    public Guid ExecutionTaskId { get; set; }
    public DateTime LogTimeUtc { get; set; }
    public string Category { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public long? DurationMs { get; set; }
    public string? ExtraJson { get; set; }
}
