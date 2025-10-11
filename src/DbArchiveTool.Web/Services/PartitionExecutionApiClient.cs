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

    /// <summary>
    /// 获取任务日志（分页）。
    /// </summary>
    /// <param name="taskId">任务标识。</param>
    /// <param name="pageIndex">页码（从 1 开始），默认 1。</param>
    /// <param name="pageSize">每页记录数，默认 20。</param>
    /// <param name="category">可选的日志分类过滤（Info、Warning、Error、Step、Cancel 等）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>返回分页后的日志响应，包含总数和日志列表。</returns>
    public Task<GetLogsPagedResponse?> GetLogsAsync(
        Guid taskId,
        int pageIndex = 1,
        int pageSize = 20,
        string? category = null,
        CancellationToken cancellationToken = default)
    {
        var query = $"?pageIndex={pageIndex}&pageSize={pageSize}";
        if (!string.IsNullOrWhiteSpace(category))
        {
            query += $"&category={Uri.EscapeDataString(category)}";
        }

        return httpClient.GetFromJsonAsync<GetLogsPagedResponse>($"api/v1/partition-executions/{taskId}/logs{query}", cancellationToken);
    }

    /// <summary>
    /// 取消分区执行任务。
    /// </summary>
    /// <param name="taskId">任务标识。</param>
    /// <param name="cancelledBy">取消人。</param>
    /// <param name="reason">取消原因（可选）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>成功时返回 true，失败时返回 false。</returns>
    public async Task<bool> CancelTaskAsync(
        Guid taskId,
        string cancelledBy,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        var request = new CancelPartitionExecutionRequestModel(cancelledBy, reason);
        var response = await httpClient.PostAsJsonAsync($"api/v1/partition-executions/{taskId}/cancel", request, cancellationToken);
        return response.IsSuccessStatusCode;
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

/// <summary>取消执行的请求模型。</summary>
public sealed record CancelPartitionExecutionRequestModel(
    string CancelledBy,
    string? Reason = null);

/// <summary>日志分页响应模型。</summary>
public sealed class GetLogsPagedResponse
{
    public int PageIndex { get; set; }
    public int PageSize { get; set; }
    public int TotalCount { get; set; }
    public List<PartitionExecutionLogModel> Items { get; set; } = new();
}

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
