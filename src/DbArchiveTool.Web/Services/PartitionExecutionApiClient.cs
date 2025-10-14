using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
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

    public async Task<StartPartitionExecutionResponse> StartAsync(StartPartitionExecutionRequestModel request, CancellationToken cancellationToken = default)
    {
        var response = await httpClient.PostAsJsonAsync("api/v1/partition-executions", request, cancellationToken);
        try
        {
            var content = await response.Content.ReadAsStringAsync(cancellationToken);
            using var doc = JsonDocument.Parse(content);

            if (!response.IsSuccessStatusCode)
            {
                var errorMessage = TryGetStringProperty(doc.RootElement, "error")
                    ?? $"创建分区执行任务失败（HTTP {(int)response.StatusCode}）。";
                return StartPartitionExecutionResponse.FromFailure(errorMessage);
            }

            // API 返回的是小写的 taskId: { "taskId": "guid-value" }
            if (TryGetStringProperty(doc.RootElement, "taskId") is { Length: > 0 } taskIdStr
                && Guid.TryParse(taskIdStr, out var taskId))
            {
                return StartPartitionExecutionResponse.FromSuccess(taskId);
            }

            return StartPartitionExecutionResponse.FromFailure("API 返回的任务标识缺失或格式无效。");
        }
        catch (JsonException)
        {
            // JSON解析失败
            return StartPartitionExecutionResponse.FromFailure("无法解析 API 返回内容，请检查后端日志。");
        }
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

    /// <summary>
    /// 获取执行向导上下文。
    /// </summary>
    /// <param name="configId">分区配置ID。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>返回向导所需的配置信息。</returns>
    public Task<ExecutionWizardContextModel?> GetWizardContextAsync(
        Guid configId,
        CancellationToken cancellationToken = default)
    {
        return httpClient.GetFromJsonAsync<ExecutionWizardContextModel>(
            $"api/v1/partition-executions/wizard/context/{configId}",
            cancellationToken);
    }

    private static string? TryGetStringProperty(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var property) || property.ValueKind != JsonValueKind.String)
        {
            return null;
        }

        return property.GetString();
    }
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

/// <summary>分区执行任务发起响应。</summary>
public sealed record StartPartitionExecutionResponse(bool Success, Guid? TaskId, string? Error)
{
    public static StartPartitionExecutionResponse FromSuccess(Guid taskId) => new(true, taskId, null);

    public static StartPartitionExecutionResponse FromFailure(string? error) => new(false, null, error);
}

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

/// <summary>执行向导上下文模型。</summary>
public sealed class ExecutionWizardContextModel
{
    public Guid ConfigurationId { get; set; }
    public Guid DataSourceId { get; set; }
    public string DataSourceName { get; set; } = string.Empty;
    public string FullTableName { get; set; } = string.Empty;
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string PartitionFunctionName { get; set; } = string.Empty;
    public string PartitionSchemeName { get; set; } = string.Empty;
    public string PartitionColumnName { get; set; } = string.Empty;
    public string PartitionColumnType { get; set; } = string.Empty;
    public bool IsRangeRight { get; set; }
    public bool RequirePartitionColumnNotNull { get; set; }
    public string PrimaryFilegroup { get; set; } = "PRIMARY";
    public List<string> AdditionalFilegroups { get; set; } = new();
    public List<PartitionBoundaryModel> Boundaries { get; set; } = new();
    public TableStatisticsModel? TableStatistics { get; set; }
    public IndexInspectionModel IndexInspection { get; set; } = new();
    public string? Remarks { get; set; }
    public string? ExecutionStage { get; set; }
    public bool IsCommitted { get; set; }
}

/// <summary>分区边界模型。</summary>
public sealed class PartitionBoundaryModel
{
    public string SortKey { get; set; } = string.Empty;
    public string RawValue { get; set; } = string.Empty;
    public string DisplayValue { get; set; } = string.Empty;
}

/// <summary>表统计信息模型。</summary>
public sealed class TableStatisticsModel
{
    public bool TableExists { get; set; }
    public long TotalRows { get; set; }
    public decimal DataSizeMB { get; set; }
    public decimal IndexSizeMB { get; set; }
    public decimal TotalSizeMB { get; set; }
    public int IndexCount { get; set; }
    public bool IsPartitioned { get; set; }
    public int PartitionCount { get; set; }
    public DateTime? LastUpdated { get; set; }
}

public sealed class IndexInspectionModel
{
    public bool HasClusteredIndex { get; set; }
    public string? ClusteredIndexName { get; set; }
    public bool ClusteredIndexContainsPartitionColumn { get; set; }
    public List<string> ClusteredIndexKeyColumns { get; set; } = new();
    public List<IndexAlignmentItemModel> UniqueIndexes { get; set; } = new();
    public List<IndexAlignmentItemModel> IndexesNeedingAlignment { get; set; } = new();
    public bool HasExternalForeignKeys { get; set; }
    public List<string> ExternalForeignKeys { get; set; } = new();
    public bool CanAutoAlign { get; set; }
    public string? BlockingReason { get; set; }
}

public sealed class IndexAlignmentItemModel
{
    public string IndexName { get; set; } = string.Empty;
    public bool IsClustered { get; set; }
    public bool IsPrimaryKey { get; set; }
    public bool IsUniqueConstraint { get; set; }
    public bool IsUnique { get; set; }
    public bool ContainsPartitionColumn { get; set; }
    public List<string> KeyColumns { get; set; } = new();
}
