using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Shared.Results;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Mvc;
using PartitionCommandPreviewDto = DbArchiveTool.Application.Partitions.PartitionCommandPreviewDto;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 调用 API 获取分区概览、安全信息与命令操作。
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

    public async Task<Result<PartitionMetadataDto>> GetMetadataAsync(Guid dataSourceId, string schema, string table, CancellationToken cancellationToken = default)
    {
        var url = $"api/v1/archive-data-sources/{dataSourceId}/partitions/metadata?schema={Uri.EscapeDataString(schema)}&table={Uri.EscapeDataString(table)}";

        try
        {
            var response = await httpClient.GetAsync(url, cancellationToken);
            if (response.IsSuccessStatusCode)
            {
                var dto = await response.Content.ReadFromJsonAsync<PartitionMetadataDto>(cancellationToken: cancellationToken);
                if (dto is null)
                {
                    return Result<PartitionMetadataDto>.Failure("服务端返回空的分区元数据信息。");
                }

                return Result<PartitionMetadataDto>.Success(dto);
            }

            // 先读取响应文本
            var responseText = await response.Content.ReadAsStringAsync(cancellationToken);
            
            // 尝试从文本解析 ProblemDetails
            try
            {
                var problem = System.Text.Json.JsonSerializer.Deserialize<HttpValidationProblemDetails>(responseText);
                if (!string.IsNullOrWhiteSpace(problem?.Detail))
                {
                    return Result<PartitionMetadataDto>.Failure(problem.Detail);
                }
            }
            catch
            {
                // ProblemDetails 解析失败，使用原始响应文本
            }

            var errorMessage = string.IsNullOrWhiteSpace(responseText)
                ? $"请求失败 ({(int)response.StatusCode} {response.ReasonPhrase})"
                : responseText;

            return Result<PartitionMetadataDto>.Failure(errorMessage);
        }
        catch (Exception ex)
        {
            return Result<PartitionMetadataDto>.Failure($"调用分区元数据接口失败: {ex.Message}");
        }
    }

    public async Task<Result<PartitionCommandPreviewDto>> PreviewSplitAsync(Guid dataSourceId, SplitPartitionRequest request, CancellationToken cancellationToken = default)
    {
        var url = $"api/v1/archive-data-sources/{dataSourceId}/partition-commands/preview";
        var payload = new SplitPartitionApiRequest(request.SchemaName, request.TableName, request.Boundaries, request.BackupConfirmed, request.RequestedBy);
        var response = await httpClient.PostAsJsonAsync(url, payload, cancellationToken);
        if (response.IsSuccessStatusCode)
        {
            var dto = await response.Content.ReadFromJsonAsync<PartitionCommandPreviewDto>(cancellationToken: cancellationToken);
            if (dto is null)
            {
                return Result<PartitionCommandPreviewDto>.Failure("未获取到预览脚本内容。");
            }

            return Result<PartitionCommandPreviewDto>.Success(dto);
        }

        var problem = await response.Content.ReadFromJsonAsync<HttpValidationProblemDetails>(cancellationToken: cancellationToken);
        return Result<PartitionCommandPreviewDto>.Failure(problem?.Detail ?? "预览拆分命令失败");
    }

    public async Task<Result<Guid>> ExecuteSplitAsync(Guid dataSourceId, SplitPartitionRequest request, CancellationToken cancellationToken = default)
    {
        var url = $"api/v1/archive-data-sources/{dataSourceId}/partition-commands/split";
        var payload = new SplitPartitionApiRequest(request.SchemaName, request.TableName, request.Boundaries, request.BackupConfirmed, request.RequestedBy);
        var response = await httpClient.PostAsJsonAsync(url, payload, cancellationToken);
        if (response.IsSuccessStatusCode)
        {
            var id = await response.Content.ReadFromJsonAsync<Guid>(cancellationToken: cancellationToken);
            if (id == Guid.Empty)
            {
                return Result<Guid>.Failure("接口未返回有效的命令标识。");
            }

            return Result<Guid>.Success(id);
        }

        var problem = await response.Content.ReadFromJsonAsync<HttpValidationProblemDetails>(cancellationToken: cancellationToken);
        return Result<Guid>.Failure(problem?.Detail ?? "创建拆分命令失败");
    }

    public async Task<Result> ApproveAsync(Guid dataSourceId, Guid commandId, string approver, CancellationToken cancellationToken = default)
    {
        var url = $"api/v1/archive-data-sources/{dataSourceId}/partition-commands/{commandId}/approve";
        var response = await httpClient.PostAsJsonAsync(url, new ApprovePartitionCommandApiRequest(approver), cancellationToken);
        return response.IsSuccessStatusCode ? Result.Success() : Result.Failure("命令审批失败");
    }

    public async Task<Result> RejectAsync(Guid dataSourceId, Guid commandId, string approver, string reason, CancellationToken cancellationToken = default)
    {
        var url = $"api/v1/archive-data-sources/{dataSourceId}/partition-commands/{commandId}/reject";
        var response = await httpClient.PostAsJsonAsync(url, new RejectPartitionCommandApiRequest(approver, reason), cancellationToken);
        return response.IsSuccessStatusCode ? Result.Success() : Result.Failure("命令拒绝失败");
    }

    public async Task<Result> AddBoundaryAsync(Guid dataSourceId, AddPartitionBoundaryApiRequest request, CancellationToken cancellationToken = default)
    {
        var url = $"api/v1/archive-data-sources/{dataSourceId}/partitions/boundaries";

        try
        {
            var response = await httpClient.PostAsJsonAsync(url, request, cancellationToken);
            if (response.IsSuccessStatusCode)
            {
                return Result.Success();
            }

            var problem = await response.Content.ReadFromJsonAsync<HttpValidationProblemDetails>(cancellationToken: cancellationToken);
            if (!string.IsNullOrWhiteSpace(problem?.Detail))
            {
                return Result.Failure(problem.Detail);
            }

            var responseText = await response.Content.ReadAsStringAsync(cancellationToken);
            var fallback = string.IsNullOrWhiteSpace(responseText)
                ? $"添加分区边界失败 ({(int)response.StatusCode} {response.ReasonPhrase})"
                : responseText;

            return Result.Failure(fallback);
        }
        catch (Exception ex)
        {
            return Result.Failure($"调用添加分区边界接口失败: {ex.Message}");
        }
    }
}

file sealed record SplitPartitionApiRequest(string SchemaName, string TableName, IReadOnlyList<string> Boundaries, bool BackupConfirmed, string RequestedBy);

file sealed record ApprovePartitionCommandApiRequest(string Approver);

file sealed record RejectPartitionCommandApiRequest(string Approver, string Reason);

/// <summary>
/// 添加分区边界的请求负载。
/// </summary>
public sealed record AddPartitionBoundaryApiRequest(
    string SchemaName,
    string TableName,
    string BoundaryValue,
    string? FilegroupName,
    string? RequestedBy,
    string? Notes);
