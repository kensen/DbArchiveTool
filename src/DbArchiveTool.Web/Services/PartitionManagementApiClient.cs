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
}

file sealed record SplitPartitionApiRequest(string SchemaName, string TableName, IReadOnlyList<string> Boundaries, bool BackupConfirmed, string RequestedBy);

file sealed record ApprovePartitionCommandApiRequest(string Approver);

file sealed record RejectPartitionCommandApiRequest(string Approver, string Reason);
