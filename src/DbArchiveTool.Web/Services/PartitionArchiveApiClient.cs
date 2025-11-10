using System;
using System.Collections.Generic;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Web.Services;

/// <summary>调用分区归档相关接口的 API 客户端。</summary>
public sealed class PartitionArchiveApiClient
{
    private readonly HttpClient httpClient;
    private readonly ILogger<PartitionArchiveApiClient> logger;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public PartitionArchiveApiClient(HttpClient httpClient, ILogger<PartitionArchiveApiClient> logger)
    {
        this.httpClient = httpClient;
        this.logger = logger;
    }

    /// <summary>检查分区切换是否满足执行条件。</summary>
    public async Task<Result<PartitionSwitchInspectionResultDto>> InspectSwitchAsync(SwitchArchiveInspectRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await httpClient.PostAsJsonAsync("api/v1/partition-archive/switch/inspect", request, JsonOptions, cancellationToken);
            return await ReadResultAsync<PartitionSwitchInspectionResultDto>(response, "检查分区切换", cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "调用分区切换检查接口失败");
            return Result<PartitionSwitchInspectionResultDto>.Failure("请求分区切换检查失败，请稍后重试。");
        }
    }

    /// <summary>执行可自动补齐的步骤。</summary>
    public async Task<Result<PartitionSwitchAutoFixResultDto>> AutoFixSwitchAsync(SwitchArchiveAutoFixRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await httpClient.PostAsJsonAsync("api/v1/partition-archive/switch/autofix", request, JsonOptions, cancellationToken);
            return await ReadResultAsync<PartitionSwitchAutoFixResultDto>(response, "执行自动补齐", cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "调用分区切换自动补齐接口失败");
            return Result<PartitionSwitchAutoFixResultDto>.Failure("执行自动补齐失败，请稍后重试。");
        }
    }

    /// <summary>提交分区切换归档任务。</summary>
    public async Task<Result<Guid>> ArchiveBySwitchAsync(SwitchArchiveExecuteRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await httpClient.PostAsJsonAsync("api/v1/partition-archive/switch", request, JsonOptions, cancellationToken);
            return await ReadResultAsync<Guid>(response, "提交分区切换归档任务", cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "调用分区切换归档接口失败");
            return Result<Guid>.Failure("提交分区切换任务失败，请稍后重试。");
        }
    }

    /// <summary>检查 BCP 归档是否满足执行条件。</summary>
    public async Task<Result<ArchiveInspectionResultDto>> InspectBcpAsync(BcpArchiveInspectRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await httpClient.PostAsJsonAsync("api/v1/partition-archive/bcp/inspect", request, JsonOptions, cancellationToken);
            return await ReadResultAsync<ArchiveInspectionResultDto>(response, "检查 BCP 归档", cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "调用 BCP 归档检查接口失败");
            return Result<ArchiveInspectionResultDto>.Failure("请求 BCP 归档检查失败，请稍后重试。");
        }
    }

    /// <summary>提交 BCP 归档任务。</summary>
    public async Task<Result<Guid>> ArchiveByBcpAsync(BcpArchiveExecuteRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await httpClient.PostAsJsonAsync("api/v1/partition-archive/bcp/execute", request, JsonOptions, cancellationToken);
            return await ReadResultAsync<Guid>(response, "提交 BCP 归档任务", cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "调用 BCP 归档接口失败");
            return Result<Guid>.Failure("提交 BCP 归档任务失败，请稍后重试。");
        }
    }

    /// <summary>执行 BCP/BulkCopy 归档的自动修复。</summary>
    public async Task<Result<string>> ExecuteAutoFixAsync(ArchiveAutoFixRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await httpClient.PostAsJsonAsync("api/v1/partition-archive/autofix", request, JsonOptions, cancellationToken);
            return await ReadResultAsync<string>(response, "执行自动修复", cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "调用自动修复接口失败");
            return Result<string>.Failure("执行自动修复失败，请稍后重试。");
        }
    }

    /// <summary>检查 BulkCopy 归档是否满足执行条件。</summary>
    public async Task<Result<ArchiveInspectionResultDto>> InspectBulkCopyAsync(BulkCopyArchiveInspectRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await httpClient.PostAsJsonAsync("api/v1/partition-archive/bulkcopy/inspect", request, JsonOptions, cancellationToken);
            return await ReadResultAsync<ArchiveInspectionResultDto>(response, "检查 BulkCopy 归档", cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "调用 BulkCopy 归档检查接口失败");
            return Result<ArchiveInspectionResultDto>.Failure("请求 BulkCopy 归档检查失败，请稍后重试。");
        }
    }

    /// <summary>提交 BulkCopy 归档任务。</summary>
    public async Task<Result<Guid>> ArchiveByBulkCopyAsync(BulkCopyArchiveExecuteRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await httpClient.PostAsJsonAsync("api/v1/partition-archive/bulkcopy/execute", request, JsonOptions, cancellationToken);
            return await ReadResultAsync<Guid>(response, "提交 BulkCopy 归档任务", cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "调用 BulkCopy 归档接口失败");
            return Result<Guid>.Failure("提交 BulkCopy 归档任务失败，请稍后重试。");
        }
    }

    private async Task<Result<T>> ReadResultAsync<T>(HttpResponseMessage response, string operationName, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
        {
            try
            {
                var payload = await response.Content.ReadFromJsonAsync<Result<T>>(JsonOptions, cancellationToken);
                if (payload is null)
                {
                    return Result<T>.Failure($"{operationName}返回了空的响应。");
                }

                return payload.IsSuccess
                    ? Result<T>.Success(payload.Value!)
                    : Result<T>.Failure(payload.Error ?? $"{operationName}失败。");
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "解析{Operation}响应失败", operationName);
                return Result<T>.Failure($"解析{operationName}结果时发生错误。");
            }
        }

        try
        {
            var problem = await response.Content.ReadFromJsonAsync<ProblemDetails>(JsonOptions, cancellationToken);
            if (!string.IsNullOrWhiteSpace(problem?.Detail))
            {
                return Result<T>.Failure(problem.Detail);
            }
        }
        catch
        {
            // 忽略 ProblemDetails 解析失败，继续读取原始文本
        }

        var raw = await response.Content.ReadAsStringAsync(cancellationToken);
        var fallback = string.IsNullOrWhiteSpace(raw)
            ? $"{operationName}失败 (HTTP {(int)response.StatusCode})"
            : raw;
        return Result<T>.Failure(fallback);
    }
}
