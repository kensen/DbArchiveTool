using System.Net.Http.Json;
using DbArchiveTool.Application.DataSources;
using DbArchiveTool.Shared.Results;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Web.Services;

/// <summary>归档数据源相关 API 客户端。</summary>
public sealed class ArchiveDataSourceApiClient
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<ArchiveDataSourceApiClient> _logger;

    public ArchiveDataSourceApiClient(IHttpClientFactory httpClientFactory, ILogger<ArchiveDataSourceApiClient> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    /// <summary>获取数据源列表。</summary>
    public async Task<Result<IReadOnlyList<ArchiveDataSourceDto>>> GetAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var client = _httpClientFactory.CreateClient("ArchiveApi");
            const string url = "api/v1/archive-data-sources";
            _logger.LogInformation("[DataSourceApi] GET {Url}", url);
            var response = await client.GetAsync(url, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                var message = await ReadProblemAsync(response, cancellationToken);
                _logger.LogWarning("[DataSourceApi] GET {Url} failed: {Status} {Message}", url, (int)response.StatusCode, message);
                return Result<IReadOnlyList<ArchiveDataSourceDto>>.Failure(message ?? "获取数据源失败");
            }

            var items = await response.Content.ReadFromJsonAsync<IReadOnlyList<ArchiveDataSourceDto>>(cancellationToken: cancellationToken) ?? Array.Empty<ArchiveDataSourceDto>();
            return Result<IReadOnlyList<ArchiveDataSourceDto>>.Success(items);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "请求数据源列表失败");
            return Result<IReadOnlyList<ArchiveDataSourceDto>>.Failure("请求数据源列表失败");
        }
    }

    /// <summary>创建数据源。</summary>
    public async Task<Result<Guid>> CreateAsync(CreateArchiveDataSourceRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var client = _httpClientFactory.CreateClient("ArchiveApi");
            const string url = "api/v1/archive-data-sources";
            _logger.LogInformation("[DataSourceApi] POST {Url}, Name={Name}", url, request.Name);
            var response = await client.PostAsJsonAsync(url, request, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                var message = await ReadProblemAsync(response, cancellationToken);
                _logger.LogWarning("[DataSourceApi] POST {Url} failed: {Status} {Message}", url, (int)response.StatusCode, message);
                return Result<Guid>.Failure(message ?? "新增数据源失败");
            }

            var id = await response.Content.ReadFromJsonAsync<Guid>(cancellationToken: cancellationToken);
            return Result<Guid>.Success(id);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "新增数据源失败");
            return Result<Guid>.Failure("新增数据源失败");
        }
    }

    /// <summary>测试连接。</summary>
    public async Task<Result<bool>> TestConnectionAsync(TestArchiveDataSourceRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var client = _httpClientFactory.CreateClient("ArchiveApi");
            const string url = "api/v1/archive-data-sources/test-connection";
            _logger.LogInformation("[DataSourceApi] POST {Url} for {Server}@{Database}", url, request.ServerAddress, request.DatabaseName);
            var response = await client.PostAsJsonAsync(url, request, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                var message = await ReadProblemAsync(response, cancellationToken);
                _logger.LogWarning("[DataSourceApi] Test connection failed: {Status} {Message}", (int)response.StatusCode, message);
                return Result<bool>.Failure(message ?? "测试连接失败");
            }

            var result = await response.Content.ReadFromJsonAsync<bool>(cancellationToken: cancellationToken);
            return Result<bool>.Success(result);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "测试数据源连接失败");
            return Result<bool>.Failure("测试数据源连接失败");
        }
    }

    private static async Task<string?> ReadProblemAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        try
        {
            var problem = await response.Content.ReadFromJsonAsync<ProblemDetails>(cancellationToken: cancellationToken);
            return problem?.Detail ?? problem?.Title;
        }
        catch
        {
            return await response.Content.ReadAsStringAsync(cancellationToken);
        }
    }
}
