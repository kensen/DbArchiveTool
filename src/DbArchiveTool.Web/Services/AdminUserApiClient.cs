using System.Net.Http.Json;
using DbArchiveTool.Application.AdminUsers;
using DbArchiveTool.Shared.Results;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Web.Services;

/// <summary>管理员账户相关 API 客户端。</summary>
public sealed class AdminUserApiClient
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<AdminUserApiClient> _logger;

    public AdminUserApiClient(IHttpClientFactory httpClientFactory, ILogger<AdminUserApiClient> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    /// <summary>查询系统是否已存在管理员账户。</summary>
    public async Task<Result<bool>> HasAdminAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var client = _httpClientFactory.CreateClient("ArchiveApi");
            _logger.LogInformation("[AdminUserApi] 准备调用接口: GET {Url}", "api/v1/admin-users/exists");
            var response = await client.GetAsync("api/v1/admin-users/exists", cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                var message = await ReadProblemMessageAsync(response, cancellationToken);
                _logger.LogWarning("[AdminUserApi] 调用失败: GET {Url} -> {StatusCode} {Message}", "api/v1/admin-users/exists", (int)response.StatusCode, message);
                return Result<bool>.Failure(message ?? "无法获取管理员状态");
            }

            var value = await response.Content.ReadFromJsonAsync<bool>(cancellationToken: cancellationToken);
            _logger.LogInformation("[AdminUserApi] 管理员状态接口成功响应: {StatusCode}, 已存在管理员 = {HasAdmin}", (int)response.StatusCode, value);
            return Result<bool>.Success(value);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "查询管理员状态失败");
            return Result<bool>.Failure("查询管理员状态失败，请检查网络或服务器");
        }
    }

    /// <summary>注册管理员账户。</summary>
    public async Task<Result<Guid>> RegisterAsync(RegisterAdminUserRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var client = _httpClientFactory.CreateClient("ArchiveApi");
            _logger.LogInformation("[AdminUserApi] 准备调用接口: POST {Url}, UserName = {UserName}", "api/v1/admin-users/register", request.UserName);
            var response = await client.PostAsJsonAsync("api/v1/admin-users/register", request, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                var message = await ReadProblemMessageAsync(response, cancellationToken);
                _logger.LogWarning("[AdminUserApi] 调用失败: POST {Url} -> {StatusCode} {Message}", "api/v1/admin-users/register", (int)response.StatusCode, message);
                return Result<Guid>.Failure(message ?? "注册管理员失败");
            }

            var id = await response.Content.ReadFromJsonAsync<Guid>(cancellationToken: cancellationToken);
            _logger.LogInformation("[AdminUserApi] 管理员注册成功: {StatusCode}, AdminId = {AdminId}", (int)response.StatusCode, id);
            return Result<Guid>.Success(id);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "注册管理员失败");
            return Result<Guid>.Failure("注册管理员失败，请稍后重试");
        }
    }

    /// <summary>执行管理员登录。</summary>
    public async Task<Result<Guid>> LoginAsync(AdminLoginRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var client = _httpClientFactory.CreateClient("ArchiveApi");
            _logger.LogInformation("[AdminUserApi] 准备调用接口: POST {Url}, UserName = {UserName}", "api/v1/admin-users/login", request.UserName);
            var response = await client.PostAsJsonAsync("api/v1/admin-users/login", request, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                var message = await ReadProblemMessageAsync(response, cancellationToken);
                _logger.LogWarning("[AdminUserApi] 调用失败: POST {Url} -> {StatusCode} {Message}", "api/v1/admin-users/login", (int)response.StatusCode, message);
                return Result<Guid>.Failure(message ?? "登录失败");
            }

            var id = await response.Content.ReadFromJsonAsync<Guid>(cancellationToken: cancellationToken);
            _logger.LogInformation("[AdminUserApi] 管理员登录成功: {StatusCode}, AdminId = {AdminId}", (int)response.StatusCode, id);
            return Result<Guid>.Success(id);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "管理员登录失败");
            return Result<Guid>.Failure("登录失败，请稍后重试");
        }
    }

    private static async Task<string?> ReadProblemMessageAsync(HttpResponseMessage response, CancellationToken cancellationToken)
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
