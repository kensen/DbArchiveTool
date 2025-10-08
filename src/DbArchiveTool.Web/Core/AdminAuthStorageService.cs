using System;
using System.Threading.Tasks;
using Microsoft.JSInterop;

namespace DbArchiveTool.Web.Core;

/// <summary>用于在浏览器端统一管理管理员登录票据的存储服务。</summary>
public sealed class AdminAuthStorageService : IAsyncDisposable
{
    private readonly IJSRuntime jsRuntime;
    private readonly Lazy<Task<IJSObjectReference>> moduleTask;
    private const string StorageKey = "dbArchiveTool.adminAuthTicket";

    /// <summary>注入 JavaScript 模块以便与 localStorage / sessionStorage 交互。</summary>
    public AdminAuthStorageService(IJSRuntime jsRuntime)
    {
        this.jsRuntime = jsRuntime;
        moduleTask = new(() => jsRuntime.InvokeAsync<IJSObjectReference>("import", "./js/authStorage.js").AsTask());
    }

    /// <summary>保存票据，根据记住登录开关写入不同的存储容器。</summary>
    /// <param name="ticket">待保存的管理员登录票据。</param>
    /// <param name="persistent">true 写入 localStorage，否则写入 sessionStorage。</param>
    public async Task SaveAsync(AdminAuthTicket ticket, bool persistent)
    {
        var module = await moduleTask.Value;
        await module.InvokeVoidAsync("setAuthTicket", StorageKey, ticket, persistent);
    }

    /// <summary>读取已保存的登录票据。</summary>
    public async Task<AdminAuthTicket?> ReadAsync()
    {
        var module = await moduleTask.Value;
        return await module.InvokeAsync<AdminAuthTicket?>("getAuthTicket", StorageKey);
    }

    /// <summary>清除浏览器中的登录票据。</summary>
    public async Task ClearAsync()
    {
        if (!moduleTask.IsValueCreated)
        {
            return;
        }

        var module = await moduleTask.Value;
        await module.InvokeVoidAsync("clearAuthTicket", StorageKey);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (!moduleTask.IsValueCreated)
        {
            return;
        }

        var module = await moduleTask.Value;
        await module.DisposeAsync();
    }
}

/// <summary>管理员登录票据，封装身份信息和记住登录偏好。</summary>
/// <param name="AdminId">管理员唯一标识。</param>
/// <param name="UserName">管理员用户名。</param>
/// <param name="RememberMe">是否启用记住登录。</param>
public sealed record AdminAuthTicket(Guid AdminId, string UserName, bool RememberMe);
