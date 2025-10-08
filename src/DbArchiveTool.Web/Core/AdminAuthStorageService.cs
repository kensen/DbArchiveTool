using System;
using System.Threading.Tasks;
using Microsoft.JSInterop;

namespace DbArchiveTool.Web.Core;

/// <summary>����������Ա��֤��ƾ֤�Ĳֿ���</summary>
public sealed class AdminAuthStorageService : IAsyncDisposable
{
    private readonly IJSRuntime jsRuntime;
    private readonly Lazy<Task<IJSObjectReference>> moduleTask;
    private const string StorageKey = "dbArchiveTool.adminAuthTicket";

    /// <summary>���� JavaScript ƾ֤�洢��������</summary>
    public AdminAuthStorageService(IJSRuntime jsRuntime)
    {
        this.jsRuntime = jsRuntime;
        moduleTask = new(() => jsRuntime.InvokeAsync<IJSObjectReference>("import", "./js/authStorage.js").AsTask());
    }

    /// <summary>���沢��ȫ���µ�ƾ֤��</summary>
    /// <param name="ticket">��Ҫ�����Ĺ���Աƾ֤��</param>
    /// <param name="persistent">�Ƿ񱣴��� localStorage �������ӳ���Ч��</param>
    public async Task SaveAsync(AdminAuthTicket ticket, bool persistent)
    {
        var module = await moduleTask.Value;
        await module.InvokeVoidAsync("setAuthTicket", StorageKey, ticket, persistent);
    }

    /// <summary>��ȡǰ�ѱ�������Ա��֤ƾ֤��</summary>
    public async Task<AdminAuthTicket?> ReadAsync()
    {
        var module = await moduleTask.Value;
        return await module.InvokeAsync<AdminAuthTicket?>("getAuthTicket", StorageKey);
    }

    /// <summary>����˳���ղ���ڵ��ƾ֤����</summary>
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

/// <summary>��ʾ�洢�Ĺ���Ա��¼ƾ֤��</summary>
/// <param name="AdminId">����Ա��ʶ��</param>
/// <param name="UserName">����Ա�û���</param>
/// <param name="RememberMe">�Ƿ񱣴�������¼״̬��</param>
public sealed record AdminAuthTicket(Guid AdminId, string UserName, bool RememberMe);
