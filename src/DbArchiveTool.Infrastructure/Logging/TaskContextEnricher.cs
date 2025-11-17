using Microsoft.AspNetCore.Http;
using Serilog.Core;
using Serilog.Events;

namespace DbArchiveTool.Infrastructure.Logging;

/// <summary>
/// 任务上下文富化器
/// 自动从 HttpContext.Items 中读取 TaskId、DataSourceId 等业务上下文字段并注入到日志事件中
/// </summary>
public class TaskContextEnricher : ILogEventEnricher
{
    private readonly IHttpContextAccessor? _httpContextAccessor;

    public TaskContextEnricher(IHttpContextAccessor? httpContextAccessor = null)
    {
        _httpContextAccessor = httpContextAccessor;
    }

    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
    {
        var httpContext = _httpContextAccessor?.HttpContext;
        if (httpContext == null) return;

        // 从 HttpContext.Items 读取业务上下文
        if (httpContext.Items.TryGetValue("TaskId", out var taskId))
        {
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("TaskId", taskId));
        }

        if (httpContext.Items.TryGetValue("DataSourceId", out var dsId))
        {
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("DataSourceId", dsId));
        }

        if (httpContext.Items.TryGetValue("UserId", out var userId))
        {
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("UserId", userId));
        }
    }
}
