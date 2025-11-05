using Hangfire.Dashboard;

namespace DbArchiveTool.Api;

/// <summary>
/// Hangfire Dashboard 授权过滤器
/// </summary>
public sealed class HangfireAuthorizationFilter : IDashboardAuthorizationFilter
{
    /// <summary>
    /// 授权检查
    /// </summary>
    public bool Authorize(DashboardContext context)
    {
        // 开发环境允许访问
        // 生产环境应实现实际的身份验证逻辑
        var httpContext = context.GetHttpContext();
        
        // TODO: 实现实际的身份验证逻辑
        // 例如: return httpContext.User.Identity?.IsAuthenticated ?? false;
        
        // 暂时允许所有访问(仅用于开发测试)
        return true;
    }
}
