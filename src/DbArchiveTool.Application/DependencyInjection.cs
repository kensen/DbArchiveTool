using DbArchiveTool.Application.AdminUsers;
using DbArchiveTool.Application.ArchiveTasks;
using DbArchiveTool.Application.DataSources;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Domain.AdminUsers;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.DependencyInjection;

namespace DbArchiveTool.Application;

/// <summary>
/// 应用层依赖注册入口。
/// </summary>
public static class DependencyInjection
{
    /// <summary>注册应用层服务。</summary>
    public static IServiceCollection AddApplicationLayer(this IServiceCollection services)
    {
        services.AddScoped<IArchiveTaskQueryService, ArchiveTaskQueryService>();
        services.AddScoped<IArchiveTaskCommandService, ArchiveTaskCommandService>();
        services.AddScoped<IAdminUserAppService, AdminUserAppService>();
        services.AddScoped<IArchiveDataSourceAppService, ArchiveDataSourceAppService>();
        services.AddScoped<IPartitionManagementAppService, PartitionManagementAppService>();
        services.AddScoped<IPartitionCommandAppService, PartitionCommandAppService>();
        services.AddScoped<PartitionValueParser>();
        services.AddScoped<IPasswordHasher<AdminUser>, PasswordHasher<AdminUser>>();

        return services;
    }
}
