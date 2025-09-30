using DbArchiveTool.Application.AdminUsers;
using DbArchiveTool.Application.ArchiveTasks;
using DbArchiveTool.Domain.AdminUsers;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.DependencyInjection;

namespace DbArchiveTool.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddApplicationLayer(this IServiceCollection services)
    {
        services.AddScoped<IArchiveTaskQueryService, ArchiveTaskQueryService>();
        services.AddScoped<IArchiveTaskCommandService, ArchiveTaskCommandService>();
        services.AddScoped<IAdminUserAppService, AdminUserAppService>();
        services.AddScoped<IPasswordHasher<AdminUser>, PasswordHasher<AdminUser>>();

        return services;
    }
}
