using DbArchiveTool.Application.ArchiveTasks;
using Microsoft.Extensions.DependencyInjection;

namespace DbArchiveTool.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddApplicationLayer(this IServiceCollection services)
    {
        services.AddScoped<IArchiveTaskQueryService, ArchiveTaskQueryService>();
        services.AddScoped<IArchiveTaskCommandService, ArchiveTaskCommandService>();

        return services;
    }
}
