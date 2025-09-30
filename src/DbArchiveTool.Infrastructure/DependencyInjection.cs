using DbArchiveTool.Domain.ArchiveTasks;
using DbArchiveTool.Infrastructure.Persistence;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DbArchiveTool.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddInfrastructureLayer(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("ArchiveDatabase") ??
                               "Server=localhost;Database=DbArchiveTool;Trusted_Connection=True;TrustServerCertificate=True";

        services.AddDbContext<ArchiveDbContext>(options =>
            options.UseSqlServer(connectionString));

        services.AddScoped<IArchiveTaskRepository, ArchiveTaskRepository>();
        services.AddSingleton<IDbConnectionFactory, SqlConnectionFactory>();
        services.AddScoped<ISqlExecutor, SqlExecutor>();

        return services;
    }
}
