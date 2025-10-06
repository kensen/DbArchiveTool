using System;
using DbArchiveTool.Domain.AdminUsers;
using DbArchiveTool.Domain.ArchiveTasks;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.DataSources;
using DbArchiveTool.Infrastructure.Partitions;
using DbArchiveTool.Infrastructure.Persistence;
using DbArchiveTool.Infrastructure.SqlExecution;
using DbArchiveTool.Shared.DataSources;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DbArchiveTool.Infrastructure;

/// <summary>
/// 基础设施层依赖注入扩展，注册数据库上下文、仓储与脚本服务。
/// </summary>
public static class DependencyInjection
{
    /// <summary>
    /// 注册基础设施层服务。
    /// </summary>
    public static IServiceCollection AddInfrastructureLayer(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("ArchiveDatabase") ??
                               "Server=localhost;Database=DbArchiveTool;Trusted_Connection=True;TrustServerCertificate=True";

        services.AddDbContext<ArchiveDbContext>(options =>
            options.UseSqlServer(connectionString, sqlOptions =>
                sqlOptions.EnableRetryOnFailure(maxRetryCount: 5, maxRetryDelay: TimeSpan.FromSeconds(10), errorNumbersToAdd: null)));

        services.AddScoped<IArchiveTaskRepository, ArchiveTaskRepository>();
        services.AddScoped<IAdminUserRepository, AdminUserRepository>();
        services.AddScoped<IDataSourceRepository, DataSourceRepository>();
        services.AddScoped<IArchiveConnectionTester, ArchiveConnectionTester>();
        services.AddScoped<IDbConnectionFactory, SqlConnectionFactory>();
        services.AddScoped<ISqlExecutor, SqlExecutor>();
        services.AddScoped<ISqlTemplateProvider, FileSqlTemplateProvider>();
        services.AddScoped<IPartitionMetadataRepository, SqlServerPartitionMetadataRepository>();
        services.AddScoped<IPartitionCommandRepository, PartitionCommandRepository>();
        services.AddScoped<IPartitionCommandScriptGenerator, TSqlPartitionCommandScriptGenerator>();

        return services;
    }
}
