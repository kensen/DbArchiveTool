using System;
using DbArchiveTool.Application.Abstractions;
using DbArchiveTool.Application.Archives;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Application.Services.ScheduledArchiveJobs;
using DbArchiveTool.Domain.AdminUsers;
using DbArchiveTool.Domain.ArchiveConfigurations;
using DbArchiveTool.Domain.ArchiveTasks;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Domain.ScheduledArchiveJobs;
using DbArchiveTool.Infrastructure.Archives;
using DbArchiveTool.Infrastructure.DataSources;
using DbArchiveTool.Infrastructure.Executors;
using DbArchiveTool.Infrastructure.Partitions;
using DbArchiveTool.Infrastructure.Persistence;
using DbArchiveTool.Infrastructure.Queries;
using DbArchiveTool.Infrastructure.Security;
using DbArchiveTool.Infrastructure.SqlExecution;
using DbArchiveTool.Infrastructure.Scheduling;
using DbArchiveTool.Infrastructure.Logging;
using DbArchiveTool.Shared.DataSources;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Serilog.Core;

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
        services.AddScoped<IArchiveConfigurationRepository, ArchiveConfigurationRepository>();
        services.AddScoped<IScheduledArchiveJobRepository, ScheduledArchiveJobRepository>();
        services.AddScoped<IArchiveConnectionTester, ArchiveConnectionTester>();
        services.AddScoped<IDbConnectionFactory, SqlConnectionFactory>();
        services.AddScoped<ISqlExecutor, SqlExecutor>();
        services.AddScoped<ISqlTemplateProvider, FileSqlTemplateProvider>();
        services.AddScoped<IPartitionMetadataRepository, SqlServerPartitionMetadataRepository>();
        services.AddScoped<IPartitionCommandRepository, PartitionCommandRepository>();
        services.AddScoped<IPartitionConfigurationRepository, PartitionConfigurationRepository>();
        services.AddScoped<IBackgroundTaskRepository, BackgroundTaskRepository>();
        services.AddScoped<IBackgroundTaskLogRepository, BackgroundTaskLogRepository>();
        services.AddScoped<IPartitionAuditLogRepository, PartitionAuditLogRepository>();
        services.AddScoped<IPartitionCommandScriptGenerator, TSqlPartitionCommandScriptGenerator>();
        services.AddScoped<IPermissionInspectionRepository, SqlServerPermissionInspectionRepository>();
        services.AddScoped<IPartitionSwitchInspectionService, PartitionSwitchInspectionService>();
        services.AddScoped<IArchiveTaskScheduler, ArchiveTaskScheduler>();
        services.AddScoped<IPartitionSwitchAutoFixExecutor, PartitionSwitchAutoFixExecutor>();
        services.AddScoped<SqlPartitionQueryService>();
        services.AddScoped<SqlPartitionCommandExecutor>();
        services.AddScoped<BackgroundTaskProcessor>();
        services.AddScoped<IPartitionMetadataService, PartitionMetadataService>();
        services.AddScoped<ITableManagementService, TableManagementService>();
        services.AddScoped<SqlBulkCopyExecutor>();
        services.AddScoped<BcpExecutor>();
        services.AddScoped<PartitionSwitchHelper>();
        services.AddScoped<OptimizedPartitionArchiveExecutor>();
        services.AddScoped<IArchiveExecutor, ArchiveExecutorAdapter>();
        services.AddScoped<ArchiveOrchestrationService>();
        services.AddScoped<IPartitionCommandExecutor, SplitPartitionCommandExecutor>();
        services.AddScoped<IPartitionCommandExecutor, MergePartitionCommandExecutor>();
        services.AddScoped<IPartitionCommandExecutor, SwitchPartitionCommandExecutor>();
        services.AddSingleton<BackgroundTaskQueue>();
        services.AddSingleton<IPartitionCommandQueue, PartitionCommandQueue>();
        services.AddHostedService<BackgroundTaskHostedService>();
        services.AddHostedService<PartitionCommandHostedService>();
        services.AddSingleton<IBackgroundTaskDispatcher, BackgroundTaskDispatcher>();

        // 注册定时归档任务调度器和执行器
        services.AddScoped<IScheduledArchiveJobScheduler, ScheduledArchiveJobScheduler>();
        services.AddScoped<IScheduledArchiveJobExecutor, ScheduledArchiveJobExecutor>();

        // 注册密码加密服务
        services.AddDataProtection();
        services.AddSingleton<IPasswordEncryptionService, PasswordEncryptionService>();

        // 注册 Serilog 自定义 Enricher
        services.AddHttpContextAccessor();
        services.AddSingleton<ILogEventEnricher, TaskContextEnricher>();

        return services;
    }
}
