using System.Reflection;
using System.Text.Json.Serialization;
using DbArchiveTool.Application;
using DbArchiveTool.Application.Archives;
using DbArchiveTool.Infrastructure;
using DbArchiveTool.Infrastructure.Persistence;
using Hangfire;
using Hangfire.SqlServer;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// 配置 Serilog
builder.Host.UseSerilog((context, services, configuration) => configuration
    .ReadFrom.Configuration(context.Configuration)
    .ReadFrom.Services(services)
    .Enrich.WithProperty("Application", "DbArchiveTool.Api")
    .Enrich.WithProperty("Environment", context.HostingEnvironment.EnvironmentName)
);

builder.Services.AddControllers()
    .AddJsonOptions(options =>
    {
        // 配置枚举序列化为字符串
        options.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
    });
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Version = "v1",
        Title = "DbArchiveTool API",
        Description = "归档与分区管理接口",
    });

    var xmlPath = Path.Combine(AppContext.BaseDirectory, "DbArchiveTool.Api.xml");
    if (File.Exists(xmlPath))
    {
        options.IncludeXmlComments(xmlPath);
    }
});

builder.Services.AddApplicationLayer();
builder.Services.AddInfrastructureLayer(builder.Configuration);

// 提前执行数据库迁移（必须在 Hangfire 初始化之前）
using (var tempScope = builder.Services.BuildServiceProvider().CreateScope())
{
    var dbContext = tempScope.ServiceProvider.GetRequiredService<ArchiveDbContext>();
    var logger = tempScope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    
    try
    {
        if (dbContext.Database.IsRelational())
        {
            await dbContext.Database.MigrateAsync();
            logger.LogInformation("数据库迁移成功完成");
        }
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "数据库迁移失败，应用启动中止");
        throw;
    }
}

// 配置 Hangfire
// 说明：Hangfire 存储建议与业务库共用同一连接串（通过 SchemaName 区分），
// 但为了便于 Web 端监控与环境配置统一，这里优先读取 HangfireDatabase，回退到 ArchiveDatabase。
var hangfireConnectionString = builder.Configuration.GetConnectionString("HangfireDatabase")
    ?? builder.Configuration.GetConnectionString("ArchiveDatabase")
    ?? "Server=localhost;Database=DbArchiveTool;Trusted_Connection=True;TrustServerCertificate=True";

builder.Services.AddHangfire(configuration => configuration
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseSimpleAssemblyNameTypeSerializer()
    .UseRecommendedSerializerSettings()
    .UseSqlServerStorage(hangfireConnectionString, new SqlServerStorageOptions
    {
        CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
        SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
        QueuePollInterval = TimeSpan.Zero,
        UseRecommendedIsolationLevel = true,
        DisableGlobalLocks = true,
        SchemaName = "Hangfire"
    }));

builder.Services.AddHangfireServer(options =>
{
    options.WorkerCount = Environment.ProcessorCount * 2;
    options.Queues = new[] { "archive", "default" };
    options.ServerName = $"{Environment.MachineName}-archive";
});

var app = builder.Build();

// 应用启动后立即注册所有启用的定时归档任务
using (var scope = app.Services.CreateScope())
{
    var scheduler = scope.ServiceProvider.GetRequiredService<DbArchiveTool.Application.Services.ScheduledArchiveJobs.IScheduledArchiveJobScheduler>();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    
    try
    {
        var registeredCount = await scheduler.RegisterAllJobsAsync();
        logger.LogInformation("成功注册 {Count} 个定时归档任务到 Hangfire", registeredCount);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "注册定时归档任务失败");
        // 不中止应用启动,允许手动管理任务
    }
}

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(options =>
    {
        options.SwaggerEndpoint("/swagger/v1/swagger.json", "DbArchiveTool API v1");
    });
}

// 配置 Hangfire Dashboard
app.UseHangfireDashboard("/hangfire", new DashboardOptions
{
    Authorization = new[] { new DbArchiveTool.Api.HangfireAuthorizationFilter() },
    DisplayStorageConnectionString = false,
    DashboardTitle = "归档任务调度中心"
});

// ⚠️ 暂时禁用全局定时归档任务
// 原因: ArchiveConfiguration 应仅作为手动归档的配置模板,不应被定时任务自动执行
// 后续将实现独立的 ScheduledArchiveJob 实体用于定时归档功能
// 配置定时任务示例(根据实际需求调整)
// 每天凌晨2点执行所有启用的归档任务
// RecurringJob.AddOrUpdate<IArchiveJobService>(
//     "daily-archive-all",
//     "archive", // 队列名称
//     service => service.ExecuteAllEnabledArchiveJobsAsync(),
//     Cron.Daily(2), // 每天凌晨2点
//     new RecurringJobOptions
//     {
//         TimeZone = TimeZoneInfo.Local
//     });

// 添加 Serilog HTTP 请求日志中间件
app.UseSerilogRequestLogging(options =>
{
    options.MessageTemplate = "HTTP {RequestMethod} {RequestPath} responded {StatusCode} in {Elapsed:0.0000} ms";
    options.EnrichDiagnosticContext = (diagnosticContext, httpContext) =>
    {
        diagnosticContext.Set("RequestHost", httpContext.Request.Host.Value);
        diagnosticContext.Set("UserAgent", httpContext.Request.Headers["User-Agent"].ToString());
    };
});

app.UseHttpsRedirection();

app.MapControllers();

app.Run();

public partial class Program;
