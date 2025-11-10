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

var builder = WebApplication.CreateBuilder(args);

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
var hangfireConnectionString = builder.Configuration.GetConnectionString("ArchiveDatabase") ??
                                "Server=localhost;Database=DbArchiveTool;Trusted_Connection=True;TrustServerCertificate=True";

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

// 配置定时任务示例(根据实际需求调整)
// 每天凌晨2点执行所有启用的归档任务
RecurringJob.AddOrUpdate<IArchiveJobService>(
    "daily-archive-all",
    "archive", // 队列名称
    service => service.ExecuteAllEnabledArchiveJobsAsync(),
    Cron.Daily(2), // 每天凌晨2点
    new RecurringJobOptions
    {
        TimeZone = TimeZoneInfo.Local
    });

app.UseHttpsRedirection();

app.MapControllers();

app.Run();

public partial class Program;
