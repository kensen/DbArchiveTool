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

await EnsureDatabaseAsync(app.Services, app.Logger);

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

async Task EnsureDatabaseAsync(IServiceProvider services, ILogger logger)
{
    using var scope = services.CreateScope();
    var dbContext = scope.ServiceProvider.GetRequiredService<ArchiveDbContext>();

    try
    {
        if (!dbContext.Database.IsRelational())
        {
            await dbContext.Database.EnsureCreatedAsync();
            return;
        }

        await dbContext.Database.MigrateAsync();
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "初始化数据库失败，请检查连接字符串和数据库权限配置。");
        throw;
    }
}

public partial class Program;
