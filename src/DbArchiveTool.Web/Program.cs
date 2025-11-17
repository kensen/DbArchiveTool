using AntDesign;
using System.Text.Json;
using System.Text.Json.Serialization;
using Hangfire;
using Hangfire.SqlServer;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// 配置 Serilog
builder.Host.UseSerilog((context, services, configuration) => configuration
    .ReadFrom.Configuration(context.Configuration)
    .ReadFrom.Services(services)
    .Enrich.WithProperty("Application", "DbArchiveTool.Web")
    .Enrich.WithProperty("Environment", context.HostingEnvironment.EnvironmentName)
);

// 配置全局 JSON 序列化选项（枚举使用 PascalCase 字符串）
var jsonOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true,
    Converters = { new JsonStringEnumConverter() }
};

// Add services to the container.
builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();
builder.Services.AddAntDesign();
builder.Services.AddScoped<ReuseTabsService>();
builder.Services.AddScoped<DbArchiveTool.Web.Core.AdminSessionState>();
builder.Services.AddScoped<DbArchiveTool.Web.Core.AdminAuthStorageService>();
builder.Services.AddScoped<DbArchiveTool.Web.Core.PartitionPageState>();
builder.Services.AddScoped<DbArchiveTool.Web.Services.AdminUserApiClient>();
builder.Services.AddScoped<DbArchiveTool.Web.Services.ArchiveDataSourceApiClient>();

// 配置 Hangfire 存储(只读模式,用于监控)
var hangfireConnectionString = builder.Configuration.GetConnectionString("HangfireDatabase");
if (string.IsNullOrWhiteSpace(hangfireConnectionString))
{
    throw new InvalidOperationException("未在配置中找到 ConnectionStrings:HangfireDatabase。");
}

builder.Services.AddHangfire(config => config
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

// 注册 Hangfire 监控服务
builder.Services.AddScoped<DbArchiveTool.Web.Services.IHangfireMonitorService, DbArchiveTool.Web.Services.HangfireMonitorService>();

var archiveApiBaseUrl = builder.Configuration["ArchiveApi:BaseUrl"];
if (string.IsNullOrWhiteSpace(archiveApiBaseUrl))
{
    throw new InvalidOperationException("未在配置中找到 ArchiveApi:BaseUrl，请根据运行环境设置准确的 API 地址。");
}

// 配置 HttpClient 使用统一的 JSON 序列化选项
Action<IServiceProvider, HttpClient> configureClient = (sp, client) =>
{
    client.BaseAddress = new Uri(archiveApiBaseUrl, UriKind.Absolute);
};

// 配置 HttpClient 的 JSON 序列化选项（支持字符串枚举）
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.PropertyNameCaseInsensitive = true;
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
});

builder.Services.AddHttpClient("ArchiveApi", configureClient);
builder.Services.AddHttpClient<DbArchiveTool.Web.Services.PartitionManagementApiClient>(configureClient);
builder.Services.AddHttpClient<DbArchiveTool.Web.Services.PartitionInfoApiClient>(configureClient);
builder.Services.AddHttpClient<DbArchiveTool.Web.Services.PartitionConfigurationApiClient>(configureClient);
builder.Services.AddHttpClient<DbArchiveTool.Web.Services.BackgroundTaskApiClient>(configureClient);
builder.Services.AddHttpClient<DbArchiveTool.Web.Services.PartitionArchiveApiClient>(configureClient);
builder.Services.AddHttpClient<DbArchiveTool.Web.Services.ArchiveConfigurationApiClient>(configureClient);

var app = builder.Build();

// 初始化 Hangfire JobStorage (不启动 Server,仅用于监控)
GlobalConfiguration.Configuration.UseSqlServerStorage(
    hangfireConnectionString,
    new SqlServerStorageOptions
    {
        CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
        SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
        QueuePollInterval = TimeSpan.Zero,
        UseRecommendedIsolationLevel = true,
        DisableGlobalLocks = true,
        SchemaName = "Hangfire"
    });

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseStaticFiles();

app.UseRouting();

app.MapBlazorHub();
app.MapFallbackToPage("/_Host");

app.Run();
