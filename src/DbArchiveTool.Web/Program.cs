using AntDesign;
using System.Text.Json;
using System.Text.Json.Serialization;

var builder = WebApplication.CreateBuilder(args);

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

builder.Services.AddHttpClient("ArchiveApi", configureClient);
builder.Services.AddHttpClient<DbArchiveTool.Web.Services.PartitionManagementApiClient>(configureClient);
builder.Services.AddHttpClient<DbArchiveTool.Web.Services.PartitionInfoApiClient>(configureClient);
builder.Services.AddHttpClient<DbArchiveTool.Web.Services.PartitionConfigurationApiClient>(configureClient);
builder.Services.AddHttpClient<DbArchiveTool.Web.Services.BackgroundTaskApiClient>(configureClient);

var app = builder.Build();

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
