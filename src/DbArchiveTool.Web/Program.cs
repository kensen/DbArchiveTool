using AntDesign;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();
builder.Services.AddAntDesign();
builder.Services.AddScoped<ReuseTabsService>();
builder.Services.AddScoped<DbArchiveTool.Web.Core.AdminSessionState>();
builder.Services.AddScoped<DbArchiveTool.Web.Core.AdminAuthStorageService>();
builder.Services.AddScoped<DbArchiveTool.Web.Services.AdminUserApiClient>();
builder.Services.AddScoped<DbArchiveTool.Web.Services.ArchiveDataSourceApiClient>();

var archiveApiBaseUrl = builder.Configuration["ArchiveApi:BaseUrl"];
if (string.IsNullOrWhiteSpace(archiveApiBaseUrl))
{
    throw new InvalidOperationException("未在配置中找到 ArchiveApi:BaseUrl，请根据运行环境设置准确的 API 地址。");
}

builder.Services.AddHttpClient("ArchiveApi", client =>
{
    client.BaseAddress = new Uri(archiveApiBaseUrl, UriKind.Absolute);
});

builder.Services.AddHttpClient<DbArchiveTool.Web.Services.PartitionManagementApiClient>(client =>
{
    client.BaseAddress = new Uri(archiveApiBaseUrl, UriKind.Absolute);
});

builder.Services.AddHttpClient<DbArchiveTool.Web.Services.PartitionInfoApiClient>(client =>
{
    client.BaseAddress = new Uri(archiveApiBaseUrl, UriKind.Absolute);
});

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
