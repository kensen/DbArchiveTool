using DbArchiveTool.Api;
using DbArchiveTool.Infrastructure.Persistence;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DbArchiveTool.IntegrationTests;

/// <summary>
/// 自定义 WebApplicationFactory，用于设置测试环境配置。
/// </summary>
public sealed class ApiWebApplicationFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureServices(services =>
        {
            services.RemoveAll(typeof(DbContextOptions<ArchiveDbContext>));
            services.RemoveAll<ArchiveDbContext>();
            services.AddDbContext<ArchiveDbContext>(options => options.UseInMemoryDatabase("IntegrationTests"));
        });
    }
}
