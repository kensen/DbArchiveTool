using System.Collections.Concurrent;
using System.IO;
using Microsoft.Extensions.Configuration;

namespace DbArchiveTool.Infrastructure.SqlExecution;

/// <summary>
/// 从文件系统加载 SQL 模板并缓存结果。
/// </summary>
internal sealed class FileSqlTemplateProvider : ISqlTemplateProvider
{
    private readonly string templateRoot;
    private readonly ConcurrentDictionary<string, string> cache = new();

    public FileSqlTemplateProvider(IConfiguration configuration)
    {
        var root = configuration.GetValue<string>("SqlTemplateRoot");
        templateRoot = string.IsNullOrWhiteSpace(root)
            ? Path.Combine(AppContext.BaseDirectory, "SqlTemplates", "Partitioning", "Commands")
            : Path.GetFullPath(root);
    }

    public string GetTemplate(string templateName)
    {
        return cache.GetOrAdd(templateName, name =>
        {
            var path = Path.Combine(templateRoot, name);
            if (!File.Exists(path))
            {
                throw new FileNotFoundException($"未找到 SQL 模板 {name}", path);
            }

            return File.ReadAllText(path);
        });
    }
}
