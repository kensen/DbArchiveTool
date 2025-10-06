using System.Collections.Generic;

namespace DbArchiveTool.Infrastructure.SqlExecution;

/// <summary>
/// 定义 SQL 模板提供器，用于从模板目录读取并缓存脚本内容。
/// </summary>
public interface ISqlTemplateProvider
{
    /// <summary>
    /// 根据模板名称获取模板内容。
    /// </summary>
    string GetTemplate(string templateName);
}
