using System;

namespace DbArchiveTool.Application.DataSources;

/// <summary>更新归档数据源请求。</summary>
public sealed class UpdateArchiveDataSourceRequest
{
    /// <summary>数据源标识。</summary>
    public Guid Id { get; set; }

    /// <summary>数据源名称。</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>描述信息。</summary>
    public string? Description { get; set; }

    /// <summary>服务器地址。</summary>
    public string ServerAddress { get; set; } = string.Empty;

    /// <summary>服务器端口。</summary>
    public int ServerPort { get; set; } = 1433;

    /// <summary>数据库名称。</summary>
    public string DatabaseName { get; set; } = string.Empty;

    /// <summary>是否使用集成认证。</summary>
    public bool UseIntegratedSecurity { get; set; }

    /// <summary>SQL 登录用户名。</summary>
    public string? UserName { get; set; }

    /// <summary>SQL 登录密码。</summary>
    public string? Password { get; set; }

    /// <summary>操作人名称。</summary>
    public string? OperatorName { get; set; }
}
