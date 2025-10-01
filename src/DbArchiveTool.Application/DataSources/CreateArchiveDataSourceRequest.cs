namespace DbArchiveTool.Application.DataSources;

/// <summary>创建归档数据源请求。</summary>
public sealed class CreateArchiveDataSourceRequest
{
    /// <summary>数据源名称。</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>备注描述。</summary>
    public string? Description { get; set; }

    /// <summary>服务器地址。</summary>
    public string ServerAddress { get; set; } = string.Empty;

    /// <summary>服务器端口。</summary>
    public int ServerPort { get; set; } = 1433;

    /// <summary>数据库名称。</summary>
    public string DatabaseName { get; set; } = string.Empty;

    /// <summary>是否使用集成身份验证。</summary>
    public bool UseIntegratedSecurity { get; set; }

    /// <summary>SQL 身份验证用户名。</summary>
    public string? UserName { get; set; }

    /// <summary>SQL 身份验证密码。</summary>
    public string? Password { get; set; }
}
