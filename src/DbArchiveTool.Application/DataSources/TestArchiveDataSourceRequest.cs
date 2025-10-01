namespace DbArchiveTool.Application.DataSources;

/// <summary>测试归档数据源连接请求。</summary>
public sealed class TestArchiveDataSourceRequest
{
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
