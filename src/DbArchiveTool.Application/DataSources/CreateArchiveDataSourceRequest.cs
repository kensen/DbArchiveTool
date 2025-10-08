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

    /// <summary>是否使用源服务器作为目标服务器（归档数据存储位置），默认 true。</summary>
    public bool UseSourceAsTarget { get; set; } = true;

    /// <summary>目标服务器地址，当 UseSourceAsTarget = false 时必填。</summary>
    public string? TargetServerAddress { get; set; }

    /// <summary>目标服务器端口，当 UseSourceAsTarget = false 时有效。</summary>
    public int TargetServerPort { get; set; } = 1433;

    /// <summary>目标数据库名称，当 UseSourceAsTarget = false 时必填。</summary>
    public string? TargetDatabaseName { get; set; }

    /// <summary>目标服务器是否使用集成身份验证，当 UseSourceAsTarget = false 时有效。</summary>
    public bool TargetUseIntegratedSecurity { get; set; } = true;

    /// <summary>目标服务器 SQL 身份验证用户名，当 UseSourceAsTarget = false 且使用 SQL 身份验证时必填。</summary>
    public string? TargetUserName { get; set; }

    /// <summary>目标服务器 SQL 身份验证密码，与 TargetUserName 配合使用。</summary>
    public string? TargetPassword { get; set; }
}
