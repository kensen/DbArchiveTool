namespace DbArchiveTool.Application.DataSources;

/// <summary>归档数据源展示信息。</summary>
public sealed record class ArchiveDataSourceDto
{
    /// <summary>主键标识。</summary>
    public Guid Id { get; init; }

    /// <summary>数据源名称。</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>备注描述。</summary>
    public string? Description { get; init; }

    /// <summary>服务器地址。</summary>
    public string ServerAddress { get; init; } = string.Empty;

    /// <summary>服务器端口。</summary>
    public int ServerPort { get; init; }

    /// <summary>数据库名称。</summary>
    public string DatabaseName { get; init; } = string.Empty;

    /// <summary>是否使用集成身份验证。</summary>
    public bool UseIntegratedSecurity { get; init; }

    /// <summary>SQL 身份验证用户名。</summary>
    public string? UserName { get; init; }

    /// <summary>是否启用当前数据源。</summary>
    public bool IsEnabled { get; init; }

    /// <summary>卡片上显示的连接信息。</summary>
    public string DisplayConnection { get; init; } = string.Empty;
}
