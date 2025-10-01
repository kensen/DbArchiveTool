using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.DataSources;

/// <summary>归档数据源实体,记录数据源连接必需信息与描述。</summary>
public sealed class ArchiveDataSource : AggregateRoot
{
    /// <summary>数据源显示名称。</summary>
    public string Name { get; private set; } = string.Empty;

    /// <summary>数据源备注描述,用于在界面展示。</summary>
    public string? Description { get; private set; }

    /// <summary>服务器地址或主机名。</summary>
    public string ServerAddress { get; private set; } = string.Empty;

    /// <summary>服务器端口号,默认 1433。</summary>
    public int ServerPort { get; private set; } = 1433;

    /// <summary>目标数据库名称。</summary>
    public string DatabaseName { get; private set; } = string.Empty;

    /// <summary>用户名,当使用 SQL 身份验证时必填。</summary>
    public string? UserName { get; private set; }

    /// <summary>密码,与 <see cref="UserName"/> 配合使用。</summary>
    public string? Password { get; private set; }

    /// <summary>是否使用集成身份验证。</summary>
    public bool UseIntegratedSecurity { get; private set; }

    /// <summary>是否启用当前数据源。</summary>
    public bool IsEnabled { get; private set; } = true;

    /// <summary>仅供 ORM 使用的无参构造函数。</summary>
    private ArchiveDataSource() { }

    /// <summary>创建归档数据源。</summary>
    public ArchiveDataSource(
        string name,
        string? description,
        string serverAddress,
        int serverPort,
        string databaseName,
        bool useIntegratedSecurity,
        string? userName,
        string? password)
    {
        Update(name, description, serverAddress, serverPort, databaseName, useIntegratedSecurity, userName, password);
    }

    /// <summary>更新数据源基础信息。</summary>
    public void Update(
        string name,
        string? description,
        string serverAddress,
        int serverPort,
        string databaseName,
        bool useIntegratedSecurity,
        string? userName,
        string? password,
        string operatorName = "SYSTEM")
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("数据源名称不能为空", nameof(name));
        }

        if (string.IsNullOrWhiteSpace(serverAddress))
        {
            throw new ArgumentException("服务器地址不能为空", nameof(serverAddress));
        }

        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new ArgumentException("数据库名称不能为空", nameof(databaseName));
        }

        if (serverPort <= 0 || serverPort > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(serverPort), "端口号必须在 1-65535 之间");
        }

        if (!useIntegratedSecurity && string.IsNullOrWhiteSpace(userName))
        {
            throw new ArgumentException("使用 SQL 身份验证时用户名不能为空", nameof(userName));
        }

        Name = name.Trim();
        Description = string.IsNullOrWhiteSpace(description) ? null : description.Trim();
        ServerAddress = serverAddress.Trim();
        ServerPort = serverPort;
        DatabaseName = databaseName.Trim();
        UseIntegratedSecurity = useIntegratedSecurity;
        UserName = string.IsNullOrWhiteSpace(userName) ? null : userName.Trim();
        Password = password;
        Touch(operatorName);
    }

    /// <summary>启用数据源。</summary>
    public void Enable(string operatorName)
    {
        IsEnabled = true;
        Touch(operatorName);
    }

    /// <summary>禁用数据源。</summary>
    public void Disable(string operatorName)
    {
        IsEnabled = false;
        Touch(operatorName);
    }
}
