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

    /// <summary>是否使用源服务器作为目标服务器（归档数据存储位置）。默认true。</summary>
    public bool UseSourceAsTarget { get; private set; } = true;

    /// <summary>目标服务器地址或主机名，当 UseSourceAsTarget = false 时有效。</summary>
    public string? TargetServerAddress { get; private set; }

    /// <summary>目标服务器端口号，当 UseSourceAsTarget = false 时有效。</summary>
    public int TargetServerPort { get; private set; } = 1433;

    /// <summary>目标数据库名称，当 UseSourceAsTarget = false 时有效。</summary>
    public string? TargetDatabaseName { get; private set; }

    /// <summary>目标服务器是否使用集成身份验证，当 UseSourceAsTarget = false 时有效。</summary>
    public bool TargetUseIntegratedSecurity { get; private set; }

    /// <summary>目标服务器用户名，当 UseSourceAsTarget = false 且使用 SQL 身份验证时必填。</summary>
    public string? TargetUserName { get; private set; }

    /// <summary>目标服务器密码，与 <see cref="TargetUserName"/> 配合使用。</summary>
    public string? TargetPassword { get; private set; }

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
        bool useSourceAsTarget = true,
        string? targetServerAddress = null,
        int targetServerPort = 1433,
        string? targetDatabaseName = null,
        bool targetUseIntegratedSecurity = true,
        string? targetUserName = null,
        string? targetPassword = null,
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

        // 验证目标服务器配置
        if (!useSourceAsTarget)
        {
            if (string.IsNullOrWhiteSpace(targetServerAddress))
            {
                throw new ArgumentException("目标服务器地址不能为空", nameof(targetServerAddress));
            }

            if (string.IsNullOrWhiteSpace(targetDatabaseName))
            {
                throw new ArgumentException("目标数据库名称不能为空", nameof(targetDatabaseName));
            }

            if (targetServerPort <= 0 || targetServerPort > 65535)
            {
                throw new ArgumentOutOfRangeException(nameof(targetServerPort), "目标服务器端口号必须在 1-65535 之间");
            }

            if (!targetUseIntegratedSecurity && string.IsNullOrWhiteSpace(targetUserName))
            {
                throw new ArgumentException("目标服务器使用 SQL 身份验证时用户名不能为空", nameof(targetUserName));
            }
        }

        Name = name.Trim();
        Description = string.IsNullOrWhiteSpace(description) ? null : description.Trim();
        ServerAddress = serverAddress.Trim();
        ServerPort = serverPort;
        DatabaseName = databaseName.Trim();
        UseIntegratedSecurity = useIntegratedSecurity;
        UserName = string.IsNullOrWhiteSpace(userName) ? null : userName.Trim();
        Password = password;

        UseSourceAsTarget = useSourceAsTarget;
        TargetServerAddress = useSourceAsTarget ? null : (string.IsNullOrWhiteSpace(targetServerAddress) ? null : targetServerAddress.Trim());
        TargetServerPort = useSourceAsTarget ? 1433 : targetServerPort;
        TargetDatabaseName = useSourceAsTarget ? null : (string.IsNullOrWhiteSpace(targetDatabaseName) ? null : targetDatabaseName.Trim());
        TargetUseIntegratedSecurity = useSourceAsTarget || targetUseIntegratedSecurity;
        TargetUserName = useSourceAsTarget ? null : (string.IsNullOrWhiteSpace(targetUserName) ? null : targetUserName.Trim());
        TargetPassword = useSourceAsTarget ? null : targetPassword;

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
