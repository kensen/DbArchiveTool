using System.Data;
using System.Threading;
using DbArchiveTool.Application.Abstractions;
using DbArchiveTool.Domain.DataSources;
using Microsoft.Data.SqlClient;

namespace DbArchiveTool.Infrastructure.SqlExecution;

/// <summary>
/// 根据归档数据源配置构造 SQL Server 连接，支持源库与目标库。
/// </summary>
internal sealed class SqlConnectionFactory : IDbConnectionFactory
{
    private readonly IDataSourceRepository dataSourceRepository;
    private readonly IPasswordEncryptionService encryptionService;

    public SqlConnectionFactory(
        IDataSourceRepository dataSourceRepository,
        IPasswordEncryptionService encryptionService)
    {
        this.dataSourceRepository = dataSourceRepository;
        this.encryptionService = encryptionService;
    }

    /// <inheritdoc />
    public IDbConnection CreateConnection(string connectionString) => new SqlConnection(connectionString);

    /// <inheritdoc />
    public async Task<SqlConnection> CreateSqlConnectionAsync(Guid dataSourceId, CancellationToken cancellationToken = default)
    {
        var dataSource = await LoadDataSourceAsync(dataSourceId, cancellationToken);
        var connectionString = BuildConnectionString(dataSource);
        return await OpenAsync(connectionString, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<SqlConnection> CreateTargetSqlConnectionAsync(Guid dataSourceId, CancellationToken cancellationToken = default)
    {
        var dataSource = await LoadDataSourceAsync(dataSourceId, cancellationToken);
        var connectionString = BuildTargetConnectionString(dataSource);
        return await OpenAsync(connectionString, cancellationToken);
    }

    /// <inheritdoc />
    public string BuildConnectionString(ArchiveDataSource dataSource)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = BuildServerAddress(dataSource.ServerAddress, dataSource.ServerPort),
            InitialCatalog = dataSource.DatabaseName,
            IntegratedSecurity = dataSource.UseIntegratedSecurity,
            TrustServerCertificate = true,
            ConnectTimeout = 30
        };

        if (!dataSource.UseIntegratedSecurity)
        {
            builder.UserID = dataSource.UserName;
            builder.Password = DecryptPassword(dataSource.Password);
        }

        return builder.ConnectionString;
    }

    /// <inheritdoc />
    public string BuildTargetConnectionString(ArchiveDataSource dataSource)
    {
        if (dataSource.UseSourceAsTarget)
        {
            return BuildConnectionString(dataSource);
        }

        if (string.IsNullOrWhiteSpace(dataSource.TargetServerAddress))
        {
            throw new InvalidOperationException("未配置目标服务器地址，无法构建目标连接字符串。");
        }

        if (string.IsNullOrWhiteSpace(dataSource.TargetDatabaseName))
        {
            throw new InvalidOperationException("未配置目标数据库名称，无法构建目标连接字符串。");
        }

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = BuildServerAddress(dataSource.TargetServerAddress, dataSource.TargetServerPort),
            InitialCatalog = dataSource.TargetDatabaseName,
            IntegratedSecurity = dataSource.TargetUseIntegratedSecurity,
            TrustServerCertificate = true,
            ConnectTimeout = 30
        };

        if (!dataSource.TargetUseIntegratedSecurity)
        {
            if (string.IsNullOrWhiteSpace(dataSource.TargetUserName))
            {
                throw new InvalidOperationException("目标服务器未配置登录账号。");
            }

            builder.UserID = dataSource.TargetUserName;
            builder.Password = DecryptPassword(dataSource.TargetPassword);
        }

        return builder.ConnectionString;
    }

    private async Task<ArchiveDataSource> LoadDataSourceAsync(Guid dataSourceId, CancellationToken cancellationToken)
    {
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        return dataSource ?? throw new InvalidOperationException($"归档数据源 {dataSourceId} 不存在。");
    }

    private static string BuildServerAddress(string serverAddress, int port)
    {
        return port == 1433 ? serverAddress : $"{serverAddress},{port}";
    }

    private string? DecryptPassword(string? encrypted)
    {
        if (string.IsNullOrWhiteSpace(encrypted))
        {
            return encrypted;
        }

        return encryptionService.IsEncrypted(encrypted)
            ? encryptionService.Decrypt(encrypted)
            : encrypted;
    }

    private static async Task<SqlConnection> OpenAsync(string connectionString, CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }
}
