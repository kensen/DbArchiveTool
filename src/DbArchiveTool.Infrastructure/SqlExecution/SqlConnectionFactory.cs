using System.Data;
using DbArchiveTool.Application.Abstractions;
using DbArchiveTool.Domain.DataSources;
using Microsoft.Data.SqlClient;

namespace DbArchiveTool.Infrastructure.SqlExecution;

/// <summary>
/// 基于数据源仓储构建 SQL Server 连接，支持集成安全或 SQL 登录。
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
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        if (dataSource is null)
        {
            throw new InvalidOperationException($"数据源 {dataSourceId} 不存在。");
        }

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = dataSource.ServerPort == 1433 ? dataSource.ServerAddress : $"{dataSource.ServerAddress},{dataSource.ServerPort}",
            InitialCatalog = dataSource.DatabaseName,
            IntegratedSecurity = dataSource.UseIntegratedSecurity,
            TrustServerCertificate = true,
            ConnectTimeout = 5
        };

        if (!dataSource.UseIntegratedSecurity)
        {
            builder.UserID = dataSource.UserName;
            
            // 解密密码（如果已加密）
            var password = dataSource.Password;
            if (!string.IsNullOrWhiteSpace(password) && encryptionService.IsEncrypted(password))
            {
                password = encryptionService.Decrypt(password);
            }
            builder.Password = password;
        }

        var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }
}
