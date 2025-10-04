using System.Data;
using DbArchiveTool.Domain.DataSources;
using Microsoft.Data.SqlClient;

namespace DbArchiveTool.Infrastructure.SqlExecution;

/// <summary>
/// 基于数据源仓储构建 SQL Server 连接，支持集成安全或 SQL 登录。
/// </summary>
internal sealed class SqlConnectionFactory : IDbConnectionFactory
{
    private readonly IDataSourceRepository dataSourceRepository;

    public SqlConnectionFactory(IDataSourceRepository dataSourceRepository)
    {
        this.dataSourceRepository = dataSourceRepository;
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
            builder.Password = dataSource.Password;
        }

        var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }
}
