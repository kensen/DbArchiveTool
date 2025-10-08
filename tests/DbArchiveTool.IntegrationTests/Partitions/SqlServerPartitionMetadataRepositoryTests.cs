using System.Data;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.Partitions;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DbArchiveTool.IntegrationTests.Partitions;

/// <summary>
/// 针对 SqlServerPartitionMetadataRepository 的集成测试，需要本地 SQL Server。
/// </summary>
public class SqlServerPartitionMetadataRepositoryTests : IAsyncLifetime
{
    private const string ConnectionString = "Server=DESKTOP-LRVB0VL;Database=db3021824;Trusted_Connection=True;TrustServerCertificate=True";
    private ServiceProvider? provider;

    [Fact(Skip = "需要本地 SQL Server，可在具备环境时取消跳过")]
    public async Task ListBoundaries_ShouldReturnConfiguredBoundaries()
    {
        var repository = provider!.GetRequiredService<IPartitionMetadataRepository>();
        var dataSourceId = Guid.Parse("00000000-0000-0000-0000-000000000001");

        var config = await repository.GetConfigurationAsync(dataSourceId, "dbo", "OrdersPartitioned", CancellationToken.None);
        Assert.NotNull(config);

        var boundaries = await repository.ListBoundariesAsync(dataSourceId, "dbo", "OrdersPartitioned", CancellationToken.None);
        Assert.NotEmpty(boundaries);

        var safety = await repository.GetSafetySnapshotAsync(dataSourceId, "dbo", "OrdersPartitioned", boundaries[0].SortKey, CancellationToken.None);
        Assert.True(safety.RowCount >= 0);
    }

    public async Task InitializeAsync()
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();

        await EnsurePartitionObjectsAsync(connection);
        await SeedPartitionedTableAsync(connection);

        provider = BuildServiceProvider();
    }

    public Task DisposeAsync()
    {
        provider?.Dispose();
        return Task.CompletedTask;
    }

    private static ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IDbConnectionFactory>(new TestConnectionFactory());
        _ = services.AddScoped<IPartitionMetadataRepository, SqlServerPartitionMetadataRepository>();
        services.AddLogging(builder => builder.AddProvider(NullLoggerProvider.Instance));
        return services.BuildServiceProvider();
    }

    private static async Task EnsurePartitionObjectsAsync(SqlConnection connection)
    {
        var sql = @"IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'ps_orders')
BEGIN
    DROP PARTITION SCHEME ps_orders;
END
IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'pf_orders')
BEGIN
    DROP PARTITION FUNCTION pf_orders;
END
CREATE PARTITION FUNCTION pf_orders (datetime) AS RANGE RIGHT FOR VALUES ('2025-01-01', '2025-07-01');
CREATE PARTITION SCHEME ps_orders AS PARTITION pf_orders ALL TO ([PRIMARY]);
IF OBJECT_ID('dbo.OrdersPartitioned', 'U') IS NOT NULL DROP TABLE dbo.OrdersPartitioned;
CREATE TABLE dbo.OrdersPartitioned
(
    Id INT NOT NULL,
    OrderDate DATETIME NOT NULL
);
CREATE CLUSTERED INDEX IX_OrdersPartitioned ON dbo.OrdersPartitioned (OrderDate) ON ps_orders(OrderDate);
";
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task SeedPartitionedTableAsync(SqlConnection connection)
    {
        var seedSql = @"TRUNCATE TABLE dbo.OrdersPartitioned;
INSERT INTO dbo.OrdersPartitioned (Id, OrderDate)
VALUES (1, '2024-12-31'), (2, '2025-02-01'), (3, '2025-08-15');
";
        await using var command = new SqlCommand(seedSql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private sealed class TestConnectionFactory : IDbConnectionFactory
    {
        public IDbConnection CreateConnection(string connectionString) => new SqlConnection(connectionString);

        public string BuildConnectionString(ArchiveDataSource dataSource) => ConnectionString;

        public async Task<SqlConnection> CreateSqlConnectionAsync(Guid dataSourceId, CancellationToken cancellationToken = default)
        {
            var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
    }
}



