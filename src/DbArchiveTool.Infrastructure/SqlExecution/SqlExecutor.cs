using System.Data;
using Dapper;
using DbArchiveTool.Domain.DataSources;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.SqlExecution;

internal sealed class SqlExecutor : ISqlExecutor
{
    private readonly IDbConnectionFactory connectionFactory;
    private readonly ILogger<SqlExecutor> logger;

    public SqlExecutor(IDbConnectionFactory connectionFactory, ILogger<SqlExecutor> logger)
    {
        this.connectionFactory = connectionFactory;
        this.logger = logger;
    }

    public async Task<int> ExecuteAsync(string connection, string sql, object? param = null, int? timeoutSeconds = null)
    {
        using var dbConnection = CreateOpenConnection(connection);
        logger.LogDebug("Executing SQL command: {Sql}", sql);
        return await dbConnection.ExecuteAsync(new CommandDefinition(sql, param, commandTimeout: timeoutSeconds));
    }

    public async Task<IEnumerable<T>> QueryAsync<T>(string connection, string sql, object? param = null, int? timeoutSeconds = null)
    {
        using var dbConnection = CreateOpenConnection(connection);
        logger.LogDebug("Querying SQL: {Sql}", sql);
        return await dbConnection.QueryAsync<T>(new CommandDefinition(sql, param, commandTimeout: timeoutSeconds));
    }

    public async Task<T?> QuerySingleAsync<T>(string connection, string sql, object? param = null)
    {
        using var dbConnection = CreateOpenConnection(connection);
        logger.LogDebug("Querying single SQL: {Sql}", sql);
        return await dbConnection.QuerySingleOrDefaultAsync<T>(sql, param);
    }

    private IDbConnection CreateOpenConnection(string connectionString)
    {
        var connection = connectionFactory.CreateConnection(connectionString);
        if (connection.State != ConnectionState.Open)
        {
            connection.Open();
        }

        return connection;
    }
}
