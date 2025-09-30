using System.Data;
using Dapper;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.SqlExecution;

internal sealed class SqlExecutor : ISqlExecutor
{
    private readonly IDbConnectionFactory _connectionFactory;
    private readonly ILogger<SqlExecutor> _logger;

    public SqlExecutor(IDbConnectionFactory connectionFactory, ILogger<SqlExecutor> logger)
    {
        _connectionFactory = connectionFactory;
        _logger = logger;
    }

    public async Task<int> ExecuteAsync(string connection, string sql, object? param = null, int? timeoutSeconds = null)
    {
        using var dbConnection = CreateOpenConnection(connection);
        _logger.LogDebug("Executing SQL command: {Sql}", sql);
        return await dbConnection.ExecuteAsync(new CommandDefinition(sql, param, commandTimeout: timeoutSeconds));
    }

    public async Task<IEnumerable<T>> QueryAsync<T>(string connection, string sql, object? param = null, int? timeoutSeconds = null)
    {
        using var dbConnection = CreateOpenConnection(connection);
        _logger.LogDebug("Querying SQL: {Sql}", sql);
        return await dbConnection.QueryAsync<T>(new CommandDefinition(sql, param, commandTimeout: timeoutSeconds));
    }

    public async Task<T?> QuerySingleAsync<T>(string connection, string sql, object? param = null)
    {
        using var dbConnection = CreateOpenConnection(connection);
        _logger.LogDebug("Querying single SQL: {Sql}", sql);
        return await dbConnection.QuerySingleOrDefaultAsync<T>(sql, param);
    }

    private IDbConnection CreateOpenConnection(string connectionString)
    {
        var connection = _connectionFactory.CreateConnection(connectionString);
        if (connection.State != ConnectionState.Open)
        {
            connection.Open();
        }

        return connection;
    }
}
