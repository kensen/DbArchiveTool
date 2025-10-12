using System.Data;

namespace DbArchiveTool.Infrastructure.SqlExecution;

public interface ISqlExecutor
{
    Task<int> ExecuteAsync(string connection, string sql, object? param = null, int? timeoutSeconds = null);
    Task<IEnumerable<T>> QueryAsync<T>(string connection, string sql, object? param = null, int? timeoutSeconds = null);
    Task<T?> QuerySingleAsync<T>(string connection, string sql, object? param = null);

    Task<int> ExecuteAsync(IDbConnection connection, string sql, object? param = null, IDbTransaction? transaction = null, int? timeoutSeconds = null);
    Task<IEnumerable<T>> QueryAsync<T>(IDbConnection connection, string sql, object? param = null, IDbTransaction? transaction = null, int? timeoutSeconds = null);
    Task<T?> QuerySingleAsync<T>(IDbConnection connection, string sql, object? param = null, IDbTransaction? transaction = null);
}
