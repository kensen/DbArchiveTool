using System.Data;
using Microsoft.Data.SqlClient;

namespace DbArchiveTool.Infrastructure.SqlExecution;

internal sealed class SqlConnectionFactory : IDbConnectionFactory
{
    public IDbConnection CreateConnection(string connectionString) => new SqlConnection(connectionString);
}
