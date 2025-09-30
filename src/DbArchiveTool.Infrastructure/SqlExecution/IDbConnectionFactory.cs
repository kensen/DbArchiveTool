using System.Data;

namespace DbArchiveTool.Infrastructure.SqlExecution;

public interface IDbConnectionFactory
{
    IDbConnection CreateConnection(string connectionString);
}
