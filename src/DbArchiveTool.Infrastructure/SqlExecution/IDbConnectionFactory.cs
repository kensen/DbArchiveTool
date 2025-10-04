using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DbArchiveTool.Infrastructure.SqlExecution;

/// <summary>
/// 提供基于连接字符串或数据源配置创建 SQL Server 连接的能力。
/// </summary>
public interface IDbConnectionFactory
{
    /// <summary>通过连接字符串创建未打开的连接实例。</summary>
    IDbConnection CreateConnection(string connectionString);

    /// <summary>根据数据源标识创建已打开的 SQL Server 连接。</summary>
    Task<SqlConnection> CreateSqlConnectionAsync(Guid dataSourceId, CancellationToken cancellationToken = default);
}
