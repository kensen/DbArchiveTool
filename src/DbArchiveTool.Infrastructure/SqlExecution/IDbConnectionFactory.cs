using System.Data;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.DataSources;
using Microsoft.Data.SqlClient;

namespace DbArchiveTool.Infrastructure.SqlExecution;

/// <summary>
/// 提供根据连接字符串或数据源配置构造 SQL Server 连接的能力。
/// </summary>
public interface IDbConnectionFactory
{
    /// <summary>通过连接字符串创建未打开的连接实例。</summary>
    IDbConnection CreateConnection(string connectionString);

    /// <summary>根据数据源标识创建已打开的 SQL Server 连接。</summary>
    Task<SqlConnection> CreateSqlConnectionAsync(Guid dataSourceId, CancellationToken cancellationToken = default);

    /// <summary>根据数据源标识创建已打开的 SQL Server 连接，使用归档目标配置。</summary>
    Task<SqlConnection> CreateTargetSqlConnectionAsync(Guid dataSourceId, CancellationToken cancellationToken = default);

    /// <summary>根据数据源配置生成源库连接字符串（自动处理密码解密）。</summary>
    string BuildConnectionString(ArchiveDataSource dataSource);

    /// <summary>根据数据源配置生成目标库连接字符串（自动处理密码解密）。</summary>
    string BuildTargetConnectionString(ArchiveDataSource dataSource);
}
