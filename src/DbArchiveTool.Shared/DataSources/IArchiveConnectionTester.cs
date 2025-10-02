using DbArchiveTool.Shared.Results;
using System.Threading;
using System.Threading.Tasks;

namespace DbArchiveTool.Shared.DataSources;

/// <summary>
/// Defines a contract for validating archive data source connectivity using Dapper-based checks.
/// </summary>
public interface IArchiveConnectionTester
{
    /// <summary>
    /// Attempts to open the provided connection string and execute a lightweight probe query.
    /// </summary>
    /// <param name="connectionString">Fully resolved SQL Server connection string.</param>
    /// <param name="cancellationToken">Propagation token.</param>
    Task<Result<bool>> TestConnectionAsync(string connectionString, CancellationToken cancellationToken = default);
}
