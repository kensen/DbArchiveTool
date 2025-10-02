using System;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Infrastructure.SqlExecution;
using DbArchiveTool.Shared.DataSources;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.DataSources;

/// <summary>
/// Uses Dapper-backed <see cref="ISqlExecutor"/> to validate archive data source connectivity.
/// </summary>
internal sealed class ArchiveConnectionTester : IArchiveConnectionTester
{
    private const string ProbeSql = "SELECT TOP(1) 1";
    private readonly ISqlExecutor _sqlExecutor;
    private readonly ILogger<ArchiveConnectionTester> _logger;

    public ArchiveConnectionTester(ISqlExecutor sqlExecutor, ILogger<ArchiveConnectionTester> logger)
    {
        _sqlExecutor = sqlExecutor;
        _logger = logger;
    }

    public async Task<Result<bool>> TestConnectionAsync(string connectionString, CancellationToken cancellationToken = default)
    {
        try
        {
            await _sqlExecutor.QuerySingleAsync<int>(connectionString, ProbeSql);
            return Result<bool>.Success(true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Dapper 连接测试失败");
            return Result<bool>.Failure($"连接失败: {ex.Message}");
        }
    }
}
