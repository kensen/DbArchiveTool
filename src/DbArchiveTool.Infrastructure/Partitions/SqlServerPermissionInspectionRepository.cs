using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Partitions;

/// <summary>
/// 使用 fn_my_permissions 检测 SQL Server 权限的实现。
/// </summary>
internal sealed class SqlServerPermissionInspectionRepository : IPermissionInspectionRepository
{
    private readonly IDbConnectionFactory connectionFactory;
    private readonly ILogger<SqlServerPermissionInspectionRepository> logger;

    public SqlServerPermissionInspectionRepository(
        IDbConnectionFactory connectionFactory,
        ILogger<SqlServerPermissionInspectionRepository> logger)
    {
        this.connectionFactory = connectionFactory;
        this.logger = logger;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PermissionCheckResult>> CheckObjectPermissionsAsync(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        var qualifiedName = $"[{schemaName}].[{tableName}]";

    await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
        logger.LogInformation("开始执行权限检查: DataSourceId={DataSourceId}, Object={Object}", dataSourceId, qualifiedName);

    var objectPermissions = await QueryPermissionsAsync(connection, qualifiedName, cancellationToken);
    var databasePermissions = await QueryPermissionsAsync(connection, null, cancellationToken);

        var allPermissions = objectPermissions.Concat(databasePermissions)
            .GroupBy(x => x.PermissionName, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(x => x.Key, x => x.ToList(), StringComparer.OrdinalIgnoreCase);

        var results = new List<PermissionCheckResult>();
        foreach (var permission in PartitionPermissionRequirement.RequiredPermissions)
        {
            if (allPermissions.TryGetValue(permission, out var grantedRows) && grantedRows.Count > 0)
            {
                var scope = string.Join("/", grantedRows.Select(r => r.ScopeDisplayName));
                results.Add(new PermissionCheckResult(permission, true, scope, "权限检查通过"));
            }
            else
            {
                results.Add(new PermissionCheckResult(permission, false, string.Empty, $"缺少 {permission} 权限"));
            }
        }

        return results;
    }

    private static async Task<List<PermissionInfo>> QueryPermissionsAsync(
        SqlConnection connection,
        string? objectName,
        CancellationToken cancellationToken)
    {
        const string objectSql = "SELECT DISTINCT permission_name AS PermissionName, entity_name AS EntityName, subentity_name AS SubentityName FROM fn_my_permissions(@ObjectName, 'OBJECT')";
        const string databaseSql = "SELECT DISTINCT permission_name AS PermissionName, entity_name AS EntityName, subentity_name AS SubentityName FROM fn_my_permissions(NULL, 'DATABASE')";

        IEnumerable<PermissionRow> rows;
        string scopePrefix;
        if (string.IsNullOrWhiteSpace(objectName))
        {
            rows = await connection.QueryAsync<PermissionRow>(new CommandDefinition(databaseSql, cancellationToken: cancellationToken));
            scopePrefix = "数据库级";
        }
        else
        {
            rows = await connection.QueryAsync<PermissionRow>(new CommandDefinition(objectSql, new { ObjectName = objectName }, cancellationToken: cancellationToken));
            scopePrefix = "对象级";
        }

        return rows
            .Select(row => new PermissionInfo(row.PermissionName, BuildScopeDisplay(scopePrefix, row.EntityName, row.SubentityName)))
            .ToList();
    }

    private static string BuildScopeDisplay(string prefix, string entityName, string subentityName)
    {
        // 将权限层级与返回的实体名称组合，方便日志分析
        var segments = new List<string> { prefix };
        if (!string.IsNullOrWhiteSpace(entityName))
        {
            segments.Add(entityName);
        }

        if (!string.IsNullOrWhiteSpace(subentityName))
        {
            segments.Add(subentityName);
        }

        return string.Join(" / ", segments);
    }

    private sealed class PermissionRow
    {
        public string PermissionName { get; set; } = string.Empty;

        public string EntityName { get; set; } = string.Empty;

        public string SubentityName { get; set; } = string.Empty;
    }

    private sealed record PermissionInfo(string PermissionName, string ScopeDisplayName);
}
