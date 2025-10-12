using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 提供针对 SQL Server 权限的检查能力。
/// </summary>
public interface IPermissionInspectionRepository
{
    /// <summary>
    /// 检查分区目标对象所需的权限授予情况。
    /// </summary>
    /// <param name="dataSourceId">数据源标识。</param>
    /// <param name="schemaName">架构名称。</param>
    /// <param name="tableName">表名称。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>权限检查结果列表。</returns>
    Task<IReadOnlyList<PermissionCheckResult>> CheckObjectPermissionsAsync(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default);
}
