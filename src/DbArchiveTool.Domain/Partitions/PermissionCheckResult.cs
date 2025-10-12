using System;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 表示单项数据库权限检查的结果。
/// </summary>
public sealed class PermissionCheckResult
{
    /// <summary>
    /// 初始化 <see cref="PermissionCheckResult"/> 实例。
    /// </summary>
    public PermissionCheckResult(string permissionName, bool granted, string scopeDisplayName, string? detail = null)
    {
        PermissionName = string.IsNullOrWhiteSpace(permissionName)
            ? throw new ArgumentException("权限名称不能为空。", nameof(permissionName))
            : permissionName.Trim();
        Granted = granted;
        ScopeDisplayName = scopeDisplayName ?? string.Empty;
        Detail = string.IsNullOrWhiteSpace(detail) ? null : detail.Trim();
    }

    /// <summary>权限名称，例如 ALTER、CONTROL。</summary>
    public string PermissionName { get; }

    /// <summary>是否已授予该权限。</summary>
    public bool Granted { get; }

    /// <summary>权限授予范围的显示名称。</summary>
    public string ScopeDisplayName { get; }

    /// <summary>额外说明信息。</summary>
    public string? Detail { get; }
}
