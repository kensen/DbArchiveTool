using System.Collections.Generic;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 定义分区执行过程中所需的数据库权限常量。
/// </summary>
public static class PartitionPermissionRequirement
{
    /// <summary>执行 ALTER 操作的权限名称。</summary>
    public const string Alter = "ALTER";

    /// <summary>执行 CONTROL 操作的权限名称。</summary>
    public const string Control = "CONTROL";

    /// <summary>查看对象定义所需的权限名称。</summary>
    public const string ViewDefinition = "VIEW DEFINITION";

    /// <summary>分区执行所需的权限集合。</summary>
    public static readonly IReadOnlyList<string> RequiredPermissions = new[]
    {
        Alter,
        Control,
        ViewDefinition
    };
}
