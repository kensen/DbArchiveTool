using System.Collections.Generic;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 封装分区操作的安全前置条件与风险提示，辅助应用层决策。
/// </summary>
public sealed class PartitionSafetyRule
{
    /// <summary>是否要求目标分区在执行前为空。</summary>
    public bool RequiresEmptyPartition { get; }

    /// <summary>允许执行的锁模式列表。</summary>
    public IReadOnlyCollection<string> AllowedLockModes { get; }

    /// <summary>推荐的执行时段说明。</summary>
    public string ExecutionWindowHint { get; }

    /// <summary>额外的风险提示集合。</summary>
    public IReadOnlyCollection<string> AdditionalWarnings { get; }

    /// <summary>
    /// 创建安全规则。
    /// </summary>
    public PartitionSafetyRule(
        bool requiresEmptyPartition,
        IReadOnlyCollection<string> allowedLockModes,
        string executionWindowHint,
        IReadOnlyCollection<string>? additionalWarnings = null)
    {
        RequiresEmptyPartition = requiresEmptyPartition;
        AllowedLockModes = allowedLockModes;
        ExecutionWindowHint = executionWindowHint;
        AdditionalWarnings = additionalWarnings ?? Array.Empty<string>();
    }
}
