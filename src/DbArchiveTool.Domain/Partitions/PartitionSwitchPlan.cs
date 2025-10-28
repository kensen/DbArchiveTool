using System;
using System.Collections.Generic;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 描述分区切换的补齐与人工处理计划。
/// </summary>
public sealed class PartitionSwitchPlan
{
    /// <summary>
    /// 创建分区切换计划实例。
    /// </summary>
    /// <param name="blockers">需要人工处理的阻塞项。</param>
    /// <param name="autoFixes">可由系统自动补齐的步骤。</param>
    /// <param name="warnings">需要关注的风险提示。</param>
    /// <exception cref="ArgumentNullException">当任意集合参数为空时抛出。</exception>
    public PartitionSwitchPlan(
        IReadOnlyList<PartitionSwitchPlanBlocker> blockers,
        IReadOnlyList<PartitionSwitchPlanAutoFix> autoFixes,
        IReadOnlyList<PartitionSwitchPlanWarning> warnings)
    {
        Blockers = blockers ?? throw new ArgumentNullException(nameof(blockers));
        AutoFixes = autoFixes ?? throw new ArgumentNullException(nameof(autoFixes));
        Warnings = warnings ?? throw new ArgumentNullException(nameof(warnings));
    }

    /// <summary>需要人工处理的阻塞项列表。</summary>
    public IReadOnlyList<PartitionSwitchPlanBlocker> Blockers { get; }

    /// <summary>可由系统自动补齐的步骤。</summary>
    public IReadOnlyList<PartitionSwitchPlanAutoFix> AutoFixes { get; }

    /// <summary>需要关注的风险提示。</summary>
    public IReadOnlyList<PartitionSwitchPlanWarning> Warnings { get; }

    /// <summary>空计划，表示暂未生成补齐内容。</summary>
    public static PartitionSwitchPlan Empty { get; } = new(
        Array.Empty<PartitionSwitchPlanBlocker>(),
        Array.Empty<PartitionSwitchPlanAutoFix>(),
        Array.Empty<PartitionSwitchPlanWarning>());
}

/// <summary>
/// 需要人工处理的阻塞项描述。
/// </summary>
public sealed record PartitionSwitchPlanBlocker(
    string Code,
    string Title,
    string Description,
    string? ResolutionSuggestion);

/// <summary>
/// 自动补齐步骤的分类，便于前端分组展示。
/// </summary>
public enum PartitionSwitchAutoFixCategory
{
    /// <summary>创建或补齐目标表。</summary>
    CreateTargetTable,

    /// <summary>同步分区函数与分区方案。</summary>
    SyncPartitionObjects,

    /// <summary>同步索引定义。</summary>
    SyncIndexes,

    /// <summary>同步约束（主键、外键、检查、默认等）。</summary>
    SyncConstraints,

    /// <summary>刷新或补齐统计信息。</summary>
    RefreshStatistics,

    /// <summary>清理目标表残留数据。</summary>
    CleanupResidualData,

    /// <summary>其他类型的补齐步骤。</summary>
    Other
}

/// <summary>
/// 自动补齐任务的详细描述。
/// </summary>
public sealed record PartitionSwitchPlanAutoFix(
    string Code,
    string Title,
    PartitionSwitchAutoFixCategory Category,
    string ImpactScope,
    IReadOnlyList<PartitionSwitchPlanCommand> Commands,
    string? Prerequisite = null,
    bool RequiresExclusiveLock = false);

/// <summary>
/// 自动补齐涉及的单条命令描述。
/// </summary>
public sealed record PartitionSwitchPlanCommand(
    string CommandText,
    string Description);

/// <summary>
/// 需要关注的风险提示内容。
/// </summary>
public sealed record PartitionSwitchPlanWarning(
    string Code,
    string Title,
    string Description,
    string? Guidance);
