using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 定义分区切换前的检查上下文。
/// </summary>
public sealed record PartitionSwitchInspectionContext(
    string SourcePartitionKey,
    string TargetSchema,
    string TargetTable,
    bool CreateStagingTable,
    string TargetDatabase,
    string SourceDatabase,
    bool UseSourceAsTarget);

/// <summary>
/// 表示分区切换检查的列信息。
/// </summary>
public sealed record PartitionSwitchColumnInfo(
    string Name,
    string DataType,
    int? MaxLength,
    byte? Precision,
    int? Scale,
    bool IsNullable,
    bool IsIdentity,
    bool IsComputed);

/// <summary>
/// 表示分区切换检查的表结构信息。
/// </summary>
public sealed record PartitionSwitchTableInfo(
    string SchemaName,
    string TableName,
    long RowCount,
    IReadOnlyList<PartitionSwitchColumnInfo> Columns);

/// <summary>
/// 表示检查过程中产生的阻塞或警告信息。
/// </summary>
public sealed record PartitionSwitchIssue(
    string Code,
    string Message,
    string? Recommendation = null);

/// <summary>
/// 分区切换检查结果。
/// </summary>
public sealed class PartitionSwitchInspectionResult
{
    public PartitionSwitchInspectionResult(
        bool canSwitch,
        IReadOnlyList<PartitionSwitchIssue> blockingIssues,
        IReadOnlyList<PartitionSwitchIssue> warnings,
        IReadOnlyList<PartitionSwitchAutoFixStep> autoFixSteps,
        PartitionSwitchTableInfo sourceTable,
        PartitionSwitchTableInfo targetTable,
        PartitionSwitchPlan plan)
    {
        CanSwitch = canSwitch;
        BlockingIssues = blockingIssues;
        Warnings = warnings;
        AutoFixSteps = autoFixSteps;
        SourceTable = sourceTable ?? throw new ArgumentNullException(nameof(sourceTable));
        TargetTable = targetTable ?? throw new ArgumentNullException(nameof(targetTable));
        Plan = plan ?? throw new ArgumentNullException(nameof(plan));
    }

    /// <summary>是否满足切换条件。</summary>
    public bool CanSwitch { get; }

    /// <summary>阻塞切换的原因列表。</summary>
    public IReadOnlyList<PartitionSwitchIssue> BlockingIssues { get; }

    /// <summary>提供的警告信息。</summary>
    public IReadOnlyList<PartitionSwitchIssue> Warnings { get; }

    /// <summary>系统可自动补齐的步骤建议。</summary>
    public IReadOnlyList<PartitionSwitchAutoFixStep> AutoFixSteps { get; }

    /// <summary>源表信息。</summary>
    public PartitionSwitchTableInfo SourceTable { get; }

    /// <summary>目标表信息。</summary>
    public PartitionSwitchTableInfo TargetTable { get; }

    /// <summary>补齐与人工处理计划。</summary>
    public PartitionSwitchPlan Plan { get; }
}

/// <summary>
/// 描述分区切换时可由系统自动补齐的步骤。
/// </summary>
public sealed record PartitionSwitchAutoFixStep(
    string Code,
    string Description,
    string? Recommendation = null);

/// <summary>
/// 定义分区切换前检查服务接口。
/// </summary>
public interface IPartitionSwitchInspectionService
{
    Task<PartitionSwitchInspectionResult> InspectAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        PartitionSwitchInspectionContext context,
        CancellationToken cancellationToken = default);
}
