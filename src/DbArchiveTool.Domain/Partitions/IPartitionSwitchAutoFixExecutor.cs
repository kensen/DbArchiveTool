using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 定义分区切换自动补齐执行器，用于根据计划逐条执行补齐脚本。
/// </summary>
public interface IPartitionSwitchAutoFixExecutor
{
    /// <summary>
    /// 执行所选自动补齐步骤，返回执行明细。
    /// </summary>
    Task<PartitionSwitchAutoFixResult> ExecuteAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        PartitionSwitchInspectionContext context,
        IReadOnlyList<PartitionSwitchPlanAutoFix> steps,
        CancellationToken cancellationToken = default);
}
