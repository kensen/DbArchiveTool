namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 描述分区命令的处理状态，覆盖审批、执行与完成阶段。
/// </summary>
public enum PartitionCommandStatus
{
    /// <summary>待审批。</summary>
    PendingApproval = 1,
    /// <summary>审批通过。</summary>
    Approved = 2,
    /// <summary>执行中。</summary>
    Executing = 3,
    /// <summary>执行成功。</summary>
    Succeeded = 4,
    /// <summary>执行失败。</summary>
    Failed = 5,
    /// <summary>审批被拒绝。</summary>
    Rejected = 6
}
