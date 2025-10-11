using System;
using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 表示针对某个分区配置草稿执行的后台任务。
/// </summary>
public sealed class PartitionExecutionTask : AggregateRoot
{
    private const string DefaultPhase = PartitionExecutionPhases.PendingValidation;
    private const string DefaultUser = "PARTITION-EXECUTOR";

    private PartitionExecutionTask()
    {
        Phase = DefaultPhase;
        Status = PartitionExecutionStatus.PendingValidation;
        LastHeartbeatUtc = DateTime.UtcNow;
    }

    private PartitionExecutionTask(
        Guid partitionConfigurationId,
        Guid dataSourceId,
        string requestedBy,
        string? backupReference,
        string? notes,
        int priority)
        : this()
    {
        PartitionConfigurationId = EnsureGuid(partitionConfigurationId, nameof(partitionConfigurationId));
        DataSourceId = EnsureGuid(dataSourceId, nameof(dataSourceId));
        RequestedBy = EnsureNotEmpty(requestedBy, nameof(requestedBy));
        BackupReference = NormalizeOptional(backupReference);
        Notes = NormalizeOptional(notes);
        Priority = priority;
    }

    /// <summary>关联的分区配置草稿标识。</summary>
    public Guid PartitionConfigurationId { get; private set; }

    /// <summary>分区所在的归档数据源标识。</summary>
    public Guid DataSourceId { get; private set; }

    /// <summary>任务当前状态。</summary>
    public PartitionExecutionStatus Status { get; private set; }

    /// <summary>业务阶段，例如“校验”“分区执行”“索引重建”。</summary>
    public string Phase { get; private set; } = DefaultPhase;

    /// <summary>整体进度（0~1）。</summary>
    public double Progress { get; private set; }

    /// <summary>排队时间（UTC）。</summary>
    public DateTime? QueuedAtUtc { get; private set; }

    /// <summary>实际开始执行时间（UTC）。</summary>
    public DateTime? StartedAtUtc { get; private set; }

    /// <summary>任务完成时间（UTC）。</summary>
    public DateTime? CompletedAtUtc { get; private set; }

    /// <summary>最近一次心跳时间，用于检测僵尸任务。</summary>
    public DateTime LastHeartbeatUtc { get; private set; } = DateTime.UtcNow;

    /// <summary>若任务失败，记录失败原因。</summary>
    public string? FailureReason { get; private set; }

    /// <summary>执行摘要信息（JSON）。</summary>
    public string? SummaryJson { get; private set; }

    /// <summary>配置快照(JSON格式,保存执行时的配置详情)。</summary>
    public string? ConfigurationSnapshot { get; private set; }

    /// <summary>最后一个检查点(用于失败恢复或进度展示)。</summary>
    public string? LastCheckpoint { get; private set; }

    /// <summary>任务发起人。</summary>
    public string RequestedBy { get; private set; } = DefaultUser;

    /// <summary>用户填写的备份参考信息。</summary>
    public string? BackupReference { get; private set; }

    /// <summary>用户提交执行时的备注。</summary>
    public string? Notes { get; private set; }

    /// <summary>任务优先级，数值越大优先级越高。</summary>
    public int Priority { get; private set; }

    /// <summary>是否允许后台继续执行（软删除标记仍继承 BaseEntity）。</summary>
    public bool IsCompleted => Status is PartitionExecutionStatus.Succeeded or PartitionExecutionStatus.Failed or PartitionExecutionStatus.Cancelled;

    /// <summary>
    /// 创建新的分区执行任务。
    /// </summary>
    public static PartitionExecutionTask Create(
        Guid partitionConfigurationId,
        Guid dataSourceId,
        string requestedBy,
        string createdBy,
        string? backupReference = null,
        string? notes = null,
        int priority = 0)
    {
        var task = new PartitionExecutionTask(partitionConfigurationId, dataSourceId, requestedBy, backupReference, notes, priority);
        task.InitializeAudit(createdBy);
        return task;
    }

    /// <summary>开始执行前标记为校验中。</summary>
    public void MarkValidating(string user)
    {
        EnsureStatus(PartitionExecutionStatus.PendingValidation, nameof(MarkValidating));
        Status = PartitionExecutionStatus.Validating;
        UpdateHeartbeat(user);
    }

    /// <summary>校验完成后加入执行队列。</summary>
    public void MarkQueued(string user)
    {
        if (Status is not PartitionExecutionStatus.PendingValidation and not PartitionExecutionStatus.Validating)
        {
            throw new InvalidOperationException("只有待校验或校验中的任务才能进入队列。");
        }

        Status = PartitionExecutionStatus.Queued;
        QueuedAtUtc = DateTime.UtcNow;
        UpdateHeartbeat(user);
    }

    /// <summary>标记任务进入执行。</summary>
    public void MarkRunning(string user)
    {
        if (Status is not PartitionExecutionStatus.Queued)
        {
            throw new InvalidOperationException("只有排队中的任务才能进入执行。");
        }

        Status = PartitionExecutionStatus.Running;
        StartedAtUtc = DateTime.UtcNow;
        Phase = PartitionExecutionPhases.Executing;
        UpdateHeartbeat(user);
    }

    /// <summary>更新任务阶段说明。</summary>
    public void UpdatePhase(string phase, string user)
    {
        Phase = string.IsNullOrWhiteSpace(phase) ? DefaultPhase : phase.Trim();
        UpdateHeartbeat(user);
    }

    /// <summary>保存配置快照(在任务启动时调用)。</summary>
    public void SaveConfigurationSnapshot(string snapshotJson, string user)
    {
        ConfigurationSnapshot = EnsureNotEmpty(snapshotJson, nameof(snapshotJson));
        UpdateHeartbeat(user);
    }

    /// <summary>更新检查点(记录执行到哪个步骤)。</summary>
    public void UpdateCheckpoint(string checkpoint, string user)
    {
        LastCheckpoint = EnsureNotEmpty(checkpoint, nameof(checkpoint));
        UpdateHeartbeat(user);
    }

    /// <summary>更新任务进度。</summary>
    public void UpdateProgress(double progress, string user)
    {
        if (Status is not PartitionExecutionStatus.Running and not PartitionExecutionStatus.Validating)
        {
            throw new InvalidOperationException("仅在校验或执行阶段允许更新进度。");
        }

        Progress = Math.Clamp(progress, 0d, 1d);
        UpdateHeartbeat(user);
    }

    /// <summary>记录心跳时间，后台调度会周期调用。</summary>
    public void UpdateHeartbeat(string user)
    {
        LastHeartbeatUtc = DateTime.UtcNow;
        Touch(string.IsNullOrWhiteSpace(user) ? DefaultUser : user.Trim());
    }

    /// <summary>执行成功。</summary>
    public void MarkSucceeded(string user, string? summaryJson = null)
    {
        if (Status is not PartitionExecutionStatus.Running and not PartitionExecutionStatus.Validating)
        {
            throw new InvalidOperationException("只有执行中的任务才能标记为成功。");
        }

        Status = PartitionExecutionStatus.Succeeded;
        CompletedAtUtc = DateTime.UtcNow;
        Progress = 1d;
        SummaryJson = NormalizeOptional(summaryJson);
        FailureReason = null;
        UpdateHeartbeat(user);
    }

    /// <summary>执行失败。</summary>
    public void MarkFailed(string user, string reason, string? summaryJson = null)
    {
        if (Status is PartitionExecutionStatus.Succeeded or PartitionExecutionStatus.Failed or PartitionExecutionStatus.Cancelled)
        {
            throw new InvalidOperationException("任务已结束，无法标记失败。");
        }

        Status = PartitionExecutionStatus.Failed;
        CompletedAtUtc = DateTime.UtcNow;
        FailureReason = EnsureNotEmpty(reason, nameof(reason));
        SummaryJson = NormalizeOptional(summaryJson);
        UpdateHeartbeat(user);
    }

    /// <summary>取消任务（仅限排队前）。</summary>
    public void Cancel(string user, string? reason = null)
    {
        if (Status is not PartitionExecutionStatus.PendingValidation and not PartitionExecutionStatus.Validating and not PartitionExecutionStatus.Queued)
        {
            throw new InvalidOperationException("仅允许在排队前取消任务。");
        }

        Status = PartitionExecutionStatus.Cancelled;
        CompletedAtUtc = DateTime.UtcNow;
        FailureReason = NormalizeOptional(reason);
        UpdateHeartbeat(user);
    }

    private void EnsureStatus(PartitionExecutionStatus expected, string operation)
    {
        if (Status != expected)
        {
            throw new InvalidOperationException($"{operation} 仅适用于 {expected} 状态的任务。");
        }
    }

    private static Guid EnsureGuid(Guid value, string field)
    {
        if (value == Guid.Empty)
        {
            throw new ArgumentException("标识符不能为空。", field);
        }

        return value;
    }

    private static string EnsureNotEmpty(string value, string field)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("参数不能为空。", field);
        }

        return value.Trim();
    }

    private static string? NormalizeOptional(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}

/// <summary>分区执行任务状态。</summary>
public enum PartitionExecutionStatus
{
    PendingValidation = 0,
    Validating = 1,
    Queued = 2,
    Running = 3,
    Succeeded = 4,
    Failed = 5,
    Cancelled = 6
}

/// <summary>预设的阶段名称常量。</summary>
public static class PartitionExecutionPhases
{
    public const string PendingValidation = "PendingValidation";
    public const string Validation = "Validation";
    public const string BackupCheck = "BackupCheck";
    public const string PermissionCheck = "PermissionCheck";
    public const string SafetyCheck = "SafetyCheck";
    public const string AlterNullability = "AlterNullability";
    public const string DropIndex = "DropIndex";
    public const string SplitPartition = "SplitPartition";
    public const string RebuildIndex = "RebuildIndex";
    public const string UpdateStatistics = "UpdateStatistics";
    public const string Executing = "Executing";
    public const string RebuildingIndexes = "RebuildingIndexes";
    public const string Finalizing = "Finalizing";
}
