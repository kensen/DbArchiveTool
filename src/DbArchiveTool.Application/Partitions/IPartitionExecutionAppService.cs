using System;
using System.Collections.Generic;
using System.Threading;
using DbArchiveTool.Application.Partitions.Dtos;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Partitions;
using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区执行任务的应用服务接口。
/// </summary>
public interface IPartitionExecutionAppService
{
    /// <summary>发起执行任务。</summary>
    Task<Result<Guid>> StartAsync(StartPartitionExecutionRequest request, CancellationToken cancellationToken = default);

    /// <summary>获取任务详情。</summary>
    Task<Result<PartitionExecutionTaskDetailDto>> GetAsync(Guid executionTaskId, CancellationToken cancellationToken = default);

    /// <summary>列出近期任务。</summary>
    Task<Result<List<PartitionExecutionTaskSummaryDto>>> ListAsync(Guid? dataSourceId, int maxCount, CancellationToken cancellationToken = default);

    /// <summary>获取任务日志。</summary>
    Task<Result<List<PartitionExecutionLogDto>>> GetLogsAsync(Guid executionTaskId, DateTime? sinceUtc, int take, CancellationToken cancellationToken = default);

    /// <summary>取消执行任务。</summary>
    Task<Result> CancelAsync(Guid executionTaskId, string cancelledBy, string? reason = null, CancellationToken cancellationToken = default);

    /// <summary>获取执行向导上下文。</summary>
    Task<Result<ExecutionWizardContextDto>> GetExecutionContextAsync(Guid configurationId, CancellationToken cancellationToken = default);
}

/// <summary>
/// 发起执行任务的请求。
/// </summary>
public sealed record StartPartitionExecutionRequest(
    Guid PartitionConfigurationId,
    Guid DataSourceId,
    string RequestedBy,
    bool BackupConfirmed,
    string? BackupReference,
    string? Notes,
    bool ForceWhenWarnings,
    int Priority = 0);

/// <summary>
/// 执行任务摘要。
/// </summary>
public class PartitionExecutionTaskSummaryDto
{
    public Guid Id { get; set; }
    public Guid PartitionConfigurationId { get; set; }
    public Guid DataSourceId { get; set; }
    public string TaskType { get; set; } = string.Empty;
    public string DataSourceName { get; set; } = string.Empty;
    public string SourceTable { get; set; } = string.Empty;
    public string TargetTable { get; set; } = string.Empty;
    public PartitionExecutionOperationType OperationType { get; set; } = PartitionExecutionOperationType.Unknown;
    public string? ArchiveScheme { get; set; }
    public string? ArchiveTargetConnection { get; set; }
    public string? ArchiveTargetDatabase { get; set; }
    public string? ArchiveTargetTable { get; set; }
    public PartitionExecutionStatus Status { get; set; }
    public string Phase { get; set; } = string.Empty;
    public double Progress { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }
    public string RequestedBy { get; set; } = string.Empty;
    public string? FailureReason { get; set; }
    public string? BackupReference { get; set; }
}

/// <summary>
/// 执行任务详情。
/// </summary>
public sealed class PartitionExecutionTaskDetailDto : PartitionExecutionTaskSummaryDto
{
    public string? SummaryJson { get; set; }
    public string? Notes { get; set; }
}

/// <summary>
/// 执行日志 DTO。
/// </summary>
public sealed class PartitionExecutionLogDto
{
    public Guid Id { get; set; }
    public Guid ExecutionTaskId { get; set; }
    public DateTime LogTimeUtc { get; set; }
    public string Category { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public long? DurationMs { get; set; }
    public string? ExtraJson { get; set; }
}
