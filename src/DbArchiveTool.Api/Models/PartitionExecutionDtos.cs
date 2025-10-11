using System;

namespace DbArchiveTool.Api.Models;

/// <summary>
/// 发起分区执行的请求体。
/// </summary>
public sealed record StartPartitionExecutionDto(
    Guid PartitionConfigurationId,
    Guid DataSourceId,
    string RequestedBy,
    bool BackupConfirmed,
    string? BackupReference,
    string? Notes,
    bool ForceWhenWarnings,
    int Priority = 0);

/// <summary>
/// 取消分区执行任务的请求体。
/// </summary>
public sealed record CancelPartitionExecutionDto(
    string CancelledBy,
    string? Reason = null);
