using System;
using System.Collections.Generic;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 切换分区检查请求。
/// </summary>
public sealed record SwitchPartitionInspectionRequest(
    Guid PartitionConfigurationId,
    string SourcePartitionKey,
    string TargetTable,
    bool CreateStagingTable,
    string RequestedBy);

/// <summary>
/// 切换分区执行请求。
/// </summary>
public sealed record SwitchPartitionExecuteRequest(
    Guid PartitionConfigurationId,
    string SourcePartitionKey,
    string TargetTable,
    bool CreateStagingTable,
    bool BackupConfirmed,
    string RequestedBy);

/// <summary>
/// 切换分区检查结果。
/// </summary>
public sealed record PartitionSwitchInspectionResultDto(
    bool CanExecute,
    IReadOnlyList<PartitionSwitchIssueDto> BlockingIssues,
    IReadOnlyList<PartitionSwitchIssueDto> Warnings,
    PartitionSwitchTableInfoDto SourceTable,
    PartitionSwitchTableInfoDto TargetTable);

/// <summary>
/// 切换检查中发现的提示信息。
/// </summary>
public sealed record PartitionSwitchIssueDto(
    string Code,
    string Message,
    string? Recommendation);

/// <summary>
/// 切换检查的表结构信息。
/// </summary>
public sealed record PartitionSwitchTableInfoDto(
    string SchemaName,
    string TableName,
    long RowCount,
    IReadOnlyList<PartitionSwitchColumnDto> Columns);

/// <summary>
/// 切换检查的列信息。
/// </summary>
public sealed record PartitionSwitchColumnDto(
    string Name,
    string DataType,
    int? MaxLength,
    byte? Precision,
    int? Scale,
    bool IsNullable,
    bool IsIdentity,
    bool IsComputed);
