using System;
using System.Collections.Generic;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 切换分区检查请求。
/// </summary>
public sealed record SwitchPartitionInspectionRequest(
    Guid? PartitionConfigurationId,
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    bool CreateStagingTable,
    string RequestedBy);

/// <summary>
/// 切换分区执行请求。
/// </summary>
public sealed record SwitchPartitionExecuteRequest(
    Guid? PartitionConfigurationId,
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
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
    IReadOnlyList<PartitionSwitchAutoFixStepDto> AutoFixSteps,
    PartitionSwitchTableInfoDto SourceTable,
    PartitionSwitchTableInfoDto TargetTable,
    PartitionSwitchPlanDto Plan);

/// <summary>
/// 切换检查中发现的提示信息。
/// </summary>
public sealed record PartitionSwitchIssueDto(
    string Code,
    string Message,
    string? Recommendation);

/// <summary>
/// 可由系统自动补齐的步骤信息。
/// </summary>
public sealed record PartitionSwitchAutoFixStepDto(
    string Code,
    string Description,
    string? Recommendation);

/// <summary>
/// 分区切换补齐计划。
/// </summary>
public sealed record PartitionSwitchPlanDto(
    IReadOnlyList<PartitionSwitchPlanBlockerDto> Blockers,
    IReadOnlyList<PartitionSwitchPlanAutoFixDto> AutoFixes,
    IReadOnlyList<PartitionSwitchPlanWarningDto> Warnings);

/// <summary>
/// 分区切换补齐计划中的阻塞项。
/// </summary>
public sealed record PartitionSwitchPlanBlockerDto(
    string Code,
    string Title,
    string Description,
    string? ResolutionSuggestion);

/// <summary>
/// 分区切换补齐计划中的自动补齐步骤。
/// </summary>
public sealed record PartitionSwitchPlanAutoFixDto(
    string Code,
    string Title,
    string Category,
    string ImpactScope,
    IReadOnlyList<PartitionSwitchPlanCommandDto> Commands,
    string? Prerequisite,
    bool RequiresExclusiveLock);

/// <summary>
/// 自动补齐步骤中需要执行的单条命令。
/// </summary>
public sealed record PartitionSwitchPlanCommandDto(
    string CommandText,
    string Description);

/// <summary>
/// 分区切换补齐计划中的警告信息。
/// </summary>
public sealed record PartitionSwitchPlanWarningDto(
    string Code,
    string Title,
    string Description,
    string? Guidance);

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

/// <summary>
/// 自动补齐执行请求，携带用户勾选的补齐步骤编码。
/// </summary>
public sealed record SwitchPartitionAutoFixRequest(
    Guid? PartitionConfigurationId,
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    bool CreateStagingTable,
    string RequestedBy,
    IReadOnlyList<string> AutoFixStepCodes);

/// <summary>
/// 自动补齐执行结果。
/// </summary>
public sealed record PartitionSwitchAutoFixResultDto(
    bool Succeeded,
    IReadOnlyList<PartitionSwitchAutoFixExecutionDto> Executions,
    string CombinedLog);

/// <summary>
/// 自动补齐执行明细。
/// </summary>
public sealed record PartitionSwitchAutoFixExecutionDto(
    string Code,
    bool Succeeded,
    string Message,
    string Script,
    long ElapsedMilliseconds);
