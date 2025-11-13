using System;
using System.Collections.Generic;
using DbArchiveTool.Application.Partitions;

namespace DbArchiveTool.Api.Models;

/// <summary>
/// 切换分区检查请求 DTO。
/// </summary>
public sealed record SwitchArchiveInspectDto(
    Guid? PartitionConfigurationId,
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    bool CreateStagingTable,
    string RequestedBy)
{
    public SwitchPartitionInspectionRequest ToApplicationRequest()
        => new(PartitionConfigurationId, DataSourceId, SchemaName, TableName, SourcePartitionKey, TargetTable, TargetDatabase, CreateStagingTable, RequestedBy);
}

/// <summary>
/// 切换分区执行请求 DTO。
/// </summary>
public sealed record SwitchArchiveExecuteDto(
    Guid? PartitionConfigurationId,
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    bool CreateStagingTable,
    bool BackupConfirmed,
    string RequestedBy)
{
    public SwitchPartitionExecuteRequest ToApplicationRequest()
        => new(PartitionConfigurationId, DataSourceId, SchemaName, TableName, SourcePartitionKey, TargetTable, TargetDatabase, CreateStagingTable, BackupConfirmed, RequestedBy);
}

/// <summary>
/// 切换分区自动补齐请求 DTO。
/// </summary>
public sealed record SwitchArchiveAutoFixDto(
    Guid? PartitionConfigurationId,
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    bool CreateStagingTable,
    string RequestedBy,
    IReadOnlyList<string> AutoFixStepCodes)
{
    public SwitchPartitionAutoFixRequest ToApplicationRequest()
        => new(PartitionConfigurationId, DataSourceId, SchemaName, TableName, SourcePartitionKey, TargetTable, TargetDatabase, CreateStagingTable, RequestedBy, AutoFixStepCodes);
}

/// <summary>
/// BCP 归档预检请求 DTO。
/// </summary>
public sealed record BcpArchiveInspectDto(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    string TempDirectory,
    string RequestedBy)
{
    public BcpArchiveInspectRequest ToApplicationRequest()
        => new(DataSourceId, SchemaName, TableName, SourcePartitionKey, TargetTable, TargetDatabase, TempDirectory, RequestedBy);
}

/// <summary>
/// BCP/BulkCopy 归档自动修复请求 DTO。
/// </summary>
public sealed record ArchiveAutoFixDto(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string TargetTable,
    string? TargetDatabase,
    string FixCode,
    string RequestedBy)
{
    public ArchiveAutoFixRequest ToApplicationRequest()
        => new(DataSourceId, SchemaName, TableName, TargetTable, TargetDatabase, FixCode, RequestedBy);
}

/// <summary>
/// BCP 归档执行请求 DTO。
/// </summary>
public sealed record BcpArchiveExecuteDto(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    string TempDirectory,
    int BatchSize,
    bool UseNativeFormat,
    int MaxErrors,
    int TimeoutSeconds,
    bool BackupConfirmed,
    string RequestedBy)
{
    public BcpArchiveExecuteRequest ToApplicationRequest()
        => new(DataSourceId, SchemaName, TableName, SourcePartitionKey, TargetTable, TargetDatabase, 
               TempDirectory, BatchSize, UseNativeFormat, MaxErrors, TimeoutSeconds, BackupConfirmed, RequestedBy);
}

/// <summary>
/// BulkCopy 归档预检请求 DTO。
/// </summary>
public sealed record BulkCopyArchiveInspectDto(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    string RequestedBy)
{
    public BulkCopyArchiveInspectRequest ToApplicationRequest()
        => new(DataSourceId, SchemaName, TableName, SourcePartitionKey, TargetTable, TargetDatabase, RequestedBy);
}

/// <summary>
/// BulkCopy 归档执行请求 DTO。
/// </summary>
public sealed record BulkCopyArchiveExecuteDto(
    Guid DataSourceId,
    string SchemaName,
    string TableName,
    string SourcePartitionKey,
    string TargetTable,
    string? TargetDatabase,
    int BatchSize,
    int NotifyAfterRows,
    int TimeoutSeconds,
    bool EnableStreaming,
    bool BackupConfirmed,
    string RequestedBy)
{
    public BulkCopyArchiveExecuteRequest ToApplicationRequest()
        => new(DataSourceId, SchemaName, TableName, SourcePartitionKey, TargetTable, TargetDatabase,
               BatchSize, NotifyAfterRows, TimeoutSeconds, EnableStreaming, BackupConfirmed, RequestedBy);
}
