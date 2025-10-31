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
