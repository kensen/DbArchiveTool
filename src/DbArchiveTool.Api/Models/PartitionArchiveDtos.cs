using System;
using DbArchiveTool.Application.Partitions;

namespace DbArchiveTool.Api.Models;

/// <summary>
/// 切换分区检查请求 DTO。
/// </summary>
public sealed record SwitchArchiveInspectDto(
    Guid PartitionConfigurationId,
    string SourcePartitionKey,
    string TargetTable,
    bool CreateStagingTable,
    string RequestedBy)
{
    public SwitchPartitionInspectionRequest ToApplicationRequest()
        => new(PartitionConfigurationId, SourcePartitionKey, TargetTable, CreateStagingTable, RequestedBy);
}

/// <summary>
/// 切换分区执行请求 DTO。
/// </summary>
public sealed record SwitchArchiveExecuteDto(
    Guid PartitionConfigurationId,
    string SourcePartitionKey,
    string TargetTable,
    bool CreateStagingTable,
    bool BackupConfirmed,
    string RequestedBy)
{
    public SwitchPartitionExecuteRequest ToApplicationRequest()
        => new(PartitionConfigurationId, SourcePartitionKey, TargetTable, CreateStagingTable, BackupConfirmed, RequestedBy);
}
