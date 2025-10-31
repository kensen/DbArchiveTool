using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DbArchiveTool.Web.Services;

/// <summary>分区切换检查请求体。</summary>
public sealed record SwitchArchiveInspectRequest(
    [property: JsonPropertyName("partitionConfigurationId")] Guid? PartitionConfigurationId,
    [property: JsonPropertyName("dataSourceId")] Guid DataSourceId,
    [property: JsonPropertyName("schemaName")] string SchemaName,
    [property: JsonPropertyName("tableName")] string TableName,
    [property: JsonPropertyName("sourcePartitionKey")] string SourcePartitionKey,
    [property: JsonPropertyName("targetTable")] string TargetTable,
    [property: JsonPropertyName("targetDatabase")] string? TargetDatabase,
    [property: JsonPropertyName("createStagingTable")] bool CreateStagingTable,
    [property: JsonPropertyName("requestedBy")] string RequestedBy);

/// <summary>分区切换执行请求体。</summary>
public sealed record SwitchArchiveExecuteRequest(
    [property: JsonPropertyName("partitionConfigurationId")] Guid? PartitionConfigurationId,
    [property: JsonPropertyName("dataSourceId")] Guid DataSourceId,
    [property: JsonPropertyName("schemaName")] string SchemaName,
    [property: JsonPropertyName("tableName")] string TableName,
    [property: JsonPropertyName("sourcePartitionKey")] string SourcePartitionKey,
    [property: JsonPropertyName("targetTable")] string TargetTable,
    [property: JsonPropertyName("targetDatabase")] string? TargetDatabase,
    [property: JsonPropertyName("createStagingTable")] bool CreateStagingTable,
    [property: JsonPropertyName("backupConfirmed")] bool BackupConfirmed,
    [property: JsonPropertyName("requestedBy")] string RequestedBy);

/// <summary>分区切换自动补齐请求体。</summary>
public sealed record SwitchArchiveAutoFixRequest(
    [property: JsonPropertyName("partitionConfigurationId")] Guid? PartitionConfigurationId,
    [property: JsonPropertyName("dataSourceId")] Guid DataSourceId,
    [property: JsonPropertyName("schemaName")] string SchemaName,
    [property: JsonPropertyName("tableName")] string TableName,
    [property: JsonPropertyName("sourcePartitionKey")] string SourcePartitionKey,
    [property: JsonPropertyName("targetTable")] string TargetTable,
    [property: JsonPropertyName("targetDatabase")] string? TargetDatabase,
    [property: JsonPropertyName("createStagingTable")] bool CreateStagingTable,
    [property: JsonPropertyName("requestedBy")] string RequestedBy,
    [property: JsonPropertyName("autoFixStepCodes")] IReadOnlyList<string> AutoFixStepCodes);
