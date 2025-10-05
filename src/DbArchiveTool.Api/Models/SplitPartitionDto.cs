using System.Text.Json.Serialization;
using DbArchiveTool.Application.Partitions;

namespace DbArchiveTool.Api.Models;

/// <summary>
/// 拆分命令请求 DTO，用于接收前端输入。
/// </summary>
public sealed record SplitPartitionDto(
    [property: JsonPropertyName("schemaName")] string SchemaName,
    [property: JsonPropertyName("tableName")] string TableName,
    [property: JsonPropertyName("boundaries")] IReadOnlyList<string> Boundaries,
    [property: JsonPropertyName("backupConfirmed")] bool BackupConfirmed,
    [property: JsonPropertyName("requestedBy")] string RequestedBy)
{
    /// <summary>
    /// 转换为应用服务使用的请求对象。
    /// </summary>
    public SplitPartitionRequest ToRequest(Guid dataSourceId)
        => new(dataSourceId, SchemaName, TableName, Boundaries, BackupConfirmed, RequestedBy);
}
