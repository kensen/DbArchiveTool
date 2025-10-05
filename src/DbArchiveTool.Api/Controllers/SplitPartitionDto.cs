using DbArchiveTool.Application.Partitions;

namespace DbArchiveTool.Api.Controllers;

/// <summary>
/// 拆分命令请求 DTO，用于接收前端输入。
/// </summary>
public sealed record SplitPartitionDto(
    string SchemaName,
    string TableName,
    IReadOnlyList<string> Boundaries,
    bool BackupConfirmed,
    string RequestedBy)
{
    /// <summary>
    /// 转换为应用服务使用的请求对象。
    /// </summary>
    public SplitPartitionRequest ToRequest(Guid dataSourceId)
        => new(dataSourceId, SchemaName, TableName, Boundaries, BackupConfirmed, RequestedBy);
}
