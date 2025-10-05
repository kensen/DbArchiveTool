using System.Text.Json.Serialization;

namespace DbArchiveTool.Web.Services;

/// <summary>
/// 调用拆分预览与执行所需的请求体。
/// </summary>
internal sealed record SplitPartitionApiRequest(
    [property: JsonPropertyName("schemaName")] string SchemaName,
    [property: JsonPropertyName("tableName")] string TableName,
    [property: JsonPropertyName("boundaries")] IReadOnlyList<string> Boundaries,
    [property: JsonPropertyName("backupConfirmed")] bool BackupConfirmed,
    [property: JsonPropertyName("requestedBy")] string RequestedBy);

/// <summary>
/// 审批命令请求体。
/// </summary>
internal sealed record ApprovePartitionCommandApiRequest([property: JsonPropertyName("approver")] string Approver);

/// <summary>
/// 拒绝命令请求体。
/// </summary>
internal sealed record RejectPartitionCommandApiRequest(
    [property: JsonPropertyName("approver")] string Approver,
    [property: JsonPropertyName("reason")] string Reason);
