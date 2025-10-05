using System.Text.Json.Serialization;

namespace DbArchiveTool.Api.Models;

/// <summary>
/// 审批命令请求。
/// </summary>
public sealed record ApprovePartitionCommandDto([property: JsonPropertyName("approver")] string Approver);

/// <summary>
/// 拒绝命令请求。
/// </summary>
public sealed record RejectPartitionCommandDto(
    [property: JsonPropertyName("approver")] string Approver,
    [property: JsonPropertyName("reason")] string Reason);
