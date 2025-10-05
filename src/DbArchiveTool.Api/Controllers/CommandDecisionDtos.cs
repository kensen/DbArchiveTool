using DbArchiveTool.Application.Partitions;

namespace DbArchiveTool.Api.Controllers;

/// <summary>
/// 审批命令请求。
/// </summary>
public sealed record ApprovePartitionCommandDto(string Approver);

/// <summary>
/// 拒绝命令请求。
/// </summary>
public sealed record RejectPartitionCommandDto(string Approver, string Reason);
