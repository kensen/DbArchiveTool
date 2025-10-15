using System;
using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 记录分区相关操作的审计日志，便于追踪配置与执行的历史行为。
/// </summary>
public sealed class PartitionAuditLog : BaseEntity
{
    private PartitionAuditLog()
    {
    }

    private PartitionAuditLog(
        string userId,
        string action,
        string resourceType,
        string resourceId,
        string? summary,
        string? payloadJson,
        string result,
        string? script)
    {
        UserId = EnsureNotEmpty(userId, nameof(userId));
        Action = EnsureNotEmpty(action, nameof(action));
        ResourceType = EnsureNotEmpty(resourceType, nameof(resourceType));
        ResourceId = EnsureNotEmpty(resourceId, nameof(resourceId));
        Summary = Normalize(summary);
        PayloadJson = Normalize(payloadJson);
        Result = string.IsNullOrWhiteSpace(result) ? "Success" : result.Trim();
        Script = Normalize(script);
        OccurredAtUtc = DateTime.UtcNow;

        CreatedAtUtc = OccurredAtUtc;
        UpdatedAtUtc = OccurredAtUtc;
        CreatedBy = UserId;
        UpdatedBy = UserId;
    }

    /// <summary>触发操作的用户。</summary>
    public string UserId { get; private set; } = string.Empty;

    /// <summary>动作名称，例如 AddBoundary、SplitBoundary。</summary>
    public string Action { get; private set; } = string.Empty;

    /// <summary>资源类型，例如 PartitionConfiguration。</summary>
    public string ResourceType { get; private set; } = string.Empty;

    /// <summary>资源标识，例如配置 Id。</summary>
    public string ResourceId { get; private set; } = string.Empty;

    /// <summary>审计时间（UTC）。</summary>
    public DateTime OccurredAtUtc { get; private set; }

    /// <summary>操作摘要，用于列表展示。</summary>
    public string? Summary { get; private set; }

    /// <summary>详细参数（JSON）。</summary>
    public string? PayloadJson { get; private set; }

    /// <summary>执行结果（Success/Failure）。</summary>
    public string Result { get; private set; } = "Success";

    /// <summary>相关 SQL 脚本内容（可选）。</summary>
    public string? Script { get; private set; }

    /// <summary>创建审计日志记录。</summary>
    public static PartitionAuditLog Create(
        string userId,
        string action,
        string resourceType,
        string resourceId,
        string? summary,
        string? payloadJson,
        string result = "Success",
        string? script = null)
        => new(userId, action, resourceType, resourceId, summary, payloadJson, result, script);

    private static string EnsureNotEmpty(string value, string field)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("参数不能为空。", field);
        }

        return value.Trim();
    }

    private static string? Normalize(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
