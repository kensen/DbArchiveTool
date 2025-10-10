using System;
using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 记录分区执行任务的单条日志。
/// </summary>
public sealed class PartitionExecutionLogEntry : BaseEntity
{
    private PartitionExecutionLogEntry()
    {
    }

    private PartitionExecutionLogEntry(
        Guid executionTaskId,
        string category,
        string title,
        string message,
        long? durationMs,
        string? extraJson)
    {
        ExecutionTaskId = EnsureGuid(executionTaskId, nameof(executionTaskId));
        Category = EnsureNotEmpty(category, nameof(category));
        Title = EnsureNotEmpty(title, nameof(title));
        Message = EnsureNotEmpty(message, nameof(message));
        DurationMs = durationMs;
        ExtraJson = string.IsNullOrWhiteSpace(extraJson) ? null : extraJson.Trim();
        LogTimeUtc = DateTime.UtcNow;
        CreatedAtUtc = LogTimeUtc;
        UpdatedAtUtc = LogTimeUtc;
        CreatedBy = "PARTITION-EXECUTOR";
        UpdatedBy = CreatedBy;
    }

    /// <summary>所属的执行任务标识。</summary>
    public Guid ExecutionTaskId { get; private set; }

    /// <summary>日志时间（UTC）。</summary>
    public DateTime LogTimeUtc { get; private set; }

    /// <summary>日志类别，例如 Info、Warning、Error、Step。</summary>
    public string Category { get; private set; } = "Info";

    /// <summary>简要标题。</summary>
    public string Title { get; private set; } = string.Empty;

    /// <summary>详情内容。</summary>
    public string Message { get; private set; } = string.Empty;

    /// <summary>耗时（毫秒），可为空。</summary>
    public long? DurationMs { get; private set; }

    /// <summary>额外结构化信息（JSON）。</summary>
    public string? ExtraJson { get; private set; }

    /// <summary>
    /// 创建日志记录。
    /// </summary>
    public static PartitionExecutionLogEntry Create(
        Guid executionTaskId,
        string category,
        string title,
        string message,
        long? durationMs = null,
        string? extraJson = null)
        => new(executionTaskId, category, title, message, durationMs, extraJson);

    private static Guid EnsureGuid(Guid value, string field)
    {
        if (value == Guid.Empty)
        {
            throw new ArgumentException("标识符不能为空。", field);
        }

        return value;
    }

    private static string EnsureNotEmpty(string value, string field)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("参数不能为空。", field);
        }

        return value.Trim();
    }
}
