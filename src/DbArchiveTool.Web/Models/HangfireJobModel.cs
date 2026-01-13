namespace DbArchiveTool.Web.Models;

/// <summary>
/// Hangfire 任务模型
/// </summary>
public class HangfireJobModel
{
    /// <summary>
    /// 任务ID
    /// </summary>
    public string JobId { get; set; } = string.Empty;

    /// <summary>
    /// 方法名称
    /// </summary>
    public string MethodName { get; set; } = string.Empty;

    /// <summary>
    /// 任务参数(JSON格式)
    /// </summary>
    public string? Arguments { get; set; }

    /// <summary>
    /// 状态: Enqueued, Scheduled, Processing, Succeeded, Failed, Deleted
    /// </summary>
    public string Status { get; set; } = string.Empty;

    /// <summary>
    /// 创建时间(UTC)
    /// </summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>
    /// 开始执行时间(UTC)
    /// </summary>
    public DateTime? StartedAtUtc { get; set; }

    /// <summary>
    /// 结束时间(UTC)
    /// </summary>
    public DateTime? FinishedAtUtc { get; set; }

    /// <summary>
    /// 预计执行时间(UTC) - 用于 Scheduled 状态
    /// </summary>
    public DateTime? ScheduledAtUtc { get; set; }

    /// <summary>
    /// 失败原因
    /// </summary>
    public string? FailureReason { get; set; }

    /// <summary>
    /// 队列名称
    /// </summary>
    public string? QueueName { get; set; }

    /// <summary>
    /// 重试次数
    /// </summary>
    public int RetryCount { get; set; }

    /// <summary>
    /// 执行耗时(秒)
    /// </summary>
    public double? DurationSeconds { get; set; }
}

/// <summary>
/// Hangfire 任务详情模型
/// </summary>
public class HangfireJobDetailModel : HangfireJobModel
{
    /// <summary>
    /// 执行SQL审计信息（Markdown）
    /// 用于在任务成功/跳过时也能展示具体执行SQL。
    /// </summary>
    public string? AuditMarkdown { get; set; }

    /// <summary>
    /// 状态历史记录
    /// </summary>
    public List<HangfireJobStateHistoryModel> StateHistory { get; set; } = new();

    /// <summary>
    /// 任务参数详情(已解析)
    /// </summary>
    public Dictionary<string, string> ParsedArguments { get; set; } = new();
}

/// <summary>
/// Hangfire 任务状态历史记录
/// </summary>
public class HangfireJobStateHistoryModel
{
    /// <summary>
    /// 状态名称
    /// </summary>
    public string StateName { get; set; } = string.Empty;

    /// <summary>
    /// 原因
    /// </summary>
    public string? Reason { get; set; }

    /// <summary>
    /// 创建时间(UTC)
    /// </summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>
    /// 附加数据
    /// </summary>
    public Dictionary<string, string> Data { get; set; } = new();
}

/// <summary>
/// Hangfire 定时任务模型
/// </summary>
public class HangfireRecurringJobModel
{
    /// <summary>
    /// 定时任务ID
    /// </summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Cron 表达式
    /// </summary>
    public string Cron { get; set; } = string.Empty;

    /// <summary>
    /// 方法名称
    /// </summary>
    public string MethodName { get; set; } = string.Empty;

    /// <summary>
    /// 队列名称
    /// </summary>
    public string? QueueName { get; set; }

    /// <summary>
    /// 下次执行时间(UTC)
    /// </summary>
    public DateTime? NextExecutionUtc { get; set; }

    /// <summary>
    /// 最后一次任务ID
    /// </summary>
    public string? LastJobId { get; set; }

    /// <summary>
    /// 最后一次执行时间(UTC)
    /// </summary>
    public DateTime? LastExecutionUtc { get; set; }

    /// <summary>
    /// 创建时间(UTC)
    /// </summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>
    /// 错误信息
    /// </summary>
    public string? Error { get; set; }

    /// <summary>
    /// 是否已暂停
    /// </summary>
    public bool IsPaused { get; set; }
}

/// <summary>
/// Hangfire 统计信息模型
/// </summary>
public class HangfireStatisticsModel
{
    /// <summary>
    /// 已入队数量
    /// </summary>
    public int EnqueuedCount { get; set; }

    /// <summary>
    /// 已调度数量
    /// </summary>
    public int ScheduledCount { get; set; }

    /// <summary>
    /// 处理中数量
    /// </summary>
    public int ProcessingCount { get; set; }

    /// <summary>
    /// 成功数量
    /// </summary>
    public int SucceededCount { get; set; }

    /// <summary>
    /// 失败数量
    /// </summary>
    public int FailedCount { get; set; }

    /// <summary>
    /// 已删除数量
    /// </summary>
    public int DeletedCount { get; set; }

    /// <summary>
    /// 定时任务数量
    /// </summary>
    public int RecurringJobCount { get; set; }

    /// <summary>
    /// 服务器数量
    /// </summary>
    public int ServerCount { get; set; }
}

/// <summary>
/// 分页结果模型
/// </summary>
public class PagedResult<T>
{
    /// <summary>
    /// 数据列表
    /// </summary>
    public List<T> Items { get; set; } = new();

    /// <summary>
    /// 总记录数
    /// </summary>
    public long TotalCount { get; set; }

    /// <summary>
    /// 当前页码(从0开始)
    /// </summary>
    public int PageIndex { get; set; }

    /// <summary>
    /// 每页数量
    /// </summary>
    public int PageSize { get; set; }

    /// <summary>
    /// 总页数
    /// </summary>
    public int TotalPages => (int)Math.Ceiling((double)TotalCount / PageSize);
}
