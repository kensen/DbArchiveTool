namespace DbArchiveTool.Application.Services.ScheduledArchiveJobs;

/// <summary>
/// 定时归档任务执行结果
/// </summary>
public class ArchiveExecutionResult
{
    /// <summary>
    /// 执行是否成功
    /// </summary>
    public bool Success { get; set; }

    /// <summary>
    /// 本次执行归档的行数
    /// </summary>
    public long ArchivedRowCount { get; set; }

    /// <summary>
    /// 错误信息(失败时)
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// 执行耗时
    /// </summary>
    public TimeSpan Duration { get; set; }

    /// <summary>
    /// 是否跳过执行(无数据可归档时)
    /// </summary>
    public bool Skipped { get; set; }

    /// <summary>
    /// 审计信息（Markdown 格式）。
    /// 用于在 Hangfire 任务详情中展示本次执行的 SQL 语句（成功/失败/跳过均可记录）。
    /// </summary>
    public string? AuditMarkdown { get; set; }

    /// <summary>
    /// 创建成功结果
    /// </summary>
    public static ArchiveExecutionResult CreateSuccess(long archivedRowCount, TimeSpan duration, string? auditMarkdown = null)
    {
        return new ArchiveExecutionResult
        {
            Success = true,
            ArchivedRowCount = archivedRowCount,
            Duration = duration,
            Skipped = false,
            AuditMarkdown = auditMarkdown
        };
    }

    /// <summary>
    /// 创建跳过结果(无数据可归档)
    /// </summary>
    public static ArchiveExecutionResult CreateSkipped(TimeSpan duration, string? auditMarkdown = null)
    {
        return new ArchiveExecutionResult
        {
            Success = true,
            ArchivedRowCount = 0,
            Duration = duration,
            Skipped = true,
            AuditMarkdown = auditMarkdown
        };
    }

    /// <summary>
    /// 创建失败结果
    /// </summary>
    public static ArchiveExecutionResult CreateFailure(string errorMessage, TimeSpan duration, string? auditMarkdown = null)
    {
        return new ArchiveExecutionResult
        {
            Success = false,
            ArchivedRowCount = 0,
            ErrorMessage = errorMessage,
            Duration = duration,
            Skipped = false,
            AuditMarkdown = auditMarkdown
        };
    }
}

/// <summary>
/// 定时归档任务执行器服务接口
/// 负责实际执行归档操作，并更新任务执行状态
/// </summary>
public interface IScheduledArchiveJobExecutor
{
    /// <summary>
    /// 执行指定的定时归档任务
    /// </summary>
    /// <param name="jobId">任务ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>执行结果</returns>
    Task<ArchiveExecutionResult> ExecuteAsync(Guid jobId, CancellationToken cancellationToken = default);
}
