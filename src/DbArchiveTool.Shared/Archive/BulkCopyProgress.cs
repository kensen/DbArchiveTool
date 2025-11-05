namespace DbArchiveTool.Shared.Archive;

/// <summary>
/// BulkCopy 进度信息
/// </summary>
public class BulkCopyProgress
{
    /// <summary>
    /// 已复制的行数
    /// </summary>
    public long RowsCopied { get; set; }

    /// <summary>
    /// 完成百分比 (0-100)
    /// </summary>
    public double PercentComplete { get; set; }

    /// <summary>
    /// 开始时间 (UTC)
    /// </summary>
    public DateTime? StartTimeUtc { get; set; }

    /// <summary>
    /// 估计剩余时间
    /// </summary>
    public TimeSpan? EstimatedTimeRemaining { get; set; }

    /// <summary>
    /// 当前传输速度 (行/秒)
    /// </summary>
    public double CurrentThroughput { get; set; }
}
