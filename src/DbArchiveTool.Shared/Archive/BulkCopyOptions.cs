namespace DbArchiveTool.Shared.Archive;

/// <summary>
/// BulkCopy 执行选项
/// </summary>
public class BulkCopyOptions
{
    /// <summary>
    /// 批次大小(默认 10,000 行)
    /// 每次触发 SqlRowsCopied 事件的行数
    /// </summary>
    public int BatchSize { get; set; } = 10000;

    /// <summary>
    /// 进度通知间隔(默认 5,000 行)
    /// 每复制多少行触发一次进度回调
    /// </summary>
    public int NotifyAfterRows { get; set; } = 5000;

    /// <summary>
    /// 估计总行数(用于计算进度百分比)
    /// </summary>
    public long? EstimatedTotalRows { get; set; }

    /// <summary>
    /// 超时时间(秒),0 表示无超时限制
    /// </summary>
    public int TimeoutSeconds { get; set; } = 0;
}
