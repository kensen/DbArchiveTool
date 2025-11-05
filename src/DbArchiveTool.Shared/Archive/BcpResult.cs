namespace DbArchiveTool.Shared.Archive;

/// <summary>
/// BCP 执行结果
/// </summary>
public class BcpResult
{
    /// <summary>
    /// 是否成功
    /// </summary>
    public bool Succeeded { get; set; }

    /// <summary>
    /// 已复制的行数
    /// </summary>
    public long RowsCopied { get; set; }

    /// <summary>
    /// 执行耗时
    /// </summary>
    public TimeSpan Duration { get; set; }

    /// <summary>
    /// 平均吞吐量 (行/秒)
    /// </summary>
    public double ThroughputRowsPerSecond { get; set; }

    /// <summary>
    /// 错误信息 (失败时填充)
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// 临时文件路径 (仅在 KeepTempFiles=true 时保留)
    /// </summary>
    public string? TempFilePath { get; set; }

    /// <summary>
    /// BCP 命令输出日志
    /// </summary>
    public string? CommandOutput { get; set; }

    /// <summary>
    /// 开始时间 (UTC)
    /// </summary>
    public DateTime StartTimeUtc { get; set; }

    /// <summary>
    /// 结束时间 (UTC)
    /// </summary>
    public DateTime EndTimeUtc { get; set; }
}
