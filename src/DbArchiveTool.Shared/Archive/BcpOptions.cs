namespace DbArchiveTool.Shared.Archive;

/// <summary>
/// BCP 执行选项
/// </summary>
public class BcpOptions
{
    /// <summary>
    /// 批次大小(默认 10,000 行)
    /// </summary>
    public int BatchSize { get; set; } = 10000;

    /// <summary>
    /// BCP 工具路径,如果为空则使用系统 PATH 中的 bcp.exe
    /// </summary>
    public string? BcpToolPath { get; set; }

    /// <summary>
    /// 临时文件目录,如果为空则使用系统临时目录
    /// </summary>
    public string? TempDirectory { get; set; }

    /// <summary>
    /// 是否使用 Unicode 格式 (-w 参数)
    /// </summary>
    public bool UseUnicode { get; set; } = true;

    /// <summary>
    /// 是否使用原生格式 (-n 参数)
    /// </summary>
    public bool UseNativeFormat { get; set; } = false;

    /// <summary>
    /// 最大错误数,默认 10
    /// </summary>
    public int MaxErrors { get; set; } = 10;

    /// <summary>
    /// 超时时间(秒),0 表示无超时限制
    /// </summary>
    public int TimeoutSeconds { get; set; } = 0;

    /// <summary>
    /// 是否保留临时文件用于调试
    /// </summary>
    public bool KeepTempFiles { get; set; } = false;
}
