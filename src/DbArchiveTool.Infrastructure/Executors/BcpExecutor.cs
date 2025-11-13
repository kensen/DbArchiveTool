using System.Diagnostics;
using System.Text;
using DbArchiveTool.Shared.Archive;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// BCP 命令行工具执行器
/// 封装 BCP 工具的调用,处理文件导出/导入、进度跟踪和清理
/// </summary>
public class BcpExecutor
{
    private readonly ILogger<BcpExecutor> _logger;

    public BcpExecutor(ILogger<BcpExecutor> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// 执行 BCP 归档(导出源数据到文件,再导入到目标)
    /// </summary>
    /// <param name="sourceConnectionString">源数据库连接字符串</param>
    /// <param name="targetConnectionString">目标数据库连接字符串</param>
    /// <param name="sourceQuery">源数据查询 SQL</param>
    /// <param name="targetTable">目标表名 (格式: [schema].[table])</param>
    /// <param name="options">BCP 执行选项</param>
    /// <param name="progress">进度回调</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>BCP 执行结果</returns>
    public async Task<BcpResult> ExecuteAsync(
        string sourceConnectionString,
        string targetConnectionString,
        string sourceQuery,
        string targetTable,
        BcpOptions options,
        IProgress<BulkCopyProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(sourceConnectionString))
        {
            throw new ArgumentException("源连接字符串不能为空", nameof(sourceConnectionString));
        }

        if (string.IsNullOrWhiteSpace(targetConnectionString))
        {
            throw new ArgumentException("目标连接字符串不能为空", nameof(targetConnectionString));
        }

        if (string.IsNullOrWhiteSpace(sourceQuery))
        {
            throw new ArgumentException("源查询 SQL 不能为空", nameof(sourceQuery));
        }

        if (string.IsNullOrWhiteSpace(targetTable))
        {
            throw new ArgumentException("目标表名不能为空", nameof(targetTable));
        }

        var startTimeUtc = DateTime.UtcNow;
        var stopwatch = Stopwatch.StartNew();
        var totalRowsCopied = 0L;
        string? tempFilePath = null;

        try
        {
            // 1. 准备临时文件路径
            var tempDir = string.IsNullOrWhiteSpace(options.TempDirectory)
                ? Path.GetTempPath()
                : options.TempDirectory;

            // 确保临时目录存在
            if (!Directory.Exists(tempDir))
            {
                _logger.LogWarning("临时目录不存在，正在创建: {TempDir}", tempDir);
                Directory.CreateDirectory(tempDir);
            }

            tempFilePath = Path.Combine(tempDir, $"bcp_export_{Guid.NewGuid():N}.dat");

            _logger.LogInformation(
                "开始 BCP 归档: 目标表={TargetTable}, 临时目录={TempDir}, 临时文件={TempFile}",
                targetTable, tempDir, tempFilePath);

            // 2. 执行 BCP 导出
            var exportResult = await ExportDataAsync(
                sourceConnectionString,
                sourceQuery,
                tempFilePath,
                options,
                cancellationToken);

            if (!exportResult.Success)
            {
                return new BcpResult
                {
                    Succeeded = false,
                    RowsCopied = 0,
                    Duration = stopwatch.Elapsed,
                    ErrorMessage = $"BCP 导出失败: {exportResult.ErrorMessage}",
                    CommandOutput = exportResult.Output,
                    StartTimeUtc = startTimeUtc,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            totalRowsCopied = exportResult.RowCount;

            // 检查导出文件
            if (File.Exists(tempFilePath))
            {
                var fileInfo = new FileInfo(tempFilePath);
                _logger.LogInformation(
                    "BCP 导出完成: {RowCount:N0} 行, 耗时={Duration}, 文件大小={FileSize:N0} 字节, 文件路径={FilePath}",
                    totalRowsCopied, exportResult.Duration, fileInfo.Length, tempFilePath);
            }
            else
            {
                _logger.LogWarning(
                    "BCP 导出完成但文件不存在: {RowCount:N0} 行, 耗时={Duration}, 预期路径={FilePath}",
                    totalRowsCopied, exportResult.Duration, tempFilePath);
            }

            // 3. 执行 BCP 导入
            var importResult = await ImportDataAsync(
                targetConnectionString,
                targetTable,
                tempFilePath,
                options,
                progress,
                totalRowsCopied,
                startTimeUtc,
                cancellationToken);

            if (!importResult.Success)
            {
                return new BcpResult
                {
                    Succeeded = false,
                    RowsCopied = totalRowsCopied,
                    Duration = stopwatch.Elapsed,
                    ErrorMessage = $"BCP 导入失败: {importResult.ErrorMessage}",
                    CommandOutput = $"Export:\n{exportResult.Output}\n\nImport:\n{importResult.Output}",
                    TempFilePath = options.KeepTempFiles ? tempFilePath : null,
                    StartTimeUtc = startTimeUtc,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            stopwatch.Stop();
            var duration = stopwatch.Elapsed;
            var throughput = duration.TotalSeconds > 0
                ? totalRowsCopied / duration.TotalSeconds
                : 0;

            _logger.LogInformation(
                "BCP 归档完成: 总行数={TotalRows:N0}, 总耗时={Duration}, 吞吐量={Throughput:N0} 行/秒",
                totalRowsCopied, duration, throughput);

            return new BcpResult
            {
                Succeeded = true,
                RowsCopied = totalRowsCopied,
                Duration = duration,
                ThroughputRowsPerSecond = throughput,
                CommandOutput = $"Export:\n{exportResult.Output}\n\nImport:\n{importResult.Output}",
                TempFilePath = options.KeepTempFiles ? tempFilePath : null,
                StartTimeUtc = startTimeUtc,
                EndTimeUtc = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();

            _logger.LogError(
                ex,
                "BCP 归档失败: 已复制 {RowsCopied:N0} 行, 耗时={Duration}",
                totalRowsCopied, stopwatch.Elapsed);

            return new BcpResult
            {
                Succeeded = false,
                RowsCopied = totalRowsCopied,
                Duration = stopwatch.Elapsed,
                ThroughputRowsPerSecond = stopwatch.Elapsed.TotalSeconds > 0
                    ? totalRowsCopied / stopwatch.Elapsed.TotalSeconds
                    : 0,
                ErrorMessage = ex.Message,
                TempFilePath = options.KeepTempFiles ? tempFilePath : null,
                StartTimeUtc = startTimeUtc,
                EndTimeUtc = DateTime.UtcNow
            };
        }
        finally
        {
            // 4. 清理临时文件
            if (!options.KeepTempFiles && !string.IsNullOrWhiteSpace(tempFilePath) && File.Exists(tempFilePath))
            {
                try
                {
                    File.Delete(tempFilePath);
                    _logger.LogDebug("临时文件已删除: {TempFile}", tempFilePath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "删除临时文件失败: {TempFile}", tempFilePath);
                }
            }
        }
    }

    /// <summary>
    /// 执行 BCP 导出
    /// </summary>
    private async Task<BcpCommandResult> ExportDataAsync(
        string connectionString,
        string query,
        string outputFile,
        BcpOptions options,
        CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // 构建 BCP 导出命令
            // bcp "SELECT * FROM Table" queryout "file.dat" -c -S server -d database -U user -P password
            var bcpPath = GetBcpToolPath(options);
            var arguments = BuildExportArguments(connectionString, query, outputFile, options);

            _logger.LogDebug("执行 BCP 导出: {BcpPath} {Arguments}", bcpPath, MaskPassword(arguments));

            var result = await ExecuteBcpCommandAsync(bcpPath, arguments, cancellationToken);

            stopwatch.Stop();

            if (result.ExitCode != 0)
            {
                return new BcpCommandResult
                {
                    Success = false,
                    Output = result.Output,
                    ErrorMessage = $"BCP 进程退出码 {result.ExitCode}",
                    Duration = stopwatch.Elapsed
                };
            }

            // 解析导出的行数
            var rowCount = ParseRowCountFromOutput(result.Output);

            return new BcpCommandResult
            {
                Success = true,
                Output = result.Output,
                RowCount = rowCount,
                Duration = stopwatch.Elapsed
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "BCP 导出执行异常");

            return new BcpCommandResult
            {
                Success = false,
                ErrorMessage = ex.Message,
                Duration = stopwatch.Elapsed
            };
        }
    }

    /// <summary>
    /// 执行 BCP 导入
    /// </summary>
    private async Task<BcpCommandResult> ImportDataAsync(
        string connectionString,
        string tableName,
        string inputFile,
        BcpOptions options,
        IProgress<BulkCopyProgress>? progress,
        long totalRows,
        DateTime startTimeUtc,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // 构建 BCP 导入命令
            // bcp [schema].[table] in "file.dat" -c -S server -d database -U user -P password -b batchsize
            var bcpPath = GetBcpToolPath(options);
            var arguments = BuildImportArguments(connectionString, tableName, inputFile, options);

            _logger.LogDebug("执行 BCP 导入: {BcpPath} {Arguments}", bcpPath, MaskPassword(arguments));

            var result = await ExecuteBcpCommandAsync(
                bcpPath,
                arguments,
                cancellationToken,
                (output) => ReportProgress(output, progress, totalRows, startTimeUtc));

            stopwatch.Stop();

            if (result.ExitCode != 0)
            {
                // 检查是否为字符串截断错误
                var isTruncationError = result.Output.Contains("SQLState = 22001") ||
                                       result.Output.Contains("String data, right truncation") ||
                                       result.Output.Contains("字符串数据，右截断");

                var errorMessage = isTruncationError
                    ? $"BCP 导入失败: 字符串截断错误 (SQLState=22001)。\n" +
                      $"可能原因:\n" +
                      $"1. 源表和目标表的列长度定义不匹配\n" +
                      $"2. 源数据包含超长字段值\n" +
                      $"3. 字符编码转换问题 (如 varchar vs nvarchar)\n" +
                      $"建议:\n" +
                      $"- 检查源表和目标表的列定义 (特别是 varchar/nvarchar 长度)\n" +
                      $"- 使用 SSMS 对比两个表的架构\n" +
                      $"- 如果目标表列长度不足，请先调整列定义\n" +
                      $"BCP 退出码: {result.ExitCode}"
                    : $"BCP 进程退出码 {result.ExitCode}";

                return new BcpCommandResult
                {
                    Success = false,
                    Output = result.Output,
                    ErrorMessage = errorMessage,
                    Duration = stopwatch.Elapsed
                };
            }

            // 解析导入的行数
            var rowCount = ParseRowCountFromOutput(result.Output);

            return new BcpCommandResult
            {
                Success = true,
                Output = result.Output,
                RowCount = rowCount,
                Duration = stopwatch.Elapsed
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "BCP 导入执行异常");

            return new BcpCommandResult
            {
                Success = false,
                ErrorMessage = ex.Message,
                Duration = stopwatch.Elapsed
            };
        }
    }

    /// <summary>
    /// 执行 BCP 命令
    /// </summary>
    private async Task<ProcessResult> ExecuteBcpCommandAsync(
        string bcpPath,
        string arguments,
        CancellationToken cancellationToken,
        Action<string>? outputCallback = null)
    {
        var outputBuilder = new StringBuilder();
        var errorBuilder = new StringBuilder();

        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = bcpPath,
                Arguments = arguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                // BCP 在中文 Windows 上通常输出 GBK 编码
                StandardOutputEncoding = Encoding.GetEncoding("GB2312"),
                StandardErrorEncoding = Encoding.GetEncoding("GB2312")
            }
        };

        process.OutputDataReceived += (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                outputBuilder.AppendLine(e.Data);
                outputCallback?.Invoke(e.Data);
            }
        };

        process.ErrorDataReceived += (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                errorBuilder.AppendLine(e.Data);
            }
        };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        await process.WaitForExitAsync(cancellationToken);

        var output = outputBuilder.ToString();
        var error = errorBuilder.ToString();

        return new ProcessResult
        {
            ExitCode = process.ExitCode,
            Output = string.IsNullOrWhiteSpace(error) ? output : $"{output}\n{error}"
        };
    }

    /// <summary>
    /// 构建 BCP 导出命令参数
    /// </summary>
    private string BuildExportArguments(
        string connectionString,
        string query,
        string outputFile,
        BcpOptions options)
    {
        var connParams = ParseConnectionString(connectionString);
        var args = new StringBuilder();

        // 查询语句(需要引号)
        args.Append($"\"{query}\" queryout \"{outputFile}\"");

        // 数据格式
        if (options.UseNativeFormat)
        {
            args.Append(" -n"); // 原生格式
        }
        else if (options.UseUnicode)
        {
            args.Append(" -w"); // Unicode 字符格式
        }
        else
        {
            args.Append(" -c"); // 字符格式
        }

        // 服务器和数据库
        args.Append($" -S \"{connParams.Server}\"");
        args.Append($" -d \"{connParams.Database}\"");

        // 身份验证
        if (connParams.UseIntegratedSecurity)
        {
            args.Append(" -T"); // 集成身份验证
        }
        else
        {
            args.Append($" -U \"{connParams.UserId}\"");
            args.Append($" -P \"{connParams.Password}\"");
        }

        // 其他选项
        args.Append($" -m {options.MaxErrors}"); // 最大错误数

        return args.ToString();
    }

    /// <summary>
    /// 构建 BCP 导入命令参数
    /// </summary>
    private string BuildImportArguments(
        string connectionString,
        string tableName,
        string inputFile,
        BcpOptions options)
    {
        var connParams = ParseConnectionString(connectionString);
        var args = new StringBuilder();

        // 表名和文件
        args.Append($"{tableName} in \"{inputFile}\"");

        // 数据格式
        if (options.UseNativeFormat)
        {
            args.Append(" -n");
        }
        else if (options.UseUnicode)
        {
            args.Append(" -w");
        }
        else
        {
            args.Append(" -c");
        }

        // 服务器和数据库
        args.Append($" -S \"{connParams.Server}\"");
        args.Append($" -d \"{connParams.Database}\"");

        // 身份验证
        if (connParams.UseIntegratedSecurity)
        {
            args.Append(" -T");
        }
        else
        {
            args.Append($" -U \"{connParams.UserId}\"");
            args.Append($" -P \"{connParams.Password}\"");
        }

        // 批次大小
        args.Append($" -b {options.BatchSize}");

        // 其他选项
        args.Append($" -m {options.MaxErrors}");
        
        // 增加网络数据包大小,避免字符串截断错误 (默认 4096 太小)
        // 设置为 32KB 以支持包含大字段的表
        args.Append(" -a 32768");

        return args.ToString();
    }

    /// <summary>
    /// 获取 BCP 工具路径
    /// </summary>
    private string GetBcpToolPath(BcpOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.BcpToolPath))
        {
            return options.BcpToolPath;
        }

        // 使用系统 PATH 中的 bcp.exe
        return "bcp";
    }

    /// <summary>
    /// 解析连接字符串
    /// </summary>
    private ConnectionParams ParseConnectionString(string connectionString)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);

        return new ConnectionParams
        {
            Server = builder.DataSource,
            Database = builder.InitialCatalog,
            UseIntegratedSecurity = builder.IntegratedSecurity,
            UserId = builder.UserID,
            Password = builder.Password
        };
    }

    /// <summary>
    /// 从 BCP 输出解析行数
    /// </summary>
    private long ParseRowCountFromOutput(string output)
    {
        if (string.IsNullOrWhiteSpace(output))
        {
            return 0;
        }

        // BCP 中文输出: "已复制 1000 行"
        var matchChinese = System.Text.RegularExpressions.Regex.Match(output, @"已复制\s+(\d+)\s+行", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (matchChinese.Success && long.TryParse(matchChinese.Groups[1].Value, out var rowCountChinese))
        {
            _logger.LogDebug("从 BCP 中文输出解析到行数: {RowCount}", rowCountChinese);
            return rowCountChinese;
        }

        // BCP 英文输出: "1000 rows copied."
        var matchEnglish = System.Text.RegularExpressions.Regex.Match(output, @"(\d+)\s+rows?\s+copied", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        if (matchEnglish.Success && long.TryParse(matchEnglish.Groups[1].Value, out var rowCountEnglish))
        {
            _logger.LogDebug("从 BCP 英文输出解析到行数: {RowCount}", rowCountEnglish);
            return rowCountEnglish;
        }

        // 备用：从最后一行提取数字（"Clock Time (ms.) Total: 1234"）
        var lines = output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var line in lines.Reverse())
        {
            // 尝试匹配任何包含大数字的行
            var matchNumber = System.Text.RegularExpressions.Regex.Match(line, @"(\d{1,})");
            if (matchNumber.Success && long.TryParse(matchNumber.Groups[1].Value, out var possibleCount) && possibleCount > 0)
            {
                _logger.LogDebug("从 BCP 输出备用解析到可能的行数: {RowCount} (行: {Line})", possibleCount, line.Trim());
                return possibleCount;
            }
        }

        _logger.LogWarning("无法从 BCP 输出解析行数，输出内容:\n{Output}", output);
        return 0;
    }

    /// <summary>
    /// 报告进度
    /// </summary>
    private void ReportProgress(
        string output,
        IProgress<BulkCopyProgress>? progress,
        long totalRows,
        DateTime startTimeUtc)
    {
        if (progress == null)
        {
            return;
        }

        // 尝试从输出解析当前行数
        var currentRows = ParseRowCountFromOutput(output);
        if (currentRows > 0)
        {
            var elapsed = DateTime.UtcNow - startTimeUtc;
            var throughput = elapsed.TotalSeconds > 0 ? currentRows / elapsed.TotalSeconds : 0;
            var percentComplete = totalRows > 0 ? Math.Min(100.0, (double)currentRows / totalRows * 100) : 0;

            progress.Report(new BulkCopyProgress
            {
                RowsCopied = currentRows,
                PercentComplete = percentComplete,
                StartTimeUtc = startTimeUtc,
                CurrentThroughput = throughput
            });
        }
    }

    /// <summary>
    /// 屏蔽密码用于日志记录
    /// </summary>
    private string MaskPassword(string arguments)
    {
        return System.Text.RegularExpressions.Regex.Replace(
            arguments,
            @"-P\s+""[^""]*""",
            "-P \"***\"",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    /// <summary>
    /// 连接参数
    /// </summary>
    private class ConnectionParams
    {
        public string Server { get; set; } = string.Empty;
        public string Database { get; set; } = string.Empty;
        public bool UseIntegratedSecurity { get; set; }
        public string UserId { get; set; } = string.Empty;
        public string Password { get; set; } = string.Empty;
    }

    /// <summary>
    /// BCP 命令执行结果
    /// </summary>
    private class BcpCommandResult
    {
        public bool Success { get; set; }
        public string Output { get; set; } = string.Empty;
        public string? ErrorMessage { get; set; }
        public long RowCount { get; set; }
        public TimeSpan Duration { get; set; }
    }

    /// <summary>
    /// 进程执行结果
    /// </summary>
    private class ProcessResult
    {
        public int ExitCode { get; set; }
        public string Output { get; set; } = string.Empty;
    }
}
