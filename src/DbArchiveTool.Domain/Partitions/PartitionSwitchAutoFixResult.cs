using System;
using System.Collections.Generic;
using System.Linq;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 自动补齐执行结果，包含各步骤的执行情况与汇总日志。
/// </summary>
public sealed class PartitionSwitchAutoFixResult
{
    public PartitionSwitchAutoFixResult(bool succeeded, IReadOnlyList<PartitionSwitchAutoFixExecution> executions, string combinedLog)
    {
        Succeeded = succeeded;
        Executions = executions ?? throw new ArgumentNullException(nameof(executions));
        CombinedLog = combinedLog ?? throw new ArgumentNullException(nameof(combinedLog));
    }

    /// <summary>是否全部步骤执行成功。</summary>
    public bool Succeeded { get; }

    /// <summary>各步骤执行明细。</summary>
    public IReadOnlyList<PartitionSwitchAutoFixExecution> Executions { get; }

    /// <summary>汇总日志，便于前端展示。</summary>
    public string CombinedLog { get; }

    /// <summary>创建成功结果。</summary>
    public static PartitionSwitchAutoFixResult Success(IReadOnlyList<PartitionSwitchAutoFixExecution> executions)
    {
        var log = string.Join(Environment.NewLine, executions.Select(ToLogLine));
        return new PartitionSwitchAutoFixResult(true, executions, log);
    }

    /// <summary>创建失败结果。</summary>
    public static PartitionSwitchAutoFixResult Failure(IReadOnlyList<PartitionSwitchAutoFixExecution> executions)
    {
        var log = string.Join(Environment.NewLine, executions.Select(ToLogLine));
        return new PartitionSwitchAutoFixResult(false, executions, log);
    }

    private static string ToLogLine(PartitionSwitchAutoFixExecution execution)
        => $"[{(execution.Succeeded ? "SUCCESS" : "FAILED")}] {execution.Code}: {execution.Message}";
}

/// <summary>
/// 单条自动补齐步骤的执行记录。
/// </summary>
public sealed record PartitionSwitchAutoFixExecution(
    string Code,
    bool Succeeded,
    string Message,
    string Script,
    long ElapsedMilliseconds);
