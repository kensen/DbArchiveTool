using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 表示一次待处理的分区操作命令，记录拆分、合并或切换分区的请求与执行信息。
/// </summary>
public sealed class PartitionCommand : AggregateRoot
{
    /// <summary>
    /// 关联的数据源标识。
    /// </summary>
    public Guid DataSourceId { get; private set; }

    /// <summary>
    /// 目标架构名称。
    /// </summary>
    public string SchemaName { get; private set; } = string.Empty;

    /// <summary>
    /// 目标表名称。
    /// </summary>
    public string TableName { get; private set; } = string.Empty;

    /// <summary>
    /// 分区命令类型。
    /// </summary>
    public PartitionCommandType CommandType { get; private set; }

    /// <summary>
    /// 当前命令状态。
    /// </summary>
    public PartitionCommandStatus Status { get; private set; } = PartitionCommandStatus.PendingApproval;

    /// <summary>
    /// 生成的 SQL 脚本内容。
    /// </summary>
    public string Script { get; private set; } = string.Empty;

    /// <summary>
    /// JSON 负载，描述命令的业务参数。
    /// </summary>
    public string Payload { get; private set; } = string.Empty;

    /// <summary>
    /// 提交命令的用户。
    /// </summary>
    public string RequestedBy { get; private set; } = string.Empty;

    /// <summary>
    /// 提交时间（UTC）。
    /// </summary>
    public DateTimeOffset RequestedAt { get; private set; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// 执行时间（UTC）。
    /// </summary>
    public DateTimeOffset? ExecutedAt { get; private set; }

    /// <summary>
    /// 完成时间（UTC）。
    /// </summary>
    public DateTimeOffset? CompletedAt { get; private set; }

    /// <summary>
    /// 失败原因记录。
    /// </summary>
    public string? FailureReason { get; private set; }

    private PartitionCommand()
    {
    }

    /// <summary>
    /// 创建拆分命令记录。
    /// </summary>
    public static PartitionCommand CreateSplit(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        string script,
        string payload,
        string requestedBy,
        DateTimeOffset? requestedAt = null)
    {
        var command = new PartitionCommand
        {
            Id = Guid.NewGuid(),
            DataSourceId = EnsureGuid(dataSourceId, nameof(dataSourceId)),
            SchemaName = EnsureNotEmpty(schemaName, nameof(schemaName)),
            TableName = EnsureNotEmpty(tableName, nameof(tableName)),
            CommandType = PartitionCommandType.Split,
            Script = EnsureNotEmpty(script, nameof(script)),
            Payload = EnsureNotEmpty(payload, nameof(payload)),
            RequestedBy = EnsureNotEmpty(requestedBy, nameof(requestedBy)),
            RequestedAt = requestedAt ?? DateTimeOffset.UtcNow
        };

        return command;
    }

    /// <summary>
    /// 审批通过，进入待执行阶段。
    /// </summary>
    public void Approve(string approver)
    {
        if (Status != PartitionCommandStatus.PendingApproval)
        {
            throw new InvalidOperationException("仅待审批的命令可以审批通过。");
        }

        Status = PartitionCommandStatus.Approved;
        Touch(EnsureNotEmpty(approver, nameof(approver)));
    }

    /// <summary>
    /// 拒绝命令执行并记录原因。
    /// </summary>
    public void Reject(string approver, string reason)
    {
        if (Status != PartitionCommandStatus.PendingApproval)
        {
            throw new InvalidOperationException("仅待审批的命令可以被拒绝。");
        }

        FailureReason = EnsureNotEmpty(reason, nameof(reason));
        Status = PartitionCommandStatus.Rejected;
        CompletedAt = DateTimeOffset.UtcNow;
        Touch(EnsureNotEmpty(approver, nameof(approver)));
    }

    /// <summary>
    /// 标记命令进入执行阶段。
    /// </summary>
    public void MarkExecuting(string executor)
    {
        if (Status != PartitionCommandStatus.Approved)
        {
            throw new InvalidOperationException("仅已审批的命令可以进入执行阶段。");
        }

        Status = PartitionCommandStatus.Executing;
        ExecutedAt = DateTimeOffset.UtcNow;
        Touch(EnsureNotEmpty(executor, nameof(executor)));
    }

    /// <summary>
    /// 标记命令执行成功。
    /// </summary>
    public void MarkSucceeded(string executor)
    {
        if (Status != PartitionCommandStatus.Executing)
        {
            throw new InvalidOperationException("仅执行中的命令可以标记为成功。");
        }

        Status = PartitionCommandStatus.Succeeded;
        CompletedAt = DateTimeOffset.UtcNow;
        Touch(EnsureNotEmpty(executor, nameof(executor)));
    }

    /// <summary>
    /// 标记命令执行失败。
    /// </summary>
    public void MarkFailed(string executor, string reason)
    {
        if (Status != PartitionCommandStatus.Executing)
        {
            throw new InvalidOperationException("仅执行中的命令可以标记为失败。");
        }

        FailureReason = EnsureNotEmpty(reason, nameof(reason));
        Status = PartitionCommandStatus.Failed;
        CompletedAt = DateTimeOffset.UtcNow;
        Touch(EnsureNotEmpty(executor, nameof(executor)));
    }

    private static Guid EnsureGuid(Guid value, string field)
    {
        if (value == Guid.Empty)
        {
            throw new ArgumentException("标识不能为空。", field);
        }

        return value;
    }

    private static string EnsureNotEmpty(string value, string field)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("字段不能为空。", field);
        }

        return value.Trim();
    }
}
