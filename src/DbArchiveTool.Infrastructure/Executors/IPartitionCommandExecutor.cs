using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 定义分区命令执行器接口，根据命令类型执行对应的 SQL 操作。
/// </summary>
internal interface IPartitionCommandExecutor
{
    /// <summary>该执行器支持的命令类型。</summary>
    PartitionCommandType CommandType { get; }

    /// <summary>执行命令并返回 SQL 执行结果。</summary>
    Task<SqlExecutionResult> ExecuteAsync(PartitionCommand command, CancellationToken cancellationToken);
}
