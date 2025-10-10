using System;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区执行任务的派发接口，由基础设施层实现。
/// </summary>
public interface IPartitionExecutionDispatcher
{
    Task DispatchAsync(Guid executionTaskId, Guid dataSourceId, CancellationToken cancellationToken = default);
}
