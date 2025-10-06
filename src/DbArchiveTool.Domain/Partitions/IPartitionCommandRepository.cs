namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 定义对分区命令的持久化操作，负责插入、查询及状态更新。
/// </summary>
public interface IPartitionCommandRepository
{
    /// <summary>新增分区命令。</summary>
    Task AddAsync(PartitionCommand command, CancellationToken cancellationToken = default);

    /// <summary>根据标识获取命令。</summary>
    Task<PartitionCommand?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);

    /// <summary>更新命令状态或内容。</summary>
    Task UpdateAsync(PartitionCommand command, CancellationToken cancellationToken = default);

    /// <summary>列出所有待执行的命令。</summary>
    Task<IReadOnlyList<PartitionCommand>> ListPendingAsync(CancellationToken cancellationToken = default);
}
