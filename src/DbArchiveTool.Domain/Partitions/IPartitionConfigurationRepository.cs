using System;
using System.Threading;
using System.Threading.Tasks;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 定义分区配置的持久化契约。
/// </summary>
public interface IPartitionConfigurationRepository
{
    /// <summary>根据标识获取分区配置。</summary>
    Task<PartitionConfiguration?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);

    /// <summary>根据数据源与表定位分区配置。</summary>
    Task<PartitionConfiguration?> GetByTableAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default);

    /// <summary>新增分区配置。</summary>
    Task AddAsync(PartitionConfiguration configuration, CancellationToken cancellationToken = default);

    /// <summary>更新分区配置。</summary>
    Task UpdateAsync(PartitionConfiguration configuration, CancellationToken cancellationToken = default);
}
