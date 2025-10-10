using System;
using System.Collections.Generic;
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

    /// <summary>根据数据源获取所有分区配置。</summary>
    Task<List<PartitionConfiguration>> GetByDataSourceAsync(Guid dataSourceId, CancellationToken cancellationToken = default);

    /// <summary>新增分区配置。</summary>
    Task AddAsync(PartitionConfiguration configuration, CancellationToken cancellationToken = default);

    /// <summary>更新分区配置。</summary>
    Task UpdateAsync(PartitionConfiguration configuration, CancellationToken cancellationToken = default);

    /// <summary>删除分区配置。</summary>
    Task DeleteAsync(Guid id, CancellationToken cancellationToken = default);
}
