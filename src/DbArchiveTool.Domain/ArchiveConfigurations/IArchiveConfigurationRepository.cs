namespace DbArchiveTool.Domain.ArchiveConfigurations;

/// <summary>
/// 归档配置仓储接口
/// </summary>
public interface IArchiveConfigurationRepository
{
    /// <summary>根据ID获取归档配置</summary>
    Task<ArchiveConfiguration?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);

    /// <summary>获取所有归档配置</summary>
    Task<List<ArchiveConfiguration>> GetAllAsync(CancellationToken cancellationToken = default);

    /// <summary>根据数据源ID获取归档配置</summary>
    Task<List<ArchiveConfiguration>> GetByDataSourceIdAsync(Guid dataSourceId, CancellationToken cancellationToken = default);

    /// <summary>根据分区配置ID获取归档配置</summary>
    Task<List<ArchiveConfiguration>> GetByPartitionConfigurationIdAsync(Guid partitionConfigurationId, CancellationToken cancellationToken = default);

    /// <summary>添加归档配置</summary>
    Task AddAsync(ArchiveConfiguration configuration, CancellationToken cancellationToken = default);

    /// <summary>更新归档配置</summary>
    Task UpdateAsync(ArchiveConfiguration configuration, CancellationToken cancellationToken = default);

    /// <summary>删除归档配置</summary>
    Task DeleteAsync(Guid id, CancellationToken cancellationToken = default);

    /// <summary>检查表是否已配置归档</summary>
    Task<bool> ExistsForTableAsync(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default);
}
