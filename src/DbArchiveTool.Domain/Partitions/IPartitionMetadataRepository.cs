namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 定义面向 SQL Server 分区元数据的读取接口。
/// </summary>
public interface IPartitionMetadataRepository
{
    /// <summary>获取指定表的分区配置。</summary>
    Task<PartitionConfiguration?> GetConfigurationAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default);

    /// <summary>列出指定表的所有分区边界。</summary>
    Task<IReadOnlyList<PartitionBoundary>> ListBoundariesAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default);

    /// <summary>获取指定边界当前的安全快照。</summary>
    Task<PartitionSafetySnapshot> GetSafetySnapshotAsync(Guid dataSourceId, string schemaName, string tableName, string boundaryKey, CancellationToken cancellationToken = default);
}
