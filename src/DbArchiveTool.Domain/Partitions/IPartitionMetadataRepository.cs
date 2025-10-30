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

    /// <summary>列出分区边界到文件组的映射关系。</summary>
    Task<IReadOnlyList<PartitionFilegroupMapping>> ListFilegroupMappingsAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default);

    /// <summary>获取指定表的安全规则。</summary>
    Task<PartitionSafetyRule?> GetSafetyRuleAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default);

    /// <summary>获取指定边界当前的安全快照。</summary>
    Task<PartitionSafetySnapshot> GetSafetySnapshotAsync(Guid dataSourceId, string schemaName, string tableName, string boundaryKey, CancellationToken cancellationToken = default);

        /// <summary>获取所有分区的行数统计。</summary>
        Task<IReadOnlyList<PartitionRowStatistics>> GetPartitionRowStatisticsAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// 检查目标表的索引/约束与分区列的对齐状态。
    /// </summary>
    Task<PartitionIndexInspection> GetIndexInspectionAsync(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        string partitionColumn,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取目标表的行数与空间占用统计信息。
    /// </summary>
    Task<TableStatistics> GetTableStatisticsAsync(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default);
}
