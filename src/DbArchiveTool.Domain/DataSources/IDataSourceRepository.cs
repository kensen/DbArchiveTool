using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.DataSources;

/// <summary>归档数据源仓储接口。</summary>
public interface IDataSourceRepository
{
    /// <summary>添加数据源。</summary>
    Task AddAsync(ArchiveDataSource dataSource, CancellationToken cancellationToken = default);

    /// <summary>按主键获取数据源。</summary>
    Task<ArchiveDataSource?> GetAsync(Guid id, CancellationToken cancellationToken = default);

    /// <summary>列出所有未删除的数据源。</summary>
    Task<IReadOnlyList<ArchiveDataSource>> ListAsync(CancellationToken cancellationToken = default);

    /// <summary>持久化当前上下文中的变更。</summary>
    Task SaveChangesAsync(CancellationToken cancellationToken = default);
}
