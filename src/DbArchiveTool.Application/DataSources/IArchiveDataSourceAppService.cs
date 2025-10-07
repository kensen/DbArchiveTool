using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.DataSources;

/// <summary>归档数据源应用服务接口。</summary>
public interface IArchiveDataSourceAppService
{
    /// <summary>获取当前启用的数据源列表。</summary>
    Task<Result<IReadOnlyList<ArchiveDataSourceDto>>> GetAsync(CancellationToken cancellationToken = default);

    /// <summary>根据ID获取单个数据源。</summary>
    Task<Result<ArchiveDataSourceDto>> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);

    /// <summary>创建归档数据源。</summary>
    Task<Result<Guid>> CreateAsync(CreateArchiveDataSourceRequest request, CancellationToken cancellationToken = default);

    /// <summary>更新归档数据源。</summary>
    Task<Result<bool>> UpdateAsync(UpdateArchiveDataSourceRequest request, CancellationToken cancellationToken = default);

    /// <summary>测试数据库连接是否成功。</summary>
    Task<Result<bool>> TestConnectionAsync(TestArchiveDataSourceRequest request, CancellationToken cancellationToken = default);
}
