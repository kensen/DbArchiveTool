using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.DataSources;

/// <summary>归档数据源应用服务接口。</summary>
public interface IArchiveDataSourceAppService
{
    /// <summary>获取已配置的数据源列表。</summary>
    Task<Result<IReadOnlyList<ArchiveDataSourceDto>>> GetAsync(CancellationToken cancellationToken = default);

    /// <summary>新增数据源。</summary>
    Task<Result<Guid>> CreateAsync(CreateArchiveDataSourceRequest request, CancellationToken cancellationToken = default);

    /// <summary>测试数据库连接是否成功。</summary>
    Task<Result<bool>> TestConnectionAsync(TestArchiveDataSourceRequest request, CancellationToken cancellationToken = default);
}
