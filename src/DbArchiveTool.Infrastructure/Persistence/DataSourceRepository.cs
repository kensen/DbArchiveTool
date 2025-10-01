using DbArchiveTool.Domain.DataSources;
using Microsoft.EntityFrameworkCore;
using System.Linq;

namespace DbArchiveTool.Infrastructure.Persistence;

/// <summary>归档数据源仓储实现。</summary>
internal sealed class DataSourceRepository : IDataSourceRepository
{
    private readonly ArchiveDbContext _context;

    public DataSourceRepository(ArchiveDbContext context)
    {
        _context = context;
    }

    public async Task AddAsync(ArchiveDataSource dataSource, CancellationToken cancellationToken = default)
    {
        await _context.ArchiveDataSources.AddAsync(dataSource, cancellationToken);
    }

    public async Task<ArchiveDataSource?> GetAsync(Guid id, CancellationToken cancellationToken = default)
    {
        return await _context.ArchiveDataSources.FirstOrDefaultAsync(x => x.Id == id && !x.IsDeleted, cancellationToken);
    }

    public async Task<IReadOnlyList<ArchiveDataSource>> ListAsync(CancellationToken cancellationToken = default)
    {
        return await _context.ArchiveDataSources
            .Where(x => !x.IsDeleted)
            .OrderBy(x => x.CreatedAtUtc)
            .ToListAsync(cancellationToken);
    }

    public Task SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        return _context.SaveChangesAsync(cancellationToken);
    }
}
