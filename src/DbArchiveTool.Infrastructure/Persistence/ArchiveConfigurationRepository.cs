using DbArchiveTool.Domain.ArchiveConfigurations;
using Microsoft.EntityFrameworkCore;

namespace DbArchiveTool.Infrastructure.Persistence;

/// <summary>
/// 归档配置仓储实现
/// </summary>
internal sealed class ArchiveConfigurationRepository : IArchiveConfigurationRepository
{
    private readonly ArchiveDbContext _context;

    public ArchiveConfigurationRepository(ArchiveDbContext context)
    {
        _context = context;
    }

    public async Task<ArchiveConfiguration?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        return await _context.ArchiveConfigurations
            .FirstOrDefaultAsync(x => x.Id == id && !x.IsDeleted, cancellationToken);
    }

    public async Task<List<ArchiveConfiguration>> GetAllAsync(CancellationToken cancellationToken = default)
    {
        return await _context.ArchiveConfigurations
            .Where(x => !x.IsDeleted)
            .OrderBy(x => x.Name)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<ArchiveConfiguration>> GetByDataSourceIdAsync(
        Guid dataSourceId,
        CancellationToken cancellationToken = default)
    {
        return await _context.ArchiveConfigurations
            .Where(x => x.DataSourceId == dataSourceId && !x.IsDeleted)
            .OrderBy(x => x.Name)
            .ToListAsync(cancellationToken);
    }

    public async Task<List<ArchiveConfiguration>> GetByPartitionConfigurationIdAsync(
        Guid partitionConfigurationId,
        CancellationToken cancellationToken = default)
    {
        return await _context.ArchiveConfigurations
            .Where(x => x.PartitionConfigurationId == partitionConfigurationId && !x.IsDeleted)
            .OrderBy(x => x.Name)
            .ToListAsync(cancellationToken);
    }

    public async Task AddAsync(ArchiveConfiguration configuration, CancellationToken cancellationToken = default)
    {
        await _context.ArchiveConfigurations.AddAsync(configuration, cancellationToken);
        await _context.SaveChangesAsync(cancellationToken);
    }

    public async Task UpdateAsync(ArchiveConfiguration configuration, CancellationToken cancellationToken = default)
    {
        _context.ArchiveConfigurations.Update(configuration);
        await _context.SaveChangesAsync(cancellationToken);
    }

    public async Task DeleteAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var configuration = await GetByIdAsync(id, cancellationToken);
        if (configuration != null)
        {
            configuration.MarkDeleted("SYSTEM");
            await _context.SaveChangesAsync(cancellationToken);
        }
    }

    public async Task<bool> ExistsForTableAsync(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        return await _context.ArchiveConfigurations
            .AnyAsync(
                x => x.DataSourceId == dataSourceId
                    && x.SourceSchemaName == schemaName
                    && x.SourceTableName == tableName
                    && !x.IsDeleted,
                cancellationToken);
    }
}
