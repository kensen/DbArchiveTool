using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.Persistence.Models;
using Microsoft.EntityFrameworkCore;

namespace DbArchiveTool.Infrastructure.Persistence;

/// <summary>
/// EF Core 对 <see cref="PartitionConfiguration"/> 的仓储实现。
/// </summary>
internal sealed class PartitionConfigurationRepository : IPartitionConfigurationRepository
{
    private readonly ArchiveDbContext context;

    public PartitionConfigurationRepository(ArchiveDbContext context)
    {
        this.context = context;
    }

    public async Task<PartitionConfiguration?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var entity = await Query()
            .FirstOrDefaultAsync(x => x.Id == id && !x.IsDeleted, cancellationToken);

        return entity is null ? null : MapToDomain(entity);
    }

    public async Task<PartitionConfiguration?> GetByTableAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default)
    {
        var entity = await Query()
            .FirstOrDefaultAsync(
                x => x.ArchiveDataSourceId == dataSourceId &&
                     x.SchemaName == schemaName &&
                     x.TableName == tableName &&
                     !x.IsDeleted,
                cancellationToken);

        return entity is null ? null : MapToDomain(entity);
    }

    public async Task<List<PartitionConfiguration>> GetByDataSourceAsync(Guid dataSourceId, CancellationToken cancellationToken = default)
    {
        var entities = await Query()
            .Where(x => x.ArchiveDataSourceId == dataSourceId && !x.IsDeleted)
            .ToListAsync(cancellationToken);

        return entities.Select(MapToDomain).ToList();
    }

    public async Task AddAsync(PartitionConfiguration configuration, CancellationToken cancellationToken = default)
    {
        var entity = MapToEntity(configuration);
        await context.PartitionConfigurations.AddAsync(entity, cancellationToken);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async Task UpdateAsync(PartitionConfiguration configuration, CancellationToken cancellationToken = default)
    {
        var executionStrategy = context.Database.CreateExecutionStrategy();

        await executionStrategy.ExecuteAsync(async () =>
        {
            await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);

            var affected = await context.PartitionConfigurations
                .Where(x => x.Id == configuration.Id)
                .ExecuteDeleteAsync(cancellationToken);

            if (affected == 0)
            {
                await transaction.RollbackAsync(cancellationToken);
                throw new InvalidOperationException("目标分区配置不存在，无法更新。");
            }

            context.ChangeTracker.Clear();

            var entity = MapToEntity(configuration);
            await context.PartitionConfigurations.AddAsync(entity, cancellationToken);
            await context.SaveChangesAsync(cancellationToken);

            context.ChangeTracker.Clear();

            await transaction.CommitAsync(cancellationToken);
        });
    }

    public async Task DeleteAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var entity = await context.PartitionConfigurations
            .Include(x => x.Boundaries)
            .Include(x => x.AdditionalFilegroups)
            .Include(x => x.FilegroupMappings)
            .FirstOrDefaultAsync(x => x.Id == id, cancellationToken);

        if (entity is null)
        {
            throw new InvalidOperationException("目标分区配置不存在，无法删除。");
        }

        // 硬删除:物理删除配置及其所有关联数据
        // EF Core 会自动级联删除关联的 Boundaries, AdditionalFilegroups, FilegroupMappings
        context.PartitionConfigurations.Remove(entity);
        await context.SaveChangesAsync(cancellationToken);
    }

    private IQueryable<PartitionConfigurationEntity> Query()
    {
        return context.PartitionConfigurations
            .AsNoTracking()
            .Include(x => x.Boundaries)
            .Include(x => x.AdditionalFilegroups)
            .Include(x => x.FilegroupMappings);
    }

    private static PartitionConfigurationEntity MapToEntity(PartitionConfiguration configuration)
    {
        var entity = new PartitionConfigurationEntity
        {
            Id = configuration.Id,
            ArchiveDataSourceId = configuration.ArchiveDataSourceId,
            SchemaName = configuration.SchemaName,
            TableName = configuration.TableName,
            PartitionFunctionName = configuration.PartitionFunctionName,
            PartitionSchemeName = configuration.PartitionSchemeName,
            PartitionColumnName = configuration.PartitionColumn.Name,
            PartitionColumnKind = (int)configuration.PartitionColumn.ValueKind,
            PartitionColumnIsNullable = configuration.PartitionColumn.IsNullable,
            PrimaryFilegroup = configuration.FilegroupStrategy.PrimaryFilegroup,
            IsRangeRight = configuration.IsRangeRight,
            RequirePartitionColumnNotNull = configuration.RequirePartitionColumnNotNull,
            Remarks = configuration.Remarks,
            StorageMode = (int)configuration.StorageSettings.Mode,
            StorageFilegroupName = configuration.StorageSettings.FilegroupName,
            StorageDataFileDirectory = configuration.StorageSettings.DataFileDirectory,
            StorageDataFileName = configuration.StorageSettings.DataFileName,
            StorageInitialSizeMb = configuration.StorageSettings.InitialSizeMb,
            StorageAutoGrowthMb = configuration.StorageSettings.AutoGrowthMb,
            TargetDatabaseName = configuration.TargetTable?.DatabaseName ?? configuration.SchemaName,
            TargetSchemaName = configuration.TargetTable?.SchemaName ?? configuration.SchemaName,
            TargetTableName = configuration.TargetTable?.TableName ?? configuration.TableName,
            TargetRemarks = configuration.TargetTable?.Remarks ?? configuration.Remarks,
            IsCommitted = configuration.IsCommitted,
            CreatedAtUtc = configuration.CreatedAtUtc,
            CreatedBy = configuration.CreatedBy,
            UpdatedAtUtc = configuration.UpdatedAtUtc,
            UpdatedBy = configuration.UpdatedBy,
            IsDeleted = configuration.IsDeleted
        };

        if (configuration.Boundaries.Count > 0)
        {
            foreach (var boundary in configuration.Boundaries)
            {
                entity.Boundaries.Add(new PartitionConfigurationBoundaryEntity
                {
                    Id = Guid.NewGuid(),
                    ConfigurationId = configuration.Id,
                    SortKey = boundary.SortKey,
                    ValueKind = (int)boundary.Value.Kind,
                    RawValue = boundary.Value.ToInvariantString()
                });
            }
        }

        if (configuration.FilegroupStrategy.AdditionalFilegroups.Count > 0)
        {
            var index = 0;
            foreach (var filegroup in configuration.FilegroupStrategy.AdditionalFilegroups)
            {
                entity.AdditionalFilegroups.Add(new PartitionConfigurationFilegroupEntity
                {
                    Id = Guid.NewGuid(),
                    ConfigurationId = configuration.Id,
                    FilegroupName = filegroup,
                    SortOrder = index++
                });
            }
        }

        if (configuration.FilegroupMappings.Count > 0)
        {
            foreach (var mapping in configuration.FilegroupMappings)
            {
                entity.FilegroupMappings.Add(new PartitionConfigurationFilegroupMappingEntity
                {
                    Id = Guid.NewGuid(),
                    ConfigurationId = configuration.Id,
                    BoundaryKey = mapping.BoundaryKey,
                    FilegroupName = mapping.FilegroupName
                });
            }
        }

        return entity;
    }

    private static PartitionConfiguration MapToDomain(PartitionConfigurationEntity entity)
    {
        var partitionColumn = new PartitionColumn(
            entity.PartitionColumnName,
            (PartitionValueKind)entity.PartitionColumnKind,
            entity.PartitionColumnIsNullable);

        var filegroupStrategy = PartitionFilegroupStrategy.Default(entity.PrimaryFilegroup);
        foreach (var filegroup in entity.AdditionalFilegroups.OrderBy(x => x.SortOrder))
        {
            filegroupStrategy.AddFilegroup(filegroup.FilegroupName);
        }

        var boundaries = entity.Boundaries
            .OrderBy(x => x.SortKey, StringComparer.Ordinal)
            .Select(x =>
            {
                var value = PartitionValue.FromInvariantString((PartitionValueKind)x.ValueKind, x.RawValue);
                return new PartitionBoundary(x.SortKey, value);
            })
            .ToList();

        var mappings = entity.FilegroupMappings
            .Select(x => PartitionFilegroupMapping.Create(x.BoundaryKey, x.FilegroupName))
            .ToList();

        var storageSettings = entity.StorageMode == (int)PartitionStorageMode.DedicatedFilegroupSingleFile
            ? PartitionStorageSettings.CreateDedicated(
                entity.StorageFilegroupName,
                entity.StorageDataFileDirectory ?? throw new InvalidOperationException("缺少数据文件目录。"),
                entity.StorageDataFileName ?? throw new InvalidOperationException("缺少数据文件名称。"),
                entity.StorageInitialSizeMb ?? throw new InvalidOperationException("缺少数据文件初始大小。"),
                entity.StorageAutoGrowthMb ?? throw new InvalidOperationException("缺少数据文件自动增长设置。"))
            : PartitionStorageSettings.UsePrimary(entity.StorageFilegroupName);

        PartitionTargetTable? targetTable = null;
        if (!string.IsNullOrWhiteSpace(entity.TargetDatabaseName) &&
            !string.IsNullOrWhiteSpace(entity.TargetSchemaName) &&
            !string.IsNullOrWhiteSpace(entity.TargetTableName))
        {
            targetTable = PartitionTargetTable.Create(
                entity.TargetDatabaseName,
                entity.TargetSchemaName,
                entity.TargetTableName,
                entity.TargetRemarks);
        }

        var configuration = new PartitionConfiguration(
            entity.ArchiveDataSourceId,
            entity.SchemaName,
            entity.TableName,
            entity.PartitionFunctionName,
            entity.PartitionSchemeName,
            partitionColumn,
            filegroupStrategy,
            entity.IsRangeRight,
            existingBoundaries: boundaries,
            existingFilegroupMappings: mappings,
            storageSettings: storageSettings,
            targetTable: targetTable,
            requirePartitionColumnNotNull: entity.RequirePartitionColumnNotNull,
            remarks: entity.Remarks,
            isCommitted: entity.IsCommitted);

        configuration.OverrideId(entity.Id);
        configuration.RestoreAudit(entity.CreatedAtUtc, entity.CreatedBy, entity.UpdatedAtUtc, entity.UpdatedBy, entity.IsDeleted);
        return configuration;
    }
}

