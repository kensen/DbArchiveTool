using DbArchiveTool.Domain.AdminUsers;
using DbArchiveTool.Domain.ArchiveConfigurations;
using DbArchiveTool.Domain.ArchiveTasks;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Partitions;
using DbArchiveTool.Infrastructure.Persistence.Models;
using Microsoft.EntityFrameworkCore;

namespace DbArchiveTool.Infrastructure.Persistence;

public sealed class ArchiveDbContext : DbContext
{
    public ArchiveDbContext(DbContextOptions<ArchiveDbContext> options) : base(options)
    {
    }

    public DbSet<ArchiveTask> ArchiveTasks => Set<ArchiveTask>();
    public DbSet<AdminUser> AdminUsers => Set<AdminUser>();
    public DbSet<ArchiveDataSource> ArchiveDataSources => Set<ArchiveDataSource>();
    public DbSet<ArchiveConfiguration> ArchiveConfigurations => Set<ArchiveConfiguration>();
    public DbSet<PartitionCommand> PartitionCommands => Set<PartitionCommand>();
    public DbSet<PartitionConfigurationEntity> PartitionConfigurations => Set<PartitionConfigurationEntity>();
    public DbSet<BackgroundTask> BackgroundTasks => Set<BackgroundTask>();
    public DbSet<BackgroundTaskLogEntry> BackgroundTaskLogs => Set<BackgroundTaskLogEntry>();
    public DbSet<PartitionAuditLog> PartitionAuditLogs => Set<PartitionAuditLog>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ArchiveTask>(builder =>
        {
            builder.ToTable("ArchiveTask");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.SourceTableName).HasMaxLength(128);
            builder.Property(x => x.TargetTableName).HasMaxLength(128);
            builder.Property(x => x.LegacyOperationRecordId).HasMaxLength(12);
        });

        modelBuilder.Entity<AdminUser>(builder =>
        {
            builder.ToTable("AdminUser");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.UserName).IsRequired().HasMaxLength(64);
            builder.HasIndex(x => x.UserName).IsUnique();
            builder.Property(x => x.PasswordHash).IsRequired().HasMaxLength(512);
        });

        modelBuilder.Entity<ArchiveDataSource>(builder =>
        {
            builder.ToTable("ArchiveDataSource");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.Name).IsRequired().HasMaxLength(100);
            builder.Property(x => x.Description).HasMaxLength(256);
            builder.Property(x => x.ServerAddress).IsRequired().HasMaxLength(128);
            builder.Property(x => x.DatabaseName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.UserName).HasMaxLength(64);
            builder.Property(x => x.Password).HasMaxLength(256);

            // 目标服务器配置字段
            builder.Property(x => x.UseSourceAsTarget).IsRequired().HasDefaultValue(true);
            builder.Property(x => x.TargetServerAddress).HasMaxLength(128);
            builder.Property(x => x.TargetDatabaseName).HasMaxLength(128);
            builder.Property(x => x.TargetUserName).HasMaxLength(64);
            builder.Property(x => x.TargetPassword).HasMaxLength(256);
        });

        modelBuilder.Entity<ArchiveConfiguration>(builder =>
        {
            builder.ToTable("ArchiveConfiguration");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.Name).IsRequired().HasMaxLength(100);
            builder.Property(x => x.Description).HasMaxLength(256);
            builder.Property(x => x.DataSourceId).IsRequired();
            builder.Property(x => x.SourceSchemaName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.SourceTableName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.TargetSchemaName).HasMaxLength(128);
            builder.Property(x => x.TargetTableName).HasMaxLength(128);
            builder.Property(x => x.IsPartitionedTable).IsRequired();
            builder.Property(x => x.PartitionConfigurationId);
            builder.Property(x => x.ArchiveFilterColumn).HasMaxLength(128);
            builder.Property(x => x.ArchiveFilterCondition).HasMaxLength(512);
            builder.Property(x => x.ArchiveMethod).IsRequired();
            builder.Property(x => x.DeleteSourceDataAfterArchive).IsRequired().HasDefaultValue(true);
            builder.Property(x => x.BatchSize).IsRequired().HasDefaultValue(10000);
            builder.Property(x => x.IsEnabled).IsRequired().HasDefaultValue(true);
            builder.Property(x => x.EnableScheduledArchive).IsRequired().HasDefaultValue(false);
            builder.Property(x => x.CronExpression).HasMaxLength(100);
            builder.Property(x => x.NextArchiveAtUtc);
            builder.Property(x => x.LastExecutionTimeUtc);
            builder.Property(x => x.LastExecutionStatus).HasMaxLength(50);
            builder.Property(x => x.LastArchivedRowCount);

            // 唯一索引:同一数据源下同一张表只能有一个归档配置
            builder.HasIndex(x => new { x.DataSourceId, x.SourceSchemaName, x.SourceTableName })
                .IsUnique()
                .HasFilter("[IsDeleted] = 0");

            // 调度索引:仅对启用的定时任务生效
            builder.HasIndex(x => x.NextArchiveAtUtc)
                .HasDatabaseName("IX_ArchiveConfiguration_NextArchive")
                .HasFilter("[IsEnabled] = 1 AND [EnableScheduledArchive] = 1");
        });

        modelBuilder.Entity<PartitionCommand>(builder =>
        {
            builder.ToTable("PartitionCommand");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.SchemaName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.TableName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.Payload).IsRequired();
            builder.Property(x => x.Script).IsRequired();
            builder.Property(x => x.RequestedBy).IsRequired().HasMaxLength(64);
            builder.Property(x => x.RequestedAt).HasConversion(
                v => v.UtcDateTime,
                v => DateTime.SpecifyKind(v, DateTimeKind.Utc));
        });

        modelBuilder.Entity<PartitionConfigurationEntity>(builder =>
        {
            builder.ToTable("PartitionConfiguration");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.SchemaName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.TableName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.PartitionFunctionName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.PartitionSchemeName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.PartitionColumnName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.PrimaryFilegroup).IsRequired().HasMaxLength(128);
            builder.Property(x => x.StorageFilegroupName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.StorageDataFileDirectory).HasMaxLength(260);
            builder.Property(x => x.StorageDataFileName).HasMaxLength(128);
            builder.Property(x => x.TargetDatabaseName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.TargetSchemaName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.TargetTableName).IsRequired().HasMaxLength(128);
            builder.Property(x => x.TargetRemarks).HasMaxLength(512);
            builder.Property(x => x.Remarks).HasMaxLength(512);
            builder.Property(x => x.IsCommitted).IsRequired().HasDefaultValue(false);
            builder.Property(x => x.ExecutionStage).HasMaxLength(50);
            builder.Property(x => x.LastExecutionTaskId);
            builder.Property(x => x.CreatedBy).IsRequired().HasMaxLength(64);
            builder.Property(x => x.UpdatedBy).IsRequired().HasMaxLength(64);

            // 唯一索引:只对未删除的记录生效
            builder.HasIndex(x => new { x.ArchiveDataSourceId, x.SchemaName, x.TableName })
                .IsUnique()
                .HasFilter("[IsDeleted] = 0");

            builder.HasMany(x => x.Boundaries)
                .WithOne(x => x.Configuration)
                .HasForeignKey(x => x.ConfigurationId)
                .OnDelete(DeleteBehavior.Cascade);

            builder.HasMany(x => x.AdditionalFilegroups)
                .WithOne(x => x.Configuration)
                .HasForeignKey(x => x.ConfigurationId)
                .OnDelete(DeleteBehavior.Cascade);

            builder.HasMany(x => x.FilegroupMappings)
                .WithOne(x => x.Configuration)
                .HasForeignKey(x => x.ConfigurationId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<PartitionConfigurationBoundaryEntity>(builder =>
        {
            builder.ToTable("PartitionConfigurationBoundary");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.SortKey).IsRequired().HasMaxLength(64);
            builder.Property(x => x.RawValue).IsRequired();
        });

        modelBuilder.Entity<PartitionConfigurationFilegroupEntity>(builder =>
        {
            builder.ToTable("PartitionConfigurationFilegroup");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.FilegroupName).IsRequired().HasMaxLength(128);
        });

        modelBuilder.Entity<PartitionConfigurationFilegroupMappingEntity>(builder =>
        {
            builder.ToTable("PartitionConfigurationFilegroupMapping");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.BoundaryKey).IsRequired().HasMaxLength(64);
            builder.Property(x => x.FilegroupName).IsRequired().HasMaxLength(128);
        });

        modelBuilder.Entity<BackgroundTask>(builder =>
        {
            builder.ToTable("BackgroundTask");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.PartitionConfigurationId).IsRequired(false); // 可空,用于 BCP/BulkCopy 等非分区归档场景
            builder.Property(x => x.DataSourceId).IsRequired();
            builder.Property(x => x.OperationType)
                .HasConversion<int>()
                .HasDefaultValue(BackgroundTaskOperationType.Unknown)
                .IsRequired();
            builder.Property(x => x.ArchiveScheme).HasMaxLength(128);
            builder.Property(x => x.ArchiveTargetConnection).HasMaxLength(512);
            builder.Property(x => x.ArchiveTargetDatabase).HasMaxLength(128);
            builder.Property(x => x.ArchiveTargetTable).HasMaxLength(256);
            builder.Property(x => x.Status).IsRequired();
            builder.Property(x => x.Phase).IsRequired().HasMaxLength(64);
            builder.Property(x => x.Progress).IsRequired();
            builder.Property(x => x.RequestedBy).IsRequired().HasMaxLength(64);
            builder.Property(x => x.BackupReference).HasMaxLength(256);
            builder.Property(x => x.Notes).HasMaxLength(512);
            builder.Property(x => x.FailureReason).HasMaxLength(512);
            builder.Property(x => x.SummaryJson).HasColumnType("nvarchar(max)");
            builder.Property(x => x.ConfigurationSnapshot).HasColumnType("nvarchar(max)");
            builder.Property(x => x.LastCheckpoint).HasColumnType("nvarchar(max)");
            builder.Property(x => x.Priority).HasDefaultValue(0);
            builder.Property(x => x.LastHeartbeatUtc).IsRequired();

            builder.HasIndex(x => new { x.DataSourceId, x.Status });
            builder.HasIndex(x => new { x.PartitionConfigurationId, x.IsDeleted })
                .HasFilter("[IsDeleted] = 0");
        });

        modelBuilder.Entity<BackgroundTaskLogEntry>(builder =>
        {
            builder.ToTable("BackgroundTaskLog");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.ExecutionTaskId).IsRequired();
            builder.Property(x => x.LogTimeUtc).IsRequired();
            builder.Property(x => x.Category).IsRequired().HasMaxLength(32);
            builder.Property(x => x.Title).IsRequired().HasMaxLength(256);
            builder.Property(x => x.Message).IsRequired().HasColumnType("nvarchar(max)");
            builder.Property(x => x.ExtraJson).HasColumnType("nvarchar(max)");

            builder.HasIndex(x => new { x.ExecutionTaskId, x.LogTimeUtc });
        });

        modelBuilder.Entity<PartitionAuditLog>(builder =>
        {
            builder.ToTable("PartitionAuditLog");
            builder.HasKey(x => x.Id);
            builder.Property(x => x.UserId).IsRequired().HasMaxLength(64);
            builder.Property(x => x.Action).IsRequired().HasMaxLength(100);
            builder.Property(x => x.ResourceType).IsRequired().HasMaxLength(100);
            builder.Property(x => x.ResourceId).IsRequired().HasMaxLength(64);
            builder.Property(x => x.OccurredAtUtc).IsRequired();
            builder.Property(x => x.Summary).HasMaxLength(512);
            builder.Property(x => x.PayloadJson).HasColumnType("nvarchar(max)");
            builder.Property(x => x.Result).IsRequired().HasMaxLength(32);
            builder.Property(x => x.Script).HasColumnType("nvarchar(max)");

            builder.HasIndex(x => new { x.ResourceType, x.ResourceId, x.OccurredAtUtc });
            builder.HasIndex(x => new { x.Action, x.OccurredAtUtc });
        });
    }
}
