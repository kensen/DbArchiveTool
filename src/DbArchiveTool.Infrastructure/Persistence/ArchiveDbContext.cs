using DbArchiveTool.Domain.AdminUsers;
using DbArchiveTool.Domain.ArchiveTasks;
using Microsoft.EntityFrameworkCore;

namespace DbArchiveTool.Infrastructure.Persistence;

public sealed class ArchiveDbContext : DbContext
{
    public ArchiveDbContext(DbContextOptions<ArchiveDbContext> options) : base(options)
    {
    }

    public DbSet<ArchiveTask> ArchiveTasks => Set<ArchiveTask>();
    public DbSet<AdminUser> AdminUsers => Set<AdminUser>();

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
    }
}
