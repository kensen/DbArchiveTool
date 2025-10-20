using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class RenameToBackgroundTasksAndAddAuditLog : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            // 步骤 1: 重命名表
            migrationBuilder.RenameTable(
                name: "PartitionExecutionTask",
                newName: "BackgroundTask");

            migrationBuilder.RenameTable(
                name: "PartitionExecutionLog",
                newName: "BackgroundTaskLog");

            // 步骤 2: 重命名索引
            migrationBuilder.RenameIndex(
                name: "IX_PartitionExecutionTask_PartitionConfigurationId_IsDeleted",
                table: "BackgroundTask",
                newName: "IX_BackgroundTask_PartitionConfigurationId_IsDeleted");

            migrationBuilder.RenameIndex(
                name: "IX_PartitionExecutionTask_DataSourceId_Status",
                table: "BackgroundTask",
                newName: "IX_BackgroundTask_DataSourceId_Status");

            migrationBuilder.RenameIndex(
                name: "IX_PartitionExecutionLog_ExecutionTaskId_LogTimeUtc",
                table: "BackgroundTaskLog",
                newName: "IX_BackgroundTaskLog_ExecutionTaskId_LogTimeUtc");

            // 步骤 3: 更新主键约束名称
            migrationBuilder.DropPrimaryKey(
                name: "PK_PartitionExecutionTask",
                table: "BackgroundTask");

            migrationBuilder.DropPrimaryKey(
                name: "PK_PartitionExecutionLog",
                table: "BackgroundTaskLog");

            migrationBuilder.AddPrimaryKey(
                name: "PK_BackgroundTask",
                table: "BackgroundTask",
                column: "Id");

            migrationBuilder.AddPrimaryKey(
                name: "PK_BackgroundTaskLog",
                table: "BackgroundTaskLog",
                column: "Id");

            // 步骤 4: 创建审计日志表(如果不存在)
            migrationBuilder.Sql(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PartitionAuditLog')
                BEGIN
                    CREATE TABLE [PartitionAuditLog] (
                        [Id] uniqueidentifier NOT NULL,
                        [UserId] nvarchar(64) NOT NULL,
                        [Action] nvarchar(100) NOT NULL,
                        [ResourceType] nvarchar(100) NOT NULL,
                        [ResourceId] nvarchar(64) NOT NULL,
                        [OccurredAtUtc] datetime2 NOT NULL,
                        [Summary] nvarchar(512) NULL,
                        [PayloadJson] nvarchar(max) NULL,
                        [Result] nvarchar(32) NOT NULL,
                        [Script] nvarchar(max) NULL,
                        [CreatedAtUtc] datetime2 NOT NULL,
                        [CreatedBy] nvarchar(max) NOT NULL,
                        [UpdatedAtUtc] datetime2 NOT NULL,
                        [UpdatedBy] nvarchar(max) NOT NULL,
                        [IsDeleted] bit NOT NULL,
                        CONSTRAINT [PK_PartitionAuditLog] PRIMARY KEY ([Id])
                    );
                    
                    CREATE INDEX [IX_PartitionAuditLog_Action_OccurredAtUtc] 
                        ON [PartitionAuditLog] ([Action], [OccurredAtUtc]);
                    
                    CREATE INDEX [IX_PartitionAuditLog_ResourceType_ResourceId_OccurredAtUtc] 
                        ON [PartitionAuditLog] ([ResourceType], [ResourceId], [OccurredAtUtc]);
                END
            ");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            // 步骤 1: 删除审计日志表
            migrationBuilder.DropTable(
                name: "PartitionAuditLog");

            // 步骤 2: 还原主键约束名称
            migrationBuilder.DropPrimaryKey(
                name: "PK_BackgroundTaskLog",
                table: "BackgroundTaskLog");

            migrationBuilder.DropPrimaryKey(
                name: "PK_BackgroundTask",
                table: "BackgroundTask");

            migrationBuilder.AddPrimaryKey(
                name: "PK_PartitionExecutionTask",
                table: "BackgroundTask",
                column: "Id");

            migrationBuilder.AddPrimaryKey(
                name: "PK_PartitionExecutionLog",
                table: "BackgroundTaskLog",
                column: "Id");

            // 步骤 3: 还原索引名称
            migrationBuilder.RenameIndex(
                name: "IX_BackgroundTask_PartitionConfigurationId_IsDeleted",
                table: "BackgroundTask",
                newName: "IX_PartitionExecutionTask_PartitionConfigurationId_IsDeleted");

            migrationBuilder.RenameIndex(
                name: "IX_BackgroundTask_DataSourceId_Status",
                table: "BackgroundTask",
                newName: "IX_PartitionExecutionTask_DataSourceId_Status");

            migrationBuilder.RenameIndex(
                name: "IX_BackgroundTaskLog_ExecutionTaskId_LogTimeUtc",
                table: "BackgroundTaskLog",
                newName: "IX_PartitionExecutionLog_ExecutionTaskId_LogTimeUtc");

            // 步骤 4: 还原表名
            migrationBuilder.RenameTable(
                name: "BackgroundTask",
                newName: "PartitionExecutionTask");

            migrationBuilder.RenameTable(
                name: "BackgroundTaskLog",
                newName: "PartitionExecutionLog");
        }
    }
}
