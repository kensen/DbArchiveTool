using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddPartitionExecutionTables : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "BackgroundTaskLog",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    ExecutionTaskId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    LogTimeUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Category = table.Column<string>(type: "nvarchar(32)", maxLength: 32, nullable: false),
                    Title = table.Column<string>(type: "nvarchar(256)", maxLength: 256, nullable: false),
                    Message = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    DurationMs = table.Column<long>(type: "bigint", nullable: true),
                    ExtraJson = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    CreatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    CreatedBy = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    UpdatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    UpdatedBy = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    IsDeleted = table.Column<bool>(type: "bit", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_BackgroundTaskLog", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "BackgroundTask",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    PartitionConfigurationId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    DataSourceId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    Status = table.Column<int>(type: "int", nullable: false),
                    Phase = table.Column<string>(type: "nvarchar(64)", maxLength: 64, nullable: false),
                    Progress = table.Column<double>(type: "float", nullable: false),
                    QueuedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: true),
                    StartedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: true),
                    CompletedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: true),
                    LastHeartbeatUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    FailureReason = table.Column<string>(type: "nvarchar(512)", maxLength: 512, nullable: true),
                    SummaryJson = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    RequestedBy = table.Column<string>(type: "nvarchar(64)", maxLength: 64, nullable: false),
                    BackupReference = table.Column<string>(type: "nvarchar(256)", maxLength: 256, nullable: true),
                    Notes = table.Column<string>(type: "nvarchar(512)", maxLength: 512, nullable: true),
                    Priority = table.Column<int>(type: "int", nullable: false, defaultValue: 0),
                    CreatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    CreatedBy = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    UpdatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    UpdatedBy = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    IsDeleted = table.Column<bool>(type: "bit", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_BackgroundTask", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_BackgroundTaskLog_ExecutionTaskId_LogTimeUtc",
                table: "BackgroundTaskLog",
                columns: new[] { "ExecutionTaskId", "LogTimeUtc" });

            migrationBuilder.CreateIndex(
                name: "IX_BackgroundTask_DataSourceId_Status",
                table: "BackgroundTask",
                columns: new[] { "DataSourceId", "Status" });

            migrationBuilder.CreateIndex(
                name: "IX_BackgroundTask_PartitionConfigurationId_IsDeleted",
                table: "BackgroundTask",
                columns: new[] { "PartitionConfigurationId", "IsDeleted" },
                filter: "[IsDeleted] = 0");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "BackgroundTaskLog");

            migrationBuilder.DropTable(
                name: "BackgroundTask");
        }
    }
}
