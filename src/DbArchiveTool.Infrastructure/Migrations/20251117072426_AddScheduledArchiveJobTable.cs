using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddScheduledArchiveJobTable : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "ScheduledArchiveJob",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    Name = table.Column<string>(type: "nvarchar(200)", maxLength: 200, nullable: false),
                    Description = table.Column<string>(type: "nvarchar(500)", maxLength: 500, nullable: true),
                    DataSourceId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    SourceSchemaName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    SourceTableName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    TargetSchemaName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    TargetTableName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    ArchiveFilterColumn = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    ArchiveFilterCondition = table.Column<string>(type: "nvarchar(500)", maxLength: 500, nullable: false),
                    ArchiveMethod = table.Column<int>(type: "int", nullable: false),
                    DeleteSourceDataAfterArchive = table.Column<bool>(type: "bit", nullable: false, defaultValue: true),
                    BatchSize = table.Column<int>(type: "int", nullable: false, defaultValue: 10000),
                    IntervalSeconds = table.Column<int>(type: "int", nullable: false, defaultValue: 10),
                    CronExpression = table.Column<string>(type: "nvarchar(100)", maxLength: 100, nullable: true),
                    IsEnabled = table.Column<bool>(type: "bit", nullable: false, defaultValue: true),
                    NextExecutionAtUtc = table.Column<DateTime>(type: "datetime2", nullable: true),
                    LastExecutionAtUtc = table.Column<DateTime>(type: "datetime2", nullable: true),
                    LastExecutionStatus = table.Column<int>(type: "int", nullable: false),
                    LastExecutionError = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    LastArchivedRowCount = table.Column<long>(type: "bigint", nullable: true),
                    TotalExecutionCount = table.Column<long>(type: "bigint", nullable: false, defaultValue: 0L),
                    TotalArchivedRowCount = table.Column<long>(type: "bigint", nullable: false, defaultValue: 0L),
                    ConsecutiveFailureCount = table.Column<int>(type: "int", nullable: false, defaultValue: 0),
                    MaxConsecutiveFailures = table.Column<int>(type: "int", nullable: false, defaultValue: 5),
                    CreatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    CreatedBy = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    UpdatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    UpdatedBy = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    IsDeleted = table.Column<bool>(type: "bit", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ScheduledArchiveJob", x => x.Id);
                    table.ForeignKey(
                        name: "FK_ScheduledArchiveJob_ArchiveDataSource_DataSourceId",
                        column: x => x.DataSourceId,
                        principalTable: "ArchiveDataSource",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateIndex(
                name: "IX_ScheduledArchiveJob_DataSourceId",
                table: "ScheduledArchiveJob",
                column: "DataSourceId",
                filter: "[IsDeleted] = 0");

            migrationBuilder.CreateIndex(
                name: "IX_ScheduledArchiveJob_LastExecutionAtUtc",
                table: "ScheduledArchiveJob",
                column: "LastExecutionAtUtc",
                filter: "[IsDeleted] = 0");

            migrationBuilder.CreateIndex(
                name: "IX_ScheduledArchiveJob_NextExecutionAtUtc",
                table: "ScheduledArchiveJob",
                column: "NextExecutionAtUtc",
                filter: "[IsEnabled] = 1 AND [IsDeleted] = 0");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ScheduledArchiveJob");
        }
    }
}
