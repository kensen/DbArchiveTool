using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class RemoveScheduledFieldsFromArchiveConfiguration : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_ArchiveConfiguration_NextArchive",
                table: "ArchiveConfiguration");

            migrationBuilder.DropColumn(
                name: "CronExpression",
                table: "ArchiveConfiguration");

            migrationBuilder.DropColumn(
                name: "EnableScheduledArchive",
                table: "ArchiveConfiguration");

            migrationBuilder.DropColumn(
                name: "IsEnabled",
                table: "ArchiveConfiguration");

            migrationBuilder.DropColumn(
                name: "LastArchivedRowCount",
                table: "ArchiveConfiguration");

            migrationBuilder.DropColumn(
                name: "LastExecutionStatus",
                table: "ArchiveConfiguration");

            migrationBuilder.DropColumn(
                name: "LastExecutionTimeUtc",
                table: "ArchiveConfiguration");

            migrationBuilder.DropColumn(
                name: "NextArchiveAtUtc",
                table: "ArchiveConfiguration");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "CronExpression",
                table: "ArchiveConfiguration",
                type: "nvarchar(100)",
                maxLength: 100,
                nullable: true);

            migrationBuilder.AddColumn<bool>(
                name: "EnableScheduledArchive",
                table: "ArchiveConfiguration",
                type: "bit",
                nullable: false,
                defaultValue: false);

            migrationBuilder.AddColumn<bool>(
                name: "IsEnabled",
                table: "ArchiveConfiguration",
                type: "bit",
                nullable: false,
                defaultValue: true);

            migrationBuilder.AddColumn<long>(
                name: "LastArchivedRowCount",
                table: "ArchiveConfiguration",
                type: "bigint",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "LastExecutionStatus",
                table: "ArchiveConfiguration",
                type: "nvarchar(50)",
                maxLength: 50,
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name: "LastExecutionTimeUtc",
                table: "ArchiveConfiguration",
                type: "datetime2",
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name: "NextArchiveAtUtc",
                table: "ArchiveConfiguration",
                type: "datetime2",
                nullable: true);

            migrationBuilder.CreateIndex(
                name: "IX_ArchiveConfiguration_NextArchive",
                table: "ArchiveConfiguration",
                column: "NextArchiveAtUtc",
                filter: "[IsEnabled] = 1 AND [EnableScheduledArchive] = 1");
        }
    }
}
