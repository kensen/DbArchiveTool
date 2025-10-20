using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddPartitionExecutionOperationMetadata : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "ArchiveScheme",
                table: "BackgroundTask",
                type: "nvarchar(128)",
                maxLength: 128,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ArchiveTargetConnection",
                table: "BackgroundTask",
                type: "nvarchar(512)",
                maxLength: 512,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ArchiveTargetDatabase",
                table: "BackgroundTask",
                type: "nvarchar(128)",
                maxLength: 128,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ArchiveTargetTable",
                table: "BackgroundTask",
                type: "nvarchar(256)",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "OperationType",
                table: "BackgroundTask",
                type: "int",
                nullable: false,
                defaultValue: 0);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "ArchiveScheme",
                table: "BackgroundTask");

            migrationBuilder.DropColumn(
                name: "ArchiveTargetConnection",
                table: "BackgroundTask");

            migrationBuilder.DropColumn(
                name: "ArchiveTargetDatabase",
                table: "BackgroundTask");

            migrationBuilder.DropColumn(
                name: "ArchiveTargetTable",
                table: "BackgroundTask");

            migrationBuilder.DropColumn(
                name: "OperationType",
                table: "BackgroundTask");
        }
    }
}
