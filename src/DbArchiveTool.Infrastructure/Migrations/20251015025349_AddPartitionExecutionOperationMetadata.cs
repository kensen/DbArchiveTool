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
                table: "PartitionExecutionTask",
                type: "nvarchar(128)",
                maxLength: 128,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ArchiveTargetConnection",
                table: "PartitionExecutionTask",
                type: "nvarchar(512)",
                maxLength: 512,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ArchiveTargetDatabase",
                table: "PartitionExecutionTask",
                type: "nvarchar(128)",
                maxLength: 128,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ArchiveTargetTable",
                table: "PartitionExecutionTask",
                type: "nvarchar(256)",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "OperationType",
                table: "PartitionExecutionTask",
                type: "int",
                nullable: false,
                defaultValue: 0);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "ArchiveScheme",
                table: "PartitionExecutionTask");

            migrationBuilder.DropColumn(
                name: "ArchiveTargetConnection",
                table: "PartitionExecutionTask");

            migrationBuilder.DropColumn(
                name: "ArchiveTargetDatabase",
                table: "PartitionExecutionTask");

            migrationBuilder.DropColumn(
                name: "ArchiveTargetTable",
                table: "PartitionExecutionTask");

            migrationBuilder.DropColumn(
                name: "OperationType",
                table: "PartitionExecutionTask");
        }
    }
}
