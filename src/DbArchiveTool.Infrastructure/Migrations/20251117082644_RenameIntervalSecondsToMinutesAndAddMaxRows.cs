using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class RenameIntervalSecondsToMinutesAndAddMaxRows : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "IntervalSeconds",
                table: "ScheduledArchiveJob");

            migrationBuilder.AlterColumn<int>(
                name: "BatchSize",
                table: "ScheduledArchiveJob",
                type: "int",
                nullable: false,
                defaultValue: 5000,
                oldClrType: typeof(int),
                oldType: "int",
                oldDefaultValue: 10000);

            migrationBuilder.AddColumn<int>(
                name: "IntervalMinutes",
                table: "ScheduledArchiveJob",
                type: "int",
                nullable: false,
                defaultValue: 5);

            migrationBuilder.AddColumn<int>(
                name: "MaxRowsPerExecution",
                table: "ScheduledArchiveJob",
                type: "int",
                nullable: false,
                defaultValue: 50000);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "IntervalMinutes",
                table: "ScheduledArchiveJob");

            migrationBuilder.DropColumn(
                name: "MaxRowsPerExecution",
                table: "ScheduledArchiveJob");

            migrationBuilder.AlterColumn<int>(
                name: "BatchSize",
                table: "ScheduledArchiveJob",
                type: "int",
                nullable: false,
                defaultValue: 10000,
                oldClrType: typeof(int),
                oldType: "int",
                oldDefaultValue: 5000);

            migrationBuilder.AddColumn<int>(
                name: "IntervalSeconds",
                table: "ScheduledArchiveJob",
                type: "int",
                nullable: false,
                defaultValue: 10);
        }
    }
}
