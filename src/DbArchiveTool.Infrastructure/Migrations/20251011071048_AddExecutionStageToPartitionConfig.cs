using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddExecutionStageToPartitionConfig : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "ConfigurationSnapshot",
                table: "PartitionExecutionTask",
                type: "nvarchar(max)",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "LastCheckpoint",
                table: "PartitionExecutionTask",
                type: "nvarchar(max)",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ExecutionStage",
                table: "PartitionConfiguration",
                type: "nvarchar(50)",
                maxLength: 50,
                nullable: true);

            migrationBuilder.AddColumn<Guid>(
                name: "LastExecutionTaskId",
                table: "PartitionConfiguration",
                type: "uniqueidentifier",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "ConfigurationSnapshot",
                table: "PartitionExecutionTask");

            migrationBuilder.DropColumn(
                name: "LastCheckpoint",
                table: "PartitionExecutionTask");

            migrationBuilder.DropColumn(
                name: "ExecutionStage",
                table: "PartitionConfiguration");

            migrationBuilder.DropColumn(
                name: "LastExecutionTaskId",
                table: "PartitionConfiguration");
        }
    }
}
