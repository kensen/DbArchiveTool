using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddTargetServerConfiguration : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "TargetDatabaseName",
                table: "ArchiveDataSource",
                type: "nvarchar(128)",
                maxLength: 128,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "TargetPassword",
                table: "ArchiveDataSource",
                type: "nvarchar(256)",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "TargetServerAddress",
                table: "ArchiveDataSource",
                type: "nvarchar(128)",
                maxLength: 128,
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "TargetServerPort",
                table: "ArchiveDataSource",
                type: "int",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<bool>(
                name: "TargetUseIntegratedSecurity",
                table: "ArchiveDataSource",
                type: "bit",
                nullable: false,
                defaultValue: false);

            migrationBuilder.AddColumn<string>(
                name: "TargetUserName",
                table: "ArchiveDataSource",
                type: "nvarchar(64)",
                maxLength: 64,
                nullable: true);

            migrationBuilder.AddColumn<bool>(
                name: "UseSourceAsTarget",
                table: "ArchiveDataSource",
                type: "bit",
                nullable: false,
                defaultValue: true);

            // PartitionCommand 表已在之前的迁移 20251007053916_AddPartitionCommandExtendedFields 中创建
            // 此处移除重复的 CreateTable 操作以避免冲突
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            // PartitionCommand 表由之前的迁移管理,此处不删除

            migrationBuilder.DropColumn(
                name: "TargetDatabaseName",
                table: "ArchiveDataSource");

            migrationBuilder.DropColumn(
                name: "TargetPassword",
                table: "ArchiveDataSource");

            migrationBuilder.DropColumn(
                name: "TargetServerAddress",
                table: "ArchiveDataSource");

            migrationBuilder.DropColumn(
                name: "TargetServerPort",
                table: "ArchiveDataSource");

            migrationBuilder.DropColumn(
                name: "TargetUseIntegratedSecurity",
                table: "ArchiveDataSource");

            migrationBuilder.DropColumn(
                name: "TargetUserName",
                table: "ArchiveDataSource");

            migrationBuilder.DropColumn(
                name: "UseSourceAsTarget",
                table: "ArchiveDataSource");
        }
    }
}
