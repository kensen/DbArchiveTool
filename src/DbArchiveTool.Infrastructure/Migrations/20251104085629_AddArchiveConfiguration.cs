using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddArchiveConfiguration : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "ArchiveConfiguration",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    Name = table.Column<string>(type: "nvarchar(100)", maxLength: 100, nullable: false),
                    Description = table.Column<string>(type: "nvarchar(256)", maxLength: 256, nullable: true),
                    DataSourceId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    SourceSchemaName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    SourceTableName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    IsPartitionedTable = table.Column<bool>(type: "bit", nullable: false),
                    PartitionConfigurationId = table.Column<Guid>(type: "uniqueidentifier", nullable: true),
                    ArchiveFilterColumn = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: true),
                    ArchiveFilterCondition = table.Column<string>(type: "nvarchar(512)", maxLength: 512, nullable: true),
                    ArchiveMethod = table.Column<int>(type: "int", nullable: false),
                    DeleteSourceDataAfterArchive = table.Column<bool>(type: "bit", nullable: false, defaultValue: true),
                    BatchSize = table.Column<int>(type: "int", nullable: false, defaultValue: 10000),
                    IsEnabled = table.Column<bool>(type: "bit", nullable: false, defaultValue: true),
                    LastExecutionTimeUtc = table.Column<DateTime>(type: "datetime2", nullable: true),
                    LastExecutionStatus = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: true),
                    LastArchivedRowCount = table.Column<long>(type: "bigint", nullable: true),
                    CreatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    CreatedBy = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    UpdatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    UpdatedBy = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    IsDeleted = table.Column<bool>(type: "bit", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ArchiveConfiguration", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_ArchiveConfiguration_DataSourceId_SourceSchemaName_SourceTableName",
                table: "ArchiveConfiguration",
                columns: new[] { "DataSourceId", "SourceSchemaName", "SourceTableName" },
                unique: true,
                filter: "[IsDeleted] = 0");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ArchiveConfiguration");
        }
    }
}
