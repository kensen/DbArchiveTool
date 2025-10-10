using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class UpdatePartitionConfigUniqueIndex : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_PartitionConfiguration_ArchiveDataSourceId_SchemaName_TableName",
                table: "PartitionConfiguration");

            migrationBuilder.CreateIndex(
                name: "IX_PartitionConfiguration_ArchiveDataSourceId_SchemaName_TableName",
                table: "PartitionConfiguration",
                columns: new[] { "ArchiveDataSourceId", "SchemaName", "TableName" },
                unique: true,
                filter: "[IsDeleted] = 0");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_PartitionConfiguration_ArchiveDataSourceId_SchemaName_TableName",
                table: "PartitionConfiguration");

            migrationBuilder.CreateIndex(
                name: "IX_PartitionConfiguration_ArchiveDataSourceId_SchemaName_TableName",
                table: "PartitionConfiguration",
                columns: new[] { "ArchiveDataSourceId", "SchemaName", "TableName" },
                unique: true);
        }
    }
}
