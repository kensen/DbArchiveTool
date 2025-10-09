using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace DbArchiveTool.Infrastructure.Migrations
{
    /// <inheritdoc />
    public partial class AddPartitionConfiguration : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "PartitionConfiguration",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    ArchiveDataSourceId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    SchemaName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    TableName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    PartitionFunctionName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    PartitionSchemeName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    PartitionColumnName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    PartitionColumnKind = table.Column<int>(type: "int", nullable: false),
                    PartitionColumnIsNullable = table.Column<bool>(type: "bit", nullable: false),
                    PrimaryFilegroup = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    IsRangeRight = table.Column<bool>(type: "bit", nullable: false),
                    RequirePartitionColumnNotNull = table.Column<bool>(type: "bit", nullable: false),
                    Remarks = table.Column<string>(type: "nvarchar(512)", maxLength: 512, nullable: true),
                    StorageMode = table.Column<int>(type: "int", nullable: false),
                    StorageFilegroupName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    StorageDataFileDirectory = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: true),
                    StorageDataFileName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: true),
                    StorageInitialSizeMb = table.Column<int>(type: "int", nullable: true),
                    StorageAutoGrowthMb = table.Column<int>(type: "int", nullable: true),
                    TargetDatabaseName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    TargetSchemaName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    TargetTableName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    TargetRemarks = table.Column<string>(type: "nvarchar(512)", maxLength: 512, nullable: true),
                    CreatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    CreatedBy = table.Column<string>(type: "nvarchar(64)", maxLength: 64, nullable: false),
                    UpdatedAtUtc = table.Column<DateTime>(type: "datetime2", nullable: false),
                    UpdatedBy = table.Column<string>(type: "nvarchar(64)", maxLength: 64, nullable: false),
                    IsDeleted = table.Column<bool>(type: "bit", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PartitionConfiguration", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "PartitionConfigurationBoundary",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    ConfigurationId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    SortKey = table.Column<string>(type: "nvarchar(64)", maxLength: 64, nullable: false),
                    ValueKind = table.Column<int>(type: "int", nullable: false),
                    RawValue = table.Column<string>(type: "nvarchar(max)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PartitionConfigurationBoundary", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PartitionConfigurationBoundary_PartitionConfiguration_ConfigurationId",
                        column: x => x.ConfigurationId,
                        principalTable: "PartitionConfiguration",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PartitionConfigurationFilegroup",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    ConfigurationId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    FilegroupName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    SortOrder = table.Column<int>(type: "int", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PartitionConfigurationFilegroup", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PartitionConfigurationFilegroup_PartitionConfiguration_ConfigurationId",
                        column: x => x.ConfigurationId,
                        principalTable: "PartitionConfiguration",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "PartitionConfigurationFilegroupMapping",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    ConfigurationId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    BoundaryKey = table.Column<string>(type: "nvarchar(64)", maxLength: 64, nullable: false),
                    FilegroupName = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PartitionConfigurationFilegroupMapping", x => x.Id);
                    table.ForeignKey(
                        name: "FK_PartitionConfigurationFilegroupMapping_PartitionConfiguration_ConfigurationId",
                        column: x => x.ConfigurationId,
                        principalTable: "PartitionConfiguration",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_PartitionConfiguration_ArchiveDataSourceId_SchemaName_TableName",
                table: "PartitionConfiguration",
                columns: new[] { "ArchiveDataSourceId", "SchemaName", "TableName" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_PartitionConfigurationBoundary_ConfigurationId",
                table: "PartitionConfigurationBoundary",
                column: "ConfigurationId");

            migrationBuilder.CreateIndex(
                name: "IX_PartitionConfigurationFilegroup_ConfigurationId",
                table: "PartitionConfigurationFilegroup",
                column: "ConfigurationId");

            migrationBuilder.CreateIndex(
                name: "IX_PartitionConfigurationFilegroupMapping_ConfigurationId",
                table: "PartitionConfigurationFilegroupMapping",
                column: "ConfigurationId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "PartitionConfigurationBoundary");

            migrationBuilder.DropTable(
                name: "PartitionConfigurationFilegroup");

            migrationBuilder.DropTable(
                name: "PartitionConfigurationFilegroupMapping");

            migrationBuilder.DropTable(
                name: "PartitionConfiguration");
        }
    }
}
