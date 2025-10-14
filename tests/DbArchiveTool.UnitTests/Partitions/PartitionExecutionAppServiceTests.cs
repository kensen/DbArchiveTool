using System.Threading;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Application.Partitions.Dtos;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.Logging;
using Moq;

namespace DbArchiveTool.UnitTests.Partitions;

public class PartitionExecutionAppServiceTests
{
    private readonly Mock<IPartitionConfigurationRepository> configurationRepository = new();
    private readonly Mock<IPartitionExecutionTaskRepository> taskRepository = new();
    private readonly Mock<IPartitionExecutionLogRepository> logRepository = new();
    private readonly Mock<IPartitionExecutionDispatcher> dispatcher = new();
    private readonly Mock<IDataSourceRepository> dataSourceRepository = new();
    private readonly Mock<IPermissionInspectionRepository> permissionRepository = new();
    private readonly Mock<IPartitionMetadataRepository> metadataRepository = new();
    private readonly Mock<ILogger<PartitionExecutionAppService>> logger = new();

    [Fact]
    public async Task GetExecutionContext_ShouldFlagBlocking_WhenClusteredIndexMissing()
    {
        // Arrange
        var configuration = CreatePartitionConfiguration();
        var configId = configuration.Id;
        configurationRepository.Setup(x => x.GetByIdAsync(configId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);
        dataSourceRepository.Setup(x => x.GetAsync(configuration.ArchiveDataSourceId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateDataSource());
        metadataRepository.Setup(x => x.GetIndexInspectionAsync(
                configuration.ArchiveDataSourceId,
                configuration.SchemaName,
                configuration.TableName,
                configuration.PartitionColumn.Name,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PartitionIndexInspection(
                HasClusteredIndex: false,
                ClusteredIndex: null,
                UniqueIndexes: Array.Empty<IndexAlignmentInfo>(),
                ExternalForeignKeys: Array.Empty<string>()));

        var service = CreateService();

        // Act
        var result = await service.GetExecutionContextAsync(configId);

        // Assert
        Assert.True(result.IsSuccess);
        var inspection = result.Value!.IndexInspection;
        Assert.False(inspection.CanAutoAlign);
        Assert.Equal("目标表尚未创建聚集索引，无法自动对齐分区索引。", inspection.BlockingReason);
    }

    [Fact]
    public async Task GetExecutionContext_ShouldAllowAutoAlign_WhenIndexesAlreadyValid()
    {
        var configuration = CreatePartitionConfiguration();
        var configId = configuration.Id;
        configurationRepository.Setup(x => x.GetByIdAsync(configId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);
        dataSourceRepository.Setup(x => x.GetAsync(configuration.ArchiveDataSourceId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateDataSource());
        metadataRepository.Setup(x => x.GetIndexInspectionAsync(
                configuration.ArchiveDataSourceId,
                configuration.SchemaName,
                configuration.TableName,
                configuration.PartitionColumn.Name,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PartitionIndexInspection(
                HasClusteredIndex: true,
                ClusteredIndex: new IndexAlignmentInfo(
                    IndexName: "PK_Test",
                IsClustered: true,
                IsPrimaryKey: true,
                IsUniqueConstraint: false,
                IsUnique: true,
                ContainsPartitionColumn: true,
                KeyColumns: new[] { "[Id] ASC" }),
                UniqueIndexes: Array.Empty<IndexAlignmentInfo>(),
                ExternalForeignKeys: Array.Empty<string>()));

        var service = CreateService();

        var result = await service.GetExecutionContextAsync(configId);

        Assert.True(result.IsSuccess);
        var inspection = result.Value!.IndexInspection;
        Assert.True(inspection.CanAutoAlign);
        Assert.Null(inspection.BlockingReason);
    }

    [Fact]
    public async Task GetExecutionContext_ShouldReturnBlockingReason_WhenInspectionFails()
    {
        var configuration = CreatePartitionConfiguration();
        var configId = configuration.Id;
        configurationRepository.Setup(x => x.GetByIdAsync(configId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);
        dataSourceRepository.Setup(x => x.GetAsync(configuration.ArchiveDataSourceId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateDataSource());
        metadataRepository.Setup(x => x.GetIndexInspectionAsync(
                configuration.ArchiveDataSourceId,
                configuration.SchemaName,
                configuration.TableName,
                configuration.PartitionColumn.Name,
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("metadata unavailable"));

        var service = CreateService();

        var result = await service.GetExecutionContextAsync(configId);

        Assert.True(result.IsSuccess);
        var inspection = result.Value!.IndexInspection;
        Assert.False(inspection.CanAutoAlign);
        Assert.Contains("索引检查失败", inspection.BlockingReason);
        Assert.Contains("metadata unavailable", inspection.BlockingReason);
    }

    private PartitionExecutionAppService CreateService()
        => new(configurationRepository.Object,
            taskRepository.Object,
            logRepository.Object,
            dispatcher.Object,
            dataSourceRepository.Object,
            permissionRepository.Object,
            metadataRepository.Object,
            logger.Object);

    private static PartitionConfiguration CreatePartitionConfiguration()
        => new(Guid.NewGuid(),
            "dbo",
            "TestTable",
            "pf_Test",
            "ps_Test",
            new PartitionColumn("ArchiveDate", PartitionValueKind.DateTime, isNullable: false),
            PartitionFilegroupStrategy.Default("PRIMARY"),
            isRangeRight: true,
            storageSettings: PartitionStorageSettings.UsePrimary("PRIMARY"));

    private static ArchiveDataSource CreateDataSource()
        => new("Test Source", null, "localhost", 1433, "ArchiveDb", useIntegratedSecurity: true, userName: null, password: null);
}
