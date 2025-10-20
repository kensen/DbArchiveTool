using System.Net.Http.Json;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;

namespace DbArchiveTool.IntegrationTests;

public class BackgroundTaskContextEndpointTests : IClassFixture<ApiWebApplicationFactory>
{
    private readonly ApiWebApplicationFactory factory;

    public BackgroundTaskContextEndpointTests(ApiWebApplicationFactory factory)
    {
        this.factory = factory;
    }

    [Fact]
    public async Task GetWizardContext_ShouldExposeAutoAlignmentInfo()
    {
        var configuration = CreatePartitionConfiguration();
        var configId = configuration.Id;
        var dataSource = new ArchiveDataSource("Test Source", null, "localhost", 1433, "ArchiveDb", true, null, null);

        var configRepository = new Mock<IPartitionConfigurationRepository>();
        configRepository.Setup(x => x.GetByIdAsync(configId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        var dataSourceRepository = new Mock<IDataSourceRepository>();
        dataSourceRepository.Setup(x => x.GetAsync(configuration.ArchiveDataSourceId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dataSource);

        var inspection = new PartitionIndexInspection(
            HasClusteredIndex: true,
            ClusteredIndex: new IndexAlignmentInfo("PK_Test", true, true, false, true, true, new[] { "[Id] ASC" }),
            UniqueIndexes: Array.Empty<IndexAlignmentInfo>(),
            ExternalForeignKeys: Array.Empty<string>());

        var metadataRepository = new Mock<IPartitionMetadataRepository>();
        metadataRepository.Setup(x => x.GetIndexInspectionAsync(
                configuration.ArchiveDataSourceId,
                configuration.SchemaName,
                configuration.TableName,
                configuration.PartitionColumn.Name,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(inspection);
        metadataRepository.Setup(x => x.GetTableStatisticsAsync(
                configuration.ArchiveDataSourceId,
                configuration.SchemaName,
                configuration.TableName,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TableStatistics(true, 5000, 128m, 32m, 160m));
        metadataRepository.Setup(x => x.ListBoundariesAsync(
                configuration.ArchiveDataSourceId,
                configuration.SchemaName,
                configuration.TableName,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<PartitionBoundary>());

        var client = factory.WithWebHostBuilder(builder =>
        {
            builder.ConfigureServices(services =>
            {
                services.RemoveAll<IPartitionConfigurationRepository>();
                services.RemoveAll<IDataSourceRepository>();
                services.RemoveAll<IPartitionMetadataRepository>();
                services.AddSingleton(configRepository.Object);
                services.AddSingleton(dataSourceRepository.Object);
                services.AddSingleton(metadataRepository.Object);
            });
        }).CreateClient();

        var response = await client.GetAsync($"api/v1/background-tasks/wizard/context/{configId}");
        response.EnsureSuccessStatusCode();

        var context = await response.Content.ReadFromJsonAsync<ExecutionWizardContextResponse>();

        Assert.NotNull(context);
        Assert.Equal(configId, context.ConfigurationId);
        Assert.True(context.IndexInspection.CanAutoAlign);
        Assert.Null(context.IndexInspection.BlockingReason);
        Assert.Empty(context.IndexInspection.IndexesNeedingAlignment);
        Assert.NotNull(context.TableStatistics);
        Assert.Equal(5000, context.TableStatistics!.TotalRows);
    }

    private static PartitionConfiguration CreatePartitionConfiguration()
        => new(Guid.NewGuid(),
            "dbo",
            "Orders",
            "pf_orders",
            "ps_orders",
            new PartitionColumn("ArchiveDate", PartitionValueKind.DateTime, isNullable: false),
            PartitionFilegroupStrategy.Default("PRIMARY"),
            isRangeRight: true,
            storageSettings: PartitionStorageSettings.UsePrimary("PRIMARY"));

    private sealed class ExecutionWizardContextResponse
    {
        public Guid ConfigurationId { get; set; }
        public IndexInspectionResponse IndexInspection { get; set; } = new();
        public TableStatisticsResponse? TableStatistics { get; set; }
    }

    private sealed class IndexInspectionResponse
    {
        public bool CanAutoAlign { get; set; }
        public string? BlockingReason { get; set; }
        public List<IndexAlignmentItemResponse> IndexesNeedingAlignment { get; set; } = new();
    }

    private sealed class TableStatisticsResponse
    {
        public bool TableExists { get; set; }
        public long TotalRows { get; set; }
        public decimal TotalSizeMB { get; set; }
    }

    private sealed class IndexAlignmentItemResponse
    {
        public string IndexName { get; set; } = string.Empty;
    }
}
