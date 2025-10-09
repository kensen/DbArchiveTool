
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Domain.Partitions;
using Microsoft.Extensions.Logging;
using Moq;

namespace DbArchiveTool.UnitTests.Partitions;

/// <summary>
/// 验证 PartitionConfigurationAppService 的输入校验与成功路径。
/// </summary>
public class PartitionConfigurationAppServiceTests
{
    private readonly Mock<IPartitionMetadataRepository> metadataRepository = new();
    private readonly Mock<IPartitionConfigurationRepository> configurationRepository = new();
    private readonly PartitionValueParser parser = new();
    private readonly Mock<ILogger<PartitionConfigurationAppService>> logger = new();

    [Fact]
    public async Task CreateAsync_ShouldFail_WhenDataSourceIdMissing()
    {
        var service = CreateService();
        var request = new CreatePartitionConfigurationRequest(
            Guid.Empty,
            "dbo",
            "Orders",
            PartitionStorageMode.PrimaryFilegroup,
            null,
            null,
            null,
            null,
            null,
            "ArchiveDb",
            "dbo",
            "Orders_bak",
            true,
            "tester",
            null);

        var result = await service.CreateAsync(request);

        Assert.False(result.IsSuccess);
        Assert.Equal("数据源标识不能为空。", result.Error);
        metadataRepository.VerifyNoOtherCalls();
        configurationRepository.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task CreateAsync_ShouldFail_WhenMetadataMissing()
    {
        metadataRepository.Setup(x => x.GetConfigurationAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var request = CreateValidRequest();

        var result = await service.CreateAsync(request);

        Assert.False(result.IsSuccess);
        Assert.Equal("未找到目标表的分区元数据信息。", result.Error);
        configurationRepository.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task CreateAsync_ShouldFail_WhenColumnNullableAndNotForced()
    {
        metadataRepository.Setup(x => x.GetConfigurationAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateMetadataConfiguration(isColumnNullable: true));
        configurationRepository.Setup(x => x.GetByTableAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var request = CreateValidRequest(requireNotNull: false);

        var result = await service.CreateAsync(request);

        Assert.False(result.IsSuccess);
        Assert.Equal("目标分区列当前允许 NULL，请勾选“去可空”以确保脚本生成 ALTER COLUMN。", result.Error);
        configurationRepository.Verify(x => x.AddAsync(It.IsAny<PartitionConfiguration>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CreateAsync_ShouldPersist_WhenValid()
    {
        metadataRepository.Setup(x => x.GetConfigurationAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateMetadataConfiguration(isColumnNullable: false));
        configurationRepository.Setup(x => x.GetByTableAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var request = CreateValidRequest();

        var result = await service.CreateAsync(request);

        Assert.True(result.IsSuccess);
        configurationRepository.Verify(x => x.AddAsync(It.Is<PartitionConfiguration>(p =>
            p.ArchiveDataSourceId == request.DataSourceId &&
            p.SchemaName == request.SchemaName &&
            p.TableName == request.TableName), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ReplaceValuesAsync_ShouldFail_WhenConfigurationMissing()
    {
        configurationRepository.Setup(x => x.GetByIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var result = await service.ReplaceValuesAsync(Guid.NewGuid(), new ReplacePartitionValuesRequest(new[] { "10" }, "tester"));

        Assert.False(result.IsSuccess);
        Assert.Equal("未找到分区配置。", result.Error);
    }

    [Fact]
    public async Task ReplaceValuesAsync_ShouldSortValuesAndPersist()
    {
        var configuration = CreateMetadataConfiguration(isColumnNullable: false);
        configurationRepository.Setup(x => x.GetByIdAsync(configuration.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        var service = CreateService();
        var request = new ReplacePartitionValuesRequest(new[] { "20", "10", "20" }, "tester");

        var result = await service.ReplaceValuesAsync(configuration.Id, request);

        Assert.True(result.IsSuccess);
        configurationRepository.Verify(x => x.UpdateAsync(It.Is<PartitionConfiguration>(p =>
            p.Boundaries.Select(b => b.Value.ToInvariantString()).SequenceEqual(new[] { "10", "20" })), It.IsAny<CancellationToken>()), Times.Once);
    }

    private PartitionConfigurationAppService CreateService()
        => new(metadataRepository.Object, configurationRepository.Object, parser, logger.Object);

    private static PartitionConfiguration CreateMetadataConfiguration(bool isColumnNullable)
        => new(
            Guid.NewGuid(),
            "dbo",
            "Orders",
            "pf_orders",
            "ps_orders",
            new PartitionColumn("OrderDate", PartitionValueKind.Int, isColumnNullable),
            PartitionFilegroupStrategy.Default("PRIMARY"),
            isRangeRight: true,
            storageSettings: PartitionStorageSettings.UsePrimary("PRIMARY"));

    private CreatePartitionConfigurationRequest CreateValidRequest(bool requireNotNull = true)
        => new(
            Guid.NewGuid(),
            "dbo",
            "Orders",
            PartitionStorageMode.PrimaryFilegroup,
            "PRIMARY",
            null,
            null,
            null,
            null,
            "ArchiveDb",
            "dbo",
            "Orders_bak",
            requireNotNull,
            "tester",
            "demo");
}
