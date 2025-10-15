using System;
using System.Linq;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Partitions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using Xunit.Sdk;

namespace DbArchiveTool.UnitTests.Partitions;

/// <summary>
/// 验证 PartitionConfigurationAppService 的输入校验与成功路径。
/// </summary>
public class PartitionConfigurationAppServiceTests
{
    private readonly Mock<IPartitionMetadataRepository> metadataRepository = new();
    private readonly Mock<IPartitionConfigurationRepository> configurationRepository = new();
    private readonly Mock<IPartitionExecutionTaskRepository> taskRepository = new();
    private readonly Mock<IPartitionExecutionLogRepository> logRepository = new();
    private readonly Mock<IPartitionAuditLogRepository> auditRepository = new();
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
            "OrderDate",
            PartitionValueKind.DateTime,
            false,
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
    public async Task CreateAsync_ShouldSucceed_WhenMetadataUnavailable()
    {
        configurationRepository.Setup(x => x.GetByTableAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var request = CreateValidRequest();

        var result = await service.CreateAsync(request);

        Assert.True(result.IsSuccess);
        configurationRepository.Verify(x => x.AddAsync(It.IsAny<PartitionConfiguration>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateAsync_ShouldFail_WhenColumnNullableAndNotForced()
    {
        configurationRepository.Setup(x => x.GetByTableAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var request = CreateValidRequest(requireNotNull: false, columnIsNullable: true);

        var result = await service.CreateAsync(request);

        Assert.False(result.IsSuccess);
        Assert.Equal("目标分区列当前允许 NULL，请勾选'去可空'以确保脚本生成 ALTER COLUMN。", result.Error);
        configurationRepository.Verify(x => x.AddAsync(It.IsAny<PartitionConfiguration>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CreateAsync_ShouldPersist_WhenValid()
    {
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

    [Fact]
    public async Task CreateAsync_ShouldSucceed_AfterDeletingPreviousConfiguration()
    {
        // Simulate: GetByTableAsync returns null because deleted configs are filtered out
        configurationRepository.Setup(x => x.GetByTableAsync(It.IsAny<Guid>(), "dbo", "Orders", It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var request = CreateValidRequest();

        var result = await service.CreateAsync(request);

        // Should succeed since no active (non-deleted) configuration exists
        Assert.True(result.IsSuccess);
        configurationRepository.Verify(x => x.AddAsync(It.IsAny<PartitionConfiguration>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UpdateAsync_ShouldFail_WhenConfigurationMissing()
    {
        configurationRepository.Setup(x => x.GetByIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var request = new UpdatePartitionConfigurationRequest(
            PartitionStorageMode.PrimaryFilegroup,
            "PRIMARY",
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

        var result = await service.UpdateAsync(Guid.NewGuid(), request);

        Assert.False(result.IsSuccess);
        Assert.Equal("未找到分区配置。", result.Error);
    }

    [Fact]
    public async Task UpdateAsync_ShouldFail_WhenConfigurationCommitted()
    {
        var configuration = CreateMetadataConfiguration(isColumnNullable: false);
        configuration.MarkCommitted("tester");
        configurationRepository.Setup(x => x.GetByIdAsync(configuration.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        var service = CreateService();
        var request = new UpdatePartitionConfigurationRequest(
            PartitionStorageMode.PrimaryFilegroup,
            "PRIMARY",
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

        var result = await service.UpdateAsync(configuration.Id, request);

        Assert.False(result.IsSuccess);
        Assert.Equal("配置已执行，禁止修改。", result.Error);
    }

    [Fact]
    public async Task UpdateAsync_ShouldFail_WhenTableAlreadyPartitioned()
    {
        var configuration = CreateMetadataConfiguration(isColumnNullable: false);
        configurationRepository.Setup(x => x.GetByIdAsync(configuration.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        metadataRepository.Setup(x => x.GetConfigurationAsync(configuration.ArchiveDataSourceId, configuration.SchemaName, configuration.TableName, It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateMetadataConfiguration(isColumnNullable: false));

        var service = CreateService();
        var request = new UpdatePartitionConfigurationRequest(
            PartitionStorageMode.PrimaryFilegroup,
            "PRIMARY",
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

        var result = await service.UpdateAsync(configuration.Id, request);

        Assert.False(result.IsSuccess);
        Assert.Equal("目标表已是分区表，禁止修改方案，请通过分区操作功能调整边界。", result.Error);
    }

    [Fact]
    public async Task UpdateAsync_ShouldSucceed_WhenValid()
    {
        var configuration = CreateMetadataConfiguration(isColumnNullable: false);
        configurationRepository.Setup(x => x.GetByIdAsync(configuration.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        metadataRepository.Setup(x => x.GetConfigurationAsync(configuration.ArchiveDataSourceId, configuration.SchemaName, configuration.TableName, It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var request = new UpdatePartitionConfigurationRequest(
            PartitionStorageMode.DedicatedFilegroupSingleFile,
            "ORDERS_FG",
            "D:\\\\Data",
            "Orders.ndf",
            1024,
            256,
            "ArchiveDb",
            "dbo",
            "Orders_bak_new",
            false,
            "tester",
            "updated");

        var result = await service.UpdateAsync(configuration.Id, request);

        Assert.True(result.IsSuccess);
        configurationRepository.Verify(x => x.UpdateAsync(It.Is<PartitionConfiguration>(p =>
            p.StorageSettings.Mode == PartitionStorageMode.DedicatedFilegroupSingleFile &&
            p.StorageSettings.FilegroupName == "ORDERS_FG" &&
            p.TargetTable != null &&
            p.TargetTable.TableName == "Orders_bak_new"), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetAsync_ShouldFail_WhenConfigurationMissing()
    {
        configurationRepository.Setup(x => x.GetByIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var result = await service.GetAsync(Guid.NewGuid());

        Assert.False(result.IsSuccess);
        Assert.Equal("未找到分区配置。", result.Error);
    }

    [Fact]
    public async Task GetAsync_ShouldReturnDetail_WhenFound()
    {
        var configuration = CreateMetadataConfiguration(isColumnNullable: false, valueKind: PartitionValueKind.Date);
        configuration.ReplaceBoundaries(new[]
        {
            PartitionValue.FromInt(10),
            PartitionValue.FromInt(20)
        });

        configurationRepository.Setup(x => x.GetByIdAsync(configuration.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);
        metadataRepository.Setup(x => x.GetConfigurationAsync(configuration.ArchiveDataSourceId, configuration.SchemaName, configuration.TableName, It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var result = await service.GetAsync(configuration.Id);

        Assert.True(result.IsSuccess, result.Error);
        Assert.NotNull(result.Value);
        Assert.Equal(configuration.SchemaName, result.Value!.SchemaName);
        Assert.Equal(2, result.Value.BoundaryValues.Count);
    }

    [Fact]
    public async Task AddBoundaryAsync_Should_Record_Audit_And_Filegroup()
    {
        var configuration = CreateMetadataConfiguration(isColumnNullable: false, valueKind: PartitionValueKind.Date);
        configuration.ReplaceBoundaries(new[]
        {
            PartitionValue.FromDate(new DateOnly(2024, 1, 1))
        });

        configurationRepository.Setup(x => x.GetByIdAsync(configuration.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        var service = CreateService();
        var request = new AddPartitionBoundaryRequest("2024-01-05", "FG_ARCHIVE", "tester");

        var result = await service.AddBoundaryAsync(configuration.Id, request);
        if (!result.IsSuccess)
        {
            throw new XunitException($"Expected success but got: {result.Error ?? "null"}");
        }
        Assert.Contains(configuration.Boundaries, b => b.Value.ToInvariantString() == "2024-01-05");
        Assert.Equal("FG_ARCHIVE", configuration.ResolveFilegroup(configuration.Boundaries.Last().SortKey));

        configurationRepository.Verify(x => x.UpdateAsync(configuration, It.IsAny<CancellationToken>()), Times.Once);
        taskRepository.Verify(x => x.AddAsync(It.IsAny<PartitionExecutionTask>(), It.IsAny<CancellationToken>()), Times.Once);
        logRepository.Verify(x => x.AddAsync(It.IsAny<PartitionExecutionLogEntry>(), It.IsAny<CancellationToken>()), Times.Once);
        auditRepository.Verify(x => x.AddAsync(It.Is<PartitionAuditLog>(log =>
            log.Action == PartitionExecutionOperationType.AddBoundary.ToString() &&
            log.ResourceId == configuration.Id.ToString() &&
            log.PayloadJson != null &&
            log.PayloadJson.Contains("FG_ARCHIVE", StringComparison.OrdinalIgnoreCase)), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task AddBoundaryAsync_ShouldFail_WhenConfigurationMissing()
    {
        configurationRepository.Setup(x => x.GetByIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var result = await service.AddBoundaryAsync(Guid.NewGuid(), new AddPartitionBoundaryRequest("2024-01-05", null, "tester"));

        Assert.False(result.IsSuccess);
        Assert.Equal("未找到指定的分区配置。", result.Error);
        auditRepository.Verify(x => x.AddAsync(It.IsAny<PartitionAuditLog>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task SplitBoundaryAsync_ShouldFail_WhenValueNotWithinRange()
    {
        var configuration = CreateMetadataConfiguration(isColumnNullable: false);
        configuration.ReplaceBoundaries(new[]
        {
            PartitionValue.FromInt(10),
            PartitionValue.FromInt(20)
        });

        configurationRepository.Setup(x => x.GetByIdAsync(configuration.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        var service = CreateService();
        var result = await service.SplitBoundaryAsync(configuration.Id, new SplitPartitionBoundaryRequest(configuration.Boundaries[1].SortKey, "30", null, "tester"));
        Assert.False(result.IsSuccess);
        Assert.Equal("新边界值不在允许拆分的范围内。", result.Error);
        auditRepository.Verify(x => x.AddAsync(It.IsAny<PartitionAuditLog>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task MergeBoundaryAsync_ShouldFail_WhenBoundaryMissing()
    {
        var configuration = CreateMetadataConfiguration(isColumnNullable: false);
        configuration.ReplaceBoundaries(new[]
        {
            PartitionValue.FromInt(10),
            PartitionValue.FromInt(20)
        });

        configurationRepository.Setup(x => x.GetByIdAsync(configuration.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        var service = CreateService();
        var result = await service.MergeBoundaryAsync(configuration.Id, new MergePartitionBoundaryRequest("UNKNOWN", "tester"));

        Assert.False(result.IsSuccess);
        Assert.Equal("未找到指定的分区边界。", result.Error);
    }

    [Fact]
    public async Task SplitBoundaryAsync_Should_Record_Audit_On_Success()
    {
        var configuration = CreateMetadataConfiguration(isColumnNullable: false);
        configuration.ReplaceBoundaries(new[]
        {
            PartitionValue.FromInt(10),
            PartitionValue.FromInt(20)
        });

        configurationRepository.Setup(x => x.GetByIdAsync(configuration.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        var service = CreateService();
        var result = await service.SplitBoundaryAsync(configuration.Id, new SplitPartitionBoundaryRequest(configuration.Boundaries[1].SortKey, "15", "FG_SPLIT", "tester"));
        if (!result.IsSuccess)
        {
            throw new XunitException($"Expected success but got: {result.Error ?? "null"}");
        }
        configurationRepository.Verify(x => x.UpdateAsync(configuration, It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        auditRepository.Verify(x => x.AddAsync(It.Is<PartitionAuditLog>(log =>
            log.Action == PartitionExecutionOperationType.SplitBoundary.ToString() &&
            log.PayloadJson != null &&
            log.PayloadJson.Contains("FG_SPLIT", StringComparison.OrdinalIgnoreCase)), It.IsAny<CancellationToken>()), Times.Once);
    }

    private PartitionConfigurationAppService CreateService()
        => new(
            metadataRepository.Object,
            configurationRepository.Object,
            taskRepository.Object,
            logRepository.Object,
            auditRepository.Object,
            parser,
            logger.Object);

    private static PartitionConfiguration CreateMetadataConfiguration(bool isColumnNullable, PartitionValueKind valueKind = PartitionValueKind.Int)
        => new(
            Guid.NewGuid(),
            "dbo",
            "Orders",
            "pf_orders",
            "ps_orders",
            new PartitionColumn("OrderDate", valueKind, isColumnNullable),
            PartitionFilegroupStrategy.Default("PRIMARY"),
            isRangeRight: true,
            storageSettings: PartitionStorageSettings.UsePrimary("PRIMARY"));

    private CreatePartitionConfigurationRequest CreateValidRequest(bool requireNotNull = true, bool columnIsNullable = false)
        => new(
            Guid.NewGuid(),
            "dbo",
            "Orders",
            "OrderDate",
            PartitionValueKind.DateTime,
            columnIsNullable,
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
