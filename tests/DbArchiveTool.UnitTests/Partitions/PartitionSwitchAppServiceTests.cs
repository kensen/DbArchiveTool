using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DbArchiveTool.UnitTests.Partitions;

public class PartitionSwitchAppServiceTests
{
    private readonly Mock<IPartitionConfigurationRepository> configurationRepository = new();
    private readonly Mock<IPartitionSwitchInspectionService> inspectionService = new();
    private readonly Mock<IPartitionCommandAppService> commandAppService = new();
    private readonly Mock<IPartitionAuditLogRepository> auditRepository = new();
    private readonly Mock<ILogger<PartitionSwitchAppService>> logger = new();

    [Fact]
    public async Task InspectAsync_Should_Fail_When_Configuration_NotFound()
    {
        configurationRepository
            .Setup(x => x.GetByIdAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((PartitionConfiguration?)null);

        var service = CreateService();
        var result = await service.InspectAsync(new SwitchPartitionInspectionRequest(Guid.NewGuid(), "5", "Archive.Target", false, "tester"));

        Assert.False(result.IsSuccess);
        Assert.Equal("未找到指定的分区配置。", result.Error);
    }

    [Fact]
    public async Task ArchiveBySwitchAsync_Should_Create_Command_And_Audit()
    {
        var configurationId = Guid.NewGuid();
        var configuration = CreateConfiguration(configurationId);

        configurationRepository
            .Setup(x => x.GetByIdAsync(configurationId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        inspectionService
            .Setup(x => x.InspectAsync(configuration.ArchiveDataSourceId, configuration, It.IsAny<PartitionSwitchInspectionContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(CreateInspectionResult(canSwitch: true));

        commandAppService
            .Setup(x => x.PreviewSwitchAsync(It.IsAny<SwitchPartitionRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<PartitionCommandPreviewDto>.Success(new PartitionCommandPreviewDto("SWITCH SCRIPT", Array.Empty<string>())));

        commandAppService
            .Setup(x => x.ExecuteSwitchAsync(It.IsAny<SwitchPartitionRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<Guid>.Success(Guid.NewGuid()));

        var service = CreateService();
        var request = new SwitchPartitionExecuteRequest(configurationId, "3", "Archive.Target", false, true, "tester");

        var result = await service.ArchiveBySwitchAsync(request);

        Assert.True(result.IsSuccess);
        auditRepository.Verify(x => x.AddAsync(It.IsAny<PartitionAuditLog>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ArchiveBySwitchAsync_Should_Fail_When_Inspection_Blocks()
    {
        var configurationId = Guid.NewGuid();
        var configuration = CreateConfiguration(configurationId);

        configurationRepository
            .Setup(x => x.GetByIdAsync(configurationId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        var issues = new List<PartitionSwitchIssue>
        {
            new("TargetNotEmpty", "目标表仍包含数据。", null)
        };

        inspectionService
            .Setup(x => x.InspectAsync(configuration.ArchiveDataSourceId, configuration, It.IsAny<PartitionSwitchInspectionContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PartitionSwitchInspectionResult(false, issues, Array.Empty<PartitionSwitchIssue>(), CreateTableInfo(configuration.SchemaName, configuration.TableName), CreateTableInfo("Archive", "Target")));

        var service = CreateService();
        var request = new SwitchPartitionExecuteRequest(configurationId, "3", "Archive.Target", false, true, "tester");

        var result = await service.ArchiveBySwitchAsync(request);

        Assert.False(result.IsSuccess);
        Assert.Equal("目标表仍包含数据。", result.Error);
        commandAppService.Verify(x => x.ExecuteSwitchAsync(It.IsAny<SwitchPartitionRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private PartitionSwitchAppService CreateService()
        => new(configurationRepository.Object, inspectionService.Object, commandAppService.Object, auditRepository.Object, logger.Object);

    private static PartitionSwitchInspectionResult CreateInspectionResult(bool canSwitch)
        => new(
            canSwitch,
            new List<PartitionSwitchIssue>(),
            new List<PartitionSwitchIssue>(),
            CreateTableInfo("dbo", "Orders"),
            CreateTableInfo("Archive", "Orders_Target"));

    private static PartitionSwitchTableInfo CreateTableInfo(string schema, string table)
        => new(schema, table, 0, new List<PartitionSwitchColumnInfo>
        {
            new("Id", "int", null, 10, 0, false, true, false),
            new("OrderDate", "datetime", null, null, null, false, false, false)
        });

    private static PartitionConfiguration CreateConfiguration(Guid configurationId)
    {
        var column = new PartitionColumn("OrderDate", PartitionValueKind.DateTime, false);
        var strategy = PartitionFilegroupStrategy.Default("PRIMARY");
        var boundaries = new List<PartitionBoundary>
        {
            new("0001", PartitionValue.FromDate(new DateOnly(2024, 1, 1)))
        };

        var configuration = new PartitionConfiguration(
            Guid.NewGuid(),
            "dbo",
            "Orders",
            "PF_Orders",
            "PS_Orders",
            column,
            strategy,
            isRangeRight: true,
            existingBoundaries: boundaries);

        configuration.RestoreAudit(DateTime.UtcNow, "tester", DateTime.UtcNow, "tester", false);
        configuration.OverrideId(configurationId);
        return configuration;
    }
}
