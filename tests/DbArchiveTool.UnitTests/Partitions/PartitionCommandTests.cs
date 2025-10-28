using System;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;
using Moq;

namespace DbArchiveTool.UnitTests.Partitions;

/// <summary>
/// 验证 PartitionCommandAppService 的输入校验逻辑。
/// </summary>
public class PartitionCommandTests
{
    private readonly Mock<IPartitionMetadataRepository> metadataRepository = new();
    private readonly Mock<IPartitionCommandScriptGenerator> scriptGenerator = new();
    private readonly Mock<IBackgroundTaskRepository> backgroundTaskRepository = new();
    private readonly Mock<IBackgroundTaskLogRepository> backgroundTaskLogRepository = new();
    private readonly Mock<IPartitionAuditLogRepository> auditLogRepository = new();
    private readonly Mock<IBackgroundTaskDispatcher> backgroundTaskDispatcher = new();
    private readonly PartitionValueParser parser = new();
    private readonly Mock<ILogger<PartitionCommandAppService>> logger = new();

    [Fact]
    public async Task PreviewSplitAsync_Should_Fail_When_DataSourceId_Is_Empty()
    {
        var service = CreateService();
        var request = new SplitPartitionRequest(Guid.Empty, "dbo", "Orders", new[] { "100" }, true, "tester");

        var result = await service.PreviewSplitAsync(request);

        Assert.False(result.IsSuccess);
        Assert.Equal("数据源标识不能为空。", result.Error);
        metadataRepository.VerifyNoOtherCalls();
        scriptGenerator.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ExecuteSplitAsync_Should_Fail_When_Backup_Not_Confirmed()
    {
        var service = CreateService();
        var request = new SplitPartitionRequest(Guid.NewGuid(), "dbo", "Orders", new[] { "2024-01-01" }, false, "tester");

        var result = await service.ExecuteSplitAsync(request);

        Assert.False(result.IsSuccess);
        Assert.Equal("执行拆分前需要确认已有备份或快照。", result.Error);
        metadataRepository.VerifyNoOtherCalls();
        scriptGenerator.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ExecuteSplitAsync_Should_Create_BackgroundTask_With_InvariantPayload()
    {
        var dataSourceId = Guid.NewGuid();
        var configuration = CreateConfiguration(dataSourceId, new[]
        {
            new PartitionBoundary("0001", PartitionValue.FromDate(new DateOnly(2024, 01, 01)))
        });

        metadataRepository
            .SetupSequence(x => x.GetConfigurationAsync(dataSourceId, "dbo", "Orders", It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration)
            .ReturnsAsync(configuration);

        scriptGenerator
            .Setup(x => x.GenerateSplitScript(
                It.IsAny<PartitionConfiguration>(), 
                It.IsAny<IReadOnlyList<PartitionValue>>(),
                It.IsAny<string?>()))
            .Returns(Result<string>.Success("SPLIT SCRIPT"));

        BackgroundTask? capturedTask = null;
        backgroundTaskRepository
            .Setup(x => x.AddAsync(It.IsAny<BackgroundTask>(), It.IsAny<CancellationToken>()))
            .Callback<BackgroundTask, CancellationToken>((task, _) => capturedTask = task)
            .Returns(Task.CompletedTask);

        backgroundTaskLogRepository
            .Setup(x => x.AddAsync(It.IsAny<BackgroundTaskLogEntry>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        auditLogRepository
            .Setup(x => x.AddAsync(It.IsAny<PartitionAuditLog>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        backgroundTaskDispatcher
            .Setup(x => x.DispatchAsync(It.IsAny<Guid>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var service = CreateService();
        var request = new SplitPartitionRequest(dataSourceId, "dbo", "Orders", new[] { "2024-05-01" }, true, "tester");

        var result = await service.ExecuteSplitAsync(request);

        Assert.True(result.IsSuccess);
        Assert.NotNull(capturedTask);
        Assert.Equal(dataSourceId, capturedTask!.DataSourceId);
        Assert.Equal(BackgroundTaskOperationType.SplitBoundary, capturedTask.OperationType);
        Assert.Contains("\"2024-05-01\"", capturedTask.ConfigurationSnapshot ?? string.Empty);
    }

    [Fact]
    public async Task PreviewMergeAsync_Should_Fail_When_Boundary_Not_Found()
    {
        var dataSourceId = Guid.NewGuid();
        var configuration = CreateConfiguration(dataSourceId, new[]
        {
            new PartitionBoundary("0001", PartitionValue.FromDate(new DateOnly(2024, 01, 01)))
        });

        metadataRepository
            .Setup(x => x.GetConfigurationAsync(dataSourceId, "dbo", "Orders", It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        var service = CreateService();
        var request = new MergePartitionRequest(dataSourceId, "dbo", "Orders", "9999", true, "tester");

        var result = await service.PreviewMergeAsync(request);

        Assert.False(result.IsSuccess);
        Assert.Equal("未找到分区边界 9999，请刷新后重试。", result.Error);
        scriptGenerator.Verify(x => x.GenerateMergeScript(It.IsAny<PartitionConfiguration>(), It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task ExecuteMergeAsync_Should_Create_BackgroundTask()
    {
        var dataSourceId = Guid.NewGuid();
        var configuration = CreateConfiguration(dataSourceId, new[]
        {
            new PartitionBoundary("0001", PartitionValue.FromDate(new DateOnly(2024, 01, 01)))
        });

        metadataRepository
            .SetupSequence(x => x.GetConfigurationAsync(dataSourceId, "dbo", "Orders", It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration)  // PreviewMergeAsync
            .ReturnsAsync(configuration); // ExecuteMergeAsync

        scriptGenerator
            .Setup(x => x.GenerateMergeScript(configuration, "0001"))
            .Returns(Result<string>.Success("MERGE SCRIPT"));

        BackgroundTask? capturedTask = null;
        backgroundTaskRepository
            .Setup(x => x.AddAsync(It.IsAny<BackgroundTask>(), It.IsAny<CancellationToken>()))
            .Callback<BackgroundTask, CancellationToken>((task, _) => capturedTask = task)
            .Returns(Task.CompletedTask);

        backgroundTaskLogRepository
            .Setup(x => x.AddAsync(It.IsAny<BackgroundTaskLogEntry>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        auditLogRepository
            .Setup(x => x.AddAsync(It.IsAny<PartitionAuditLog>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        backgroundTaskDispatcher
            .Setup(x => x.DispatchAsync(It.IsAny<Guid>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var service = CreateService();
        var request = new MergePartitionRequest(dataSourceId, "dbo", "Orders", "0001", true, "tester");

        var result = await service.ExecuteMergeAsync(request);

        Assert.True(result.IsSuccess);
        Assert.NotNull(capturedTask);
        Assert.Equal(dataSourceId, capturedTask!.DataSourceId);
        Assert.Equal(BackgroundTaskOperationType.MergeBoundary, capturedTask.OperationType);
        Assert.Contains("\"BoundaryKey\":\"0001\"", capturedTask.ConfigurationSnapshot ?? string.Empty);
    }

    [Fact]
    public async Task PreviewSwitchAsync_Should_Generate_Script()
    {
        var dataSourceId = Guid.NewGuid();
        var configuration = CreateConfiguration(dataSourceId);

        metadataRepository
            .Setup(x => x.GetConfigurationAsync(dataSourceId, "dbo", "Orders", It.IsAny<CancellationToken>()))
            .ReturnsAsync(configuration);

        SwitchPayload? capturedPayload = null;
        scriptGenerator
            .Setup(x => x.GenerateSwitchOutScript(configuration, It.IsAny<SwitchPayload>()))
            .Callback<PartitionConfiguration, SwitchPayload>((_, payload) => capturedPayload = payload)
            .Returns(Result<string>.Success("SWITCH SCRIPT"));

    var service = CreateService();
    var request = new SwitchPartitionRequest(dataSourceId, "dbo", "Orders", "5", "archive.SwitchTarget", "ArchiveDb", false, true, "tester");

        var result = await service.PreviewSwitchAsync(request);

        Assert.True(result.IsSuccess);
        Assert.Equal("SWITCH SCRIPT", result.Value!.Script);
        Assert.NotNull(capturedPayload);
        Assert.Equal("archive", capturedPayload!.TargetSchema);
        Assert.Equal("SwitchTarget", capturedPayload.TargetTable);
    Assert.Equal("ArchiveDb", capturedPayload.TargetDatabase);
    }

    private static PartitionConfiguration CreateConfiguration(Guid dataSourceId, IEnumerable<PartitionBoundary>? boundaries = null)
    {
        var column = new PartitionColumn("OrderDate", PartitionValueKind.Date, isNullable: false);
        var strategy = PartitionFilegroupStrategy.Default("PRIMARY");

        return new PartitionConfiguration(
            dataSourceId,
            "dbo",
            "Orders",
            "PF_Orders",
            "PS_Orders",
            column,
            strategy,
            isRangeRight: true,
            existingBoundaries: boundaries);
    }

    private PartitionCommandAppService CreateService()
        => new(
            metadataRepository.Object,
            scriptGenerator.Object,
            parser,
            logger.Object,
            backgroundTaskRepository.Object,
            backgroundTaskLogRepository.Object,
            auditLogRepository.Object,
            backgroundTaskDispatcher.Object);
}
