using DbArchiveTool.Api.Controllers.V1;
using DbArchiveTool.Api.DTOs.Archives;
using DbArchiveTool.Application.Archives;
using DbArchiveTool.Domain.ArchiveConfigurations;
using DbArchiveTool.Shared.Archive;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;

namespace DbArchiveTool.UnitTests.Api;

/// <summary>
/// 针对归档配置控制器的调度分支与输入验证策略进行单元测试。
/// </summary>
public class ArchiveConfigurationsControllerTests
{
    /// <summary>
    /// 验证启用定时归档时可以成功创建配置并同步 Hangfire 任务。
    /// </summary>
    [Fact]
    public async Task Create_ShouldReturnCreatedAndSyncScheduler_WhenScheduledEnabled()
    {
        var repositoryMock = new Mock<IArchiveConfigurationRepository>();
        var schedulerMock = new Mock<IArchiveTaskScheduler>();
        ArchiveConfiguration? persistedConfig = null;

        repositoryMock
            .Setup(r => r.AddAsync(It.IsAny<ArchiveConfiguration>(), It.IsAny<CancellationToken>()))
            .Callback<ArchiveConfiguration, CancellationToken>((config, _) => persistedConfig = config)
            .Returns(Task.CompletedTask);

        var controller = CreateController(repositoryMock, schedulerMock);
        var request = CreateRequest(enableScheduledArchive: true);

        var result = await controller.Create(request, CancellationToken.None);

        var createdResult = Assert.IsType<CreatedAtActionResult>(result);
        var dto = Assert.IsType<ArchiveConfigurationDetailDto>(createdResult.Value);
        Assert.NotEqual(Guid.Empty, dto.Id);
        Assert.True(dto.EnableScheduledArchive);
        Assert.NotNull(dto.NextArchiveAtUtc);

        Assert.NotNull(persistedConfig);
        Assert.True(persistedConfig!.EnableScheduledArchive);
        Assert.NotNull(persistedConfig.NextArchiveAtUtc);

        schedulerMock.Verify(
            s => s.SyncRecurringJobAsync(
                It.Is<ArchiveConfiguration>(cfg => cfg == persistedConfig),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    /// <summary>
    /// 验证当 Cron 表达式无效时会返回 400,且不会落库或调用调度器。
    /// </summary>
    [Fact]
    public async Task Create_ShouldReturnBadRequest_WhenCronInvalid()
    {
        var repositoryMock = new Mock<IArchiveConfigurationRepository>(MockBehavior.Strict);
        var schedulerMock = new Mock<IArchiveTaskScheduler>(MockBehavior.Strict);
        var controller = CreateController(repositoryMock, schedulerMock);

        var request = CreateRequest(enableScheduledArchive: true);
        request.CronExpression = "invalid-cron";

        var result = await controller.Create(request, CancellationToken.None);

        var badRequest = Assert.IsType<BadRequestObjectResult>(result);
        Assert.Contains("Cron", badRequest.Value!.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// 验证更新配置时会重新计算下一次执行时间并同步 Hangfire 任务。
    /// </summary>
    [Fact]
    public async Task Update_ShouldResyncScheduler_WhenConfigurationExists()
    {
        var repositoryMock = new Mock<IArchiveConfigurationRepository>();
        var schedulerMock = new Mock<IArchiveTaskScheduler>();
        var controller = CreateController(repositoryMock, schedulerMock);

        var configurationId = Guid.NewGuid();
        var existingConfig = CreateConfiguration(configurationId, enableScheduledArchive: false);

        repositoryMock
            .Setup(r => r.GetByIdAsync(configurationId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(existingConfig);

        repositoryMock
            .Setup(r => r.UpdateAsync(existingConfig, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var request = CreateUpdateRequest(enableScheduledArchive: true);

        var result = await controller.Update(configurationId, request, CancellationToken.None);

        var okResult = Assert.IsType<OkObjectResult>(result);
        var dto = Assert.IsType<ArchiveConfigurationDetailDto>(okResult.Value);
        Assert.True(dto.EnableScheduledArchive);
        Assert.Equal(request.CronExpression, dto.CronExpression);

        Assert.True(existingConfig.EnableScheduledArchive);
        Assert.Equal(request.CronExpression, existingConfig.CronExpression);
        Assert.NotNull(existingConfig.NextArchiveAtUtc);

        schedulerMock.Verify(
            s => s.SyncRecurringJobAsync(existingConfig, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    /// <summary>
    /// 验证禁用配置时会通知调度器移除定时任务。
    /// </summary>
    [Fact]
    public async Task Disable_ShouldSyncScheduler_WhenCalled()
    {
        var repositoryMock = new Mock<IArchiveConfigurationRepository>();
        var schedulerMock = new Mock<IArchiveTaskScheduler>();
        var controller = CreateController(repositoryMock, schedulerMock);

        var configurationId = Guid.NewGuid();
        var existingConfig = CreateConfiguration(configurationId, enableScheduledArchive: true);

        repositoryMock
            .Setup(r => r.GetByIdAsync(configurationId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(existingConfig);

        repositoryMock
            .Setup(r => r.UpdateAsync(existingConfig, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var result = await controller.Disable(configurationId, CancellationToken.None);

        var okResult = Assert.IsType<OkObjectResult>(result);
        Assert.Contains("禁用", okResult.Value!.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.False(existingConfig.IsEnabled);

        schedulerMock.Verify(
            s => s.SyncRecurringJobAsync(existingConfig, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    /// <summary>
    /// 验证删除配置时会调用调度器清理定时任务。
    /// </summary>
    [Fact]
    public async Task Delete_ShouldRemoveScheduler_WhenConfigurationExists()
    {
        var repositoryMock = new Mock<IArchiveConfigurationRepository>();
        var schedulerMock = new Mock<IArchiveTaskScheduler>();
        var controller = CreateController(repositoryMock, schedulerMock);

        var configurationId = Guid.NewGuid();
        var existingConfig = CreateConfiguration(configurationId, enableScheduledArchive: true);

        repositoryMock
            .Setup(r => r.GetByIdAsync(configurationId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(existingConfig);

        repositoryMock
            .Setup(r => r.UpdateAsync(existingConfig, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var result = await controller.Delete(configurationId, CancellationToken.None);

        Assert.IsType<NoContentResult>(result);

        schedulerMock.Verify(
            s => s.RemoveRecurringJobAsync(configurationId, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    private static ArchiveConfigurationsController CreateController(
        Mock<IArchiveConfigurationRepository> repositoryMock,
        Mock<IArchiveTaskScheduler> schedulerMock)
    {
        var loggerMock = new Mock<ILogger<ArchiveConfigurationsController>>();
        return new ArchiveConfigurationsController(
            repositoryMock.Object,
            schedulerMock.Object,
            loggerMock.Object);
    }

    private static CreateArchiveConfigurationRequest CreateRequest(bool enableScheduledArchive)
    {
        return new CreateArchiveConfigurationRequest
        {
            Name = "订单表归档",
            Description = "测试配置",
            DataSourceId = Guid.NewGuid(),
            SourceSchemaName = "dbo",
            SourceTableName = "Orders",
            TargetSchemaName = "archive",
            TargetTableName = "Orders_Archive",
            IsPartitionedTable = false,
            ArchiveMethod = ArchiveMethod.Bcp,
            ArchiveFilterColumn = "CreateDate",
            ArchiveFilterCondition = "< DATEADD(day, -30, GETDATE())",
            DeleteSourceDataAfterArchive = true,
            EnableScheduledArchive = enableScheduledArchive,
            CronExpression = enableScheduledArchive ? "0 2 * * *" : null
        };
    }

    private static UpdateArchiveConfigurationRequest CreateUpdateRequest(bool enableScheduledArchive)
    {
        return new UpdateArchiveConfigurationRequest
        {
            Name = "订单表归档",
            Description = "更新配置",
            DataSourceId = Guid.NewGuid(),
            SourceSchemaName = "dbo",
            SourceTableName = "Orders",
            TargetSchemaName = "archive",
            TargetTableName = "Orders_Archive",
            IsPartitionedTable = false,
            ArchiveMethod = ArchiveMethod.Bcp,
            ArchiveFilterColumn = "CreateDate",
            ArchiveFilterCondition = "< DATEADD(day, -60, GETDATE())",
            DeleteSourceDataAfterArchive = true,
            EnableScheduledArchive = enableScheduledArchive,
            CronExpression = enableScheduledArchive ? "*/30 * * * *" : null
        };
    }

    private static ArchiveConfiguration CreateConfiguration(Guid id, bool enableScheduledArchive)
    {
        var config = new ArchiveConfiguration(
            "订单表归档",
            "演示配置",
            Guid.NewGuid(),
            "dbo",
            "Orders",
            false,
            "CreateDate",
            "< DATEADD(day, -30, GETDATE())",
            ArchiveMethod.Bcp,
            true,
            10000,
            null,
            "archive",
            "Orders_Archive",
            enableScheduledArchive,
            enableScheduledArchive ? "0 2 * * *" : null,
            enableScheduledArchive ? DateTime.UtcNow.AddHours(2) : null);

        config.OverrideId(id);
        return config;
    }
}
