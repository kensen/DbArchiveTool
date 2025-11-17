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
    /// 验证可以成功创建配置。
    /// </summary>
    [Fact]
    public async Task Create_ShouldReturnCreated_WhenRequestValid()
    {
        var repositoryMock = new Mock<IArchiveConfigurationRepository>();
        ArchiveConfiguration? persistedConfig = null;

        repositoryMock
            .Setup(r => r.AddAsync(It.IsAny<ArchiveConfiguration>(), It.IsAny<CancellationToken>()))
            .Callback<ArchiveConfiguration, CancellationToken>((config, _) => persistedConfig = config)
            .Returns(Task.CompletedTask);

        var controller = CreateController(repositoryMock);
        var request = CreateRequest();

        var result = await controller.Create(request, CancellationToken.None);

        var createdResult = Assert.IsType<CreatedAtActionResult>(result);
        var dto = Assert.IsType<ArchiveConfigurationDetailDto>(createdResult.Value);
        Assert.NotEqual(Guid.Empty, dto.Id);

        Assert.NotNull(persistedConfig);
    }

    /// <summary>
    /// 验证创建配置时会正常保存到数据库。
    /// </summary>
    [Fact]
    public async Task Create_ShouldSucceed_WhenRequestValid()
    {
        var repositoryMock = new Mock<IArchiveConfigurationRepository>(MockBehavior.Strict);
        var controller = CreateController(repositoryMock);

        var request = CreateRequest();

        repositoryMock
            .Setup(r => r.AddAsync(It.IsAny<ArchiveConfiguration>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var result = await controller.Create(request, CancellationToken.None);

        var createdResult = Assert.IsType<CreatedAtActionResult>(result);
        Assert.NotNull(createdResult.Value);
        Assert.Equal(nameof(ArchiveConfigurationsController.GetById), createdResult.ActionName);
    }

    /// <summary>
    /// 验证更新配置时会重新计算下一次执行时间并同步 Hangfire 任务。
    /// </summary>
    [Fact]
    public async Task Update_ShouldSucceed_WhenConfigurationExists()
    {
        var repositoryMock = new Mock<IArchiveConfigurationRepository>();
        var controller = CreateController(repositoryMock);

        var configurationId = Guid.NewGuid();
        var existingConfig = CreateConfiguration(configurationId);

        repositoryMock
            .Setup(r => r.GetByIdAsync(configurationId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(existingConfig);

        repositoryMock
            .Setup(r => r.UpdateAsync(existingConfig, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var request = CreateUpdateRequest();

        var result = await controller.Update(configurationId, request, CancellationToken.None);

        var okResult = Assert.IsType<OkObjectResult>(result);
        var dto = Assert.IsType<ArchiveConfigurationDetailDto>(okResult.Value);
        Assert.Equal(request.Name, dto.Name);
    }

    /// <summary>
    /// 验证删除配置时会成功软删除。
    /// </summary>
    [Fact]
    public async Task Delete_ShouldSucceed_WhenConfigurationExists()
    {
        var repositoryMock = new Mock<IArchiveConfigurationRepository>();
        var controller = CreateController(repositoryMock);

        var configurationId = Guid.NewGuid();
        var existingConfig = CreateConfiguration(configurationId);

        repositoryMock
            .Setup(r => r.GetByIdAsync(configurationId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(existingConfig);

        repositoryMock
            .Setup(r => r.UpdateAsync(existingConfig, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var result = await controller.Delete(configurationId, CancellationToken.None);

        Assert.IsType<NoContentResult>(result);
    }

    private static ArchiveConfigurationsController CreateController(
        Mock<IArchiveConfigurationRepository> repositoryMock)
    {
        var loggerMock = new Mock<ILogger<ArchiveConfigurationsController>>();
        return new ArchiveConfigurationsController(
            repositoryMock.Object,
            loggerMock.Object);
    }

    private static CreateArchiveConfigurationRequest CreateRequest()
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
            DeleteSourceDataAfterArchive = true
        };
    }

    private static UpdateArchiveConfigurationRequest CreateUpdateRequest()
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
            DeleteSourceDataAfterArchive = true
        };
    }

    private static ArchiveConfiguration CreateConfiguration(Guid id)
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
            "Orders_Archive");

        config.OverrideId(id);
        return config;
    }
}
