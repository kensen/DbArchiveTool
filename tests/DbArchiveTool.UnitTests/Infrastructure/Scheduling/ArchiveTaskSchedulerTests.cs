using System.Linq.Expressions;
using DbArchiveTool.Application.Archives;
using DbArchiveTool.Domain.ArchiveConfigurations;
using DbArchiveTool.Infrastructure.Scheduling;
using DbArchiveTool.Shared.Archive;
using Hangfire;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace DbArchiveTool.UnitTests.Infrastructure.Scheduling;

/// <summary>
/// 验证 ArchiveTaskScheduler 在不同配置状态下的行为是否符合预期。
/// </summary>
public class ArchiveTaskSchedulerTests
{
    /// <summary>
    /// ArchiveConfiguration 定时归档功能已移除,SyncRecurringJobAsync 只会调用 RemoveIfExists 清理旧任务。
    /// </summary>
    [Fact]
    public async Task SyncRecurringJobAsync_ShouldRemoveOldJob_ForArchiveConfiguration()
    {
        var recurringJobManagerMock = new Mock<IRecurringJobManager>();
        var scheduler = new ArchiveTaskScheduler(recurringJobManagerMock.Object, NullLogger<ArchiveTaskScheduler>.Instance);
        var configuration = CreateConfiguration(enableScheduledArchive: true);

        await scheduler.SyncRecurringJobAsync(configuration, CancellationToken.None);

        var expectedJobId = $"archive-config-{configuration.Id:N}";
        
        // 只应调用 RemoveIfExists 清理旧任务
        recurringJobManagerMock.Verify(m => m.RemoveIfExists(expectedJobId), Times.Once);
        // 不应注册新任务
        recurringJobManagerMock.Verify(m => m.AddOrUpdate(
            It.IsAny<string>(),
            It.IsAny<Hangfire.Common.Job>(),
            It.IsAny<string>(),
            It.IsAny<RecurringJobOptions>()), Times.Never);
    }

    /// <summary>
    /// 移除周期任务时应调用 Hangfire 的 RemoveIfExists。
    /// </summary>
    [Fact]
    public async Task RemoveRecurringJobAsync_ShouldRemoveJob()
    {
        var recurringJobManagerMock = new Mock<IRecurringJobManager>();
    var scheduler = new ArchiveTaskScheduler(recurringJobManagerMock.Object, NullLogger<ArchiveTaskScheduler>.Instance);
        var configuration = CreateConfiguration(enableScheduledArchive: true);

        await scheduler.RemoveRecurringJobAsync(configuration.Id, CancellationToken.None);

        var expectedJobId = $"archive-config-{configuration.Id:N}";
        recurringJobManagerMock.Verify(m => m.RemoveIfExists(expectedJobId), Times.Once);
    }

    private static ArchiveConfiguration CreateConfiguration(bool enableScheduledArchive)
    {
        return new ArchiveConfiguration(
            "订单表归档",
            "测试用配置",
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
    }
}
