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
    /// 当配置启用且开启定时归档时,应向 Hangfire 注册或更新周期任务。
    /// </summary>
    [Fact]
    public async Task SyncRecurringJobAsync_ShouldAddOrUpdateJob_WhenConfigurationEnabled()
    {
        var recurringJobManagerMock = new Mock<IRecurringJobManager>();
    var scheduler = new ArchiveTaskScheduler(recurringJobManagerMock.Object, NullLogger<ArchiveTaskScheduler>.Instance);
        var configuration = CreateConfiguration(enableScheduledArchive: true);

        await scheduler.SyncRecurringJobAsync(configuration, CancellationToken.None);

        var expectedJobId = $"archive-config-{configuration.Id:N}";

        var addInvocation = recurringJobManagerMock.Invocations.Single(invocation => invocation.Method.Name == nameof(IRecurringJobManager.AddOrUpdate));
        Assert.Equal(expectedJobId, addInvocation.Arguments[0]);
        Assert.Equal(configuration.CronExpression, addInvocation.Arguments[2]);
        var options = Assert.IsType<RecurringJobOptions>(addInvocation.Arguments[3]);
        Assert.Equal(TimeZoneInfo.Utc, options.TimeZone);

        Assert.DoesNotContain(recurringJobManagerMock.Invocations, inv => inv.Method.Name == nameof(IRecurringJobManager.RemoveIfExists));
    }

    /// <summary>
    /// 当配置被禁用时,应移除已有的周期任务。
    /// </summary>
    [Fact]
    public async Task SyncRecurringJobAsync_ShouldRemoveJob_WhenConfigurationDisabled()
    {
        var recurringJobManagerMock = new Mock<IRecurringJobManager>();
    var scheduler = new ArchiveTaskScheduler(recurringJobManagerMock.Object, NullLogger<ArchiveTaskScheduler>.Instance);
        var configuration = CreateConfiguration(enableScheduledArchive: true);
        configuration.Disable();

        await scheduler.SyncRecurringJobAsync(configuration, CancellationToken.None);

        var expectedJobId = $"archive-config-{configuration.Id:N}";

        recurringJobManagerMock.Verify(m => m.RemoveIfExists(expectedJobId), Times.Once);
        Assert.DoesNotContain(recurringJobManagerMock.Invocations, inv => inv.Method.Name == nameof(IRecurringJobManager.AddOrUpdate));
    }

    /// <summary>
    /// 当配置未开启定时归档时,应移除对应的周期任务。
    /// </summary>
    [Fact]
    public async Task SyncRecurringJobAsync_ShouldRemoveJob_WhenScheduledDisabled()
    {
        var recurringJobManagerMock = new Mock<IRecurringJobManager>();
    var scheduler = new ArchiveTaskScheduler(recurringJobManagerMock.Object, NullLogger<ArchiveTaskScheduler>.Instance);
        var configuration = CreateConfiguration(enableScheduledArchive: false);

        await scheduler.SyncRecurringJobAsync(configuration, CancellationToken.None);

        var expectedJobId = $"archive-config-{configuration.Id:N}";
        recurringJobManagerMock.Verify(m => m.RemoveIfExists(expectedJobId), Times.Once);
        Assert.DoesNotContain(recurringJobManagerMock.Invocations, inv => inv.Method.Name == nameof(IRecurringJobManager.AddOrUpdate));
    }

    /// <summary>
    /// 当 Hangfire 抛出异常时,应将异常向上抛出供上层处理。
    /// </summary>
    [Fact]
    public async Task SyncRecurringJobAsync_ShouldThrow_WhenRecurringJobManagerFails()
    {
        var recurringJobManagerMock = new Mock<IRecurringJobManager>();
    var scheduler = new ArchiveTaskScheduler(recurringJobManagerMock.Object, NullLogger<ArchiveTaskScheduler>.Instance);
        var configuration = CreateConfiguration(enableScheduledArchive: true);

        recurringJobManagerMock
            .Setup(m => m.AddOrUpdate(
                It.IsAny<string>(),
                It.IsAny<Hangfire.Common.Job>(),
                It.IsAny<string>(),
                It.IsAny<RecurringJobOptions>()))
            .Throws(new InvalidOperationException("注册失败"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => scheduler.SyncRecurringJobAsync(configuration, CancellationToken.None));
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
        var cron = enableScheduledArchive ? "0 2 * * *" : null;
        DateTime? nextUtc = enableScheduledArchive ? DateTime.UtcNow.AddHours(2) : null;

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
            "Orders_Archive",
            enableScheduledArchive,
            cron,
            nextUtc);
    }
}
