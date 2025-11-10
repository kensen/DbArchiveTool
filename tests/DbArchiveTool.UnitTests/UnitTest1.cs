using DbArchiveTool.Domain.ArchiveTasks;

namespace DbArchiveTool.UnitTests;

/// <summary>
/// 验证归档任务在失败时会正确更新状态并发布领域事件。
/// </summary>
public class ArchiveTaskFailureTests
{
    /// <summary>
    /// 归档任务标记失败时应更新状态、时间并附加失败领域事件。
    /// </summary>
    [Fact]
    public void MarkFailed_ShouldSetStatusAndRaiseDomainEvent()
    {
        var task = new ArchiveTask(Guid.NewGuid(), "Source", "Target", false);

        task.MarkFailed("网络故障");

        Assert.Equal(ArchiveTaskStatus.Failed, task.Status);
        Assert.NotNull(task.CompletedAtUtc);
        Assert.Contains(task.DomainEvents, evt => evt is ArchiveTaskFailedEvent failed && failed.FailureReason == "网络故障");
    }
}
