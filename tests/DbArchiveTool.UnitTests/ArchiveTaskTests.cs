using DbArchiveTool.Domain.ArchiveTasks;

namespace DbArchiveTool.UnitTests;

public class ArchiveTaskTests
{
    [Fact]
    public void MarkCompleted_ShouldUpdateStatusAndTimestamps()
    {
        var task = new ArchiveTask(Guid.NewGuid(), "Source", "Target", false);

        task.MarkRunning();
        task.MarkCompleted(10, 8);

        Assert.Equal(ArchiveTaskStatus.Succeeded, task.Status);
        Assert.NotNull(task.CompletedAtUtc);
        Assert.Equal(10, task.SourceRowCount);
        Assert.Equal(8, task.TargetRowCount);
    }
}
