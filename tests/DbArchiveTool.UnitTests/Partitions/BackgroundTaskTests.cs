using System;
using DbArchiveTool.Domain.Partitions;
using Xunit;

namespace DbArchiveTool.UnitTests.Partitions;

/// <summary>
/// BackgroundTask 领域模型单元测试
/// </summary>
public class BackgroundTaskTests
{
    private const string TestUser = "test-user";
    private static readonly Guid TestConfigId = Guid.NewGuid();
    private static readonly Guid TestDataSourceId = Guid.NewGuid();

    #region 工厂方法与初始化测试

    [Fact]
    public void Create_ShouldSucceed_WhenParametersValid()
    {
        // Act
        var task = BackgroundTask.Create(
            TestConfigId,
            TestDataSourceId,
            "request-user",
            TestUser,
            backupReference: "backup-20251011",
            notes: "测试任务",
            priority: 5);

        // Assert
        Assert.NotEqual(Guid.Empty, task.Id);
        Assert.Equal(TestConfigId, task.PartitionConfigurationId);
        Assert.Equal(TestDataSourceId, task.DataSourceId);
        Assert.Equal("request-user", task.RequestedBy);
        Assert.Equal("backup-20251011", task.BackupReference);
        Assert.Equal("测试任务", task.Notes);
        Assert.Equal(5, task.Priority);
        Assert.Equal(BackgroundTaskStatus.PendingValidation, task.Status);
        Assert.Equal(BackgroundTaskPhases.PendingValidation, task.Phase);
        Assert.Equal(0d, task.Progress);
        Assert.False(task.IsCompleted);
        Assert.Null(task.QueuedAtUtc);
        Assert.Null(task.StartedAtUtc);
        Assert.Null(task.CompletedAtUtc);
        Assert.Null(task.FailureReason);
    }

    [Fact]
    public void Create_ShouldSucceed_WhenPartitionConfigurationIdEmpty()
    {
        // 非分区归档场景(如BCP/BulkCopy)允许 partitionConfigurationId 为空
        // Act
        var task = BackgroundTask.Create(null, TestDataSourceId, TestUser, TestUser);

        // Assert
        Assert.Null(task.PartitionConfigurationId);
        Assert.Equal(TestDataSourceId, task.DataSourceId);
    }

    [Fact]
    public void Create_ShouldFail_WhenDataSourceIdEmpty()
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() =>
            BackgroundTask.Create(TestConfigId, Guid.Empty, TestUser, TestUser));

        Assert.Contains("标识符不能为空", ex.Message);
    }

    [Fact]
    public void Create_ShouldFail_WhenRequestedByEmpty()
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() =>
            BackgroundTask.Create(TestConfigId, TestDataSourceId, "", TestUser));

        Assert.Contains("参数不能为空", ex.Message);
    }

    #endregion

    #region 状态流转测试 - 正常路径

    [Fact]
    public void StatusFlow_ShouldSucceed_WhenFollowingNormalPath()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);

        // Act & Assert: PendingValidation → Validating
        task.MarkValidating(TestUser);
        Assert.Equal(BackgroundTaskStatus.Validating, task.Status);

        // Act & Assert: Validating → Queued
        task.MarkQueued(TestUser);
        Assert.Equal(BackgroundTaskStatus.Queued, task.Status);
        Assert.NotNull(task.QueuedAtUtc);

        // Act & Assert: Queued → Running
        task.MarkRunning(TestUser);
        Assert.Equal(BackgroundTaskStatus.Running, task.Status);
        Assert.Equal(BackgroundTaskPhases.Executing, task.Phase);
        Assert.NotNull(task.StartedAtUtc);

        // Act & Assert: Running → Succeeded
        task.MarkSucceeded(TestUser, summaryJson: "{\"total\": 100}");
        Assert.Equal(BackgroundTaskStatus.Succeeded, task.Status);
        Assert.Equal(1d, task.Progress);
        Assert.NotNull(task.CompletedAtUtc);
        Assert.Equal("{\"total\": 100}", task.SummaryJson);
        Assert.Null(task.FailureReason);
        Assert.True(task.IsCompleted);
    }

    [Fact]
    public void StatusFlow_ShouldSucceed_WhenDirectQueueFromPending()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);

        // Act: PendingValidation → Queued (跳过 Validating)
        task.MarkQueued(TestUser);

        // Assert
        Assert.Equal(BackgroundTaskStatus.Queued, task.Status);
        Assert.NotNull(task.QueuedAtUtc);
    }

    #endregion

    #region 状态流转测试 - 失败路径

    [Fact]
    public void MarkFailed_ShouldSucceed_WhenRunning()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);

        // Act
        task.MarkFailed(TestUser, "SQL 执行失败", summaryJson: "{\"error\": \"timeout\"}");

        // Assert
        Assert.Equal(BackgroundTaskStatus.Failed, task.Status);
        Assert.Equal("SQL 执行失败", task.FailureReason);
        Assert.Equal("{\"error\": \"timeout\"}", task.SummaryJson);
        Assert.NotNull(task.CompletedAtUtc);
        Assert.True(task.IsCompleted);
    }

    [Fact]
    public void MarkFailed_ShouldFail_WhenAlreadySucceeded()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);
        task.MarkSucceeded(TestUser);

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            task.MarkFailed(TestUser, "不应该失败"));

        Assert.Contains("任务已结束", ex.Message);
    }

    [Fact]
    public void MarkFailed_ShouldFail_WhenAlreadyFailed()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);
        task.MarkFailed(TestUser, "第一次失败");

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            task.MarkFailed(TestUser, "第二次失败"));

        Assert.Contains("任务已结束", ex.Message);
    }

    [Fact]
    public void MarkFailed_ShouldFail_WhenReasonEmpty()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() =>
            task.MarkFailed(TestUser, ""));

        Assert.Contains("参数不能为空", ex.Message);
    }

    #endregion

    #region 状态流转测试 - 取消场景

    [Fact]
    public void Cancel_ShouldSucceed_WhenPendingValidation()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);

        // Act
        task.Cancel(TestUser, "用户取消");

        // Assert
        Assert.Equal(BackgroundTaskStatus.Cancelled, task.Status);
        Assert.Equal("用户取消", task.FailureReason);
        Assert.NotNull(task.CompletedAtUtc);
        Assert.True(task.IsCompleted);
    }

    [Fact]
    public void Cancel_ShouldSucceed_WhenValidating()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);

        // Act
        task.Cancel(TestUser);

        // Assert
        Assert.Equal(BackgroundTaskStatus.Cancelled, task.Status);
        Assert.Null(task.FailureReason);
    }

    [Fact]
    public void Cancel_ShouldSucceed_WhenQueued()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);

        // Act
        task.Cancel(TestUser, "系统维护");

        // Assert
        Assert.Equal(BackgroundTaskStatus.Cancelled, task.Status);
        Assert.Equal("系统维护", task.FailureReason);
    }

    [Fact]
    public void Cancel_ShouldFail_WhenRunning()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            task.Cancel(TestUser));

        Assert.Contains("仅允许在排队前取消任务", ex.Message);
    }

    #endregion

    #region 状态约束测试

    [Fact]
    public void MarkValidating_ShouldFail_WhenNotPendingValidation()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            task.MarkValidating(TestUser));

        Assert.Contains("MarkValidating 仅适用于 PendingValidation 状态的任务", ex.Message);
    }

    [Fact]
    public void MarkQueued_ShouldFail_WhenRunning()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            task.MarkQueued(TestUser));

        Assert.Contains("只有待校验或校验中的任务才能进入队列", ex.Message);
    }

    [Fact]
    public void MarkRunning_ShouldFail_WhenNotQueued()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            task.MarkRunning(TestUser));

        Assert.Contains("只有排队中的任务才能进入执行", ex.Message);
    }

    [Fact]
    public void MarkSucceeded_ShouldFail_WhenNotRunning()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            task.MarkSucceeded(TestUser));

        Assert.Contains("只有执行中的任务才能标记为成功", ex.Message);
    }

    #endregion

    #region 心跳与进度测试

    [Fact]
    public void UpdateHeartbeat_ShouldSucceed()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        var initialHeartbeat = task.LastHeartbeatUtc;

        // Act
        System.Threading.Thread.Sleep(10); // 确保时间差异
        task.UpdateHeartbeat(TestUser);

        // Assert
        Assert.True(task.LastHeartbeatUtc > initialHeartbeat);
    }

    [Fact]
    public void UpdateProgress_ShouldSucceed_WhenRunning()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);

        // Act
        task.UpdateProgress(0.5, TestUser);

        // Assert
        Assert.Equal(0.5, task.Progress);
    }

    [Fact]
    public void UpdateProgress_ShouldSucceed_WhenValidating()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);

        // Act
        task.UpdateProgress(0.2, TestUser);

        // Assert
        Assert.Equal(0.2, task.Progress);
    }

    [Fact]
    public void UpdateProgress_ShouldClamp_WhenValueOutOfRange()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);

        // Act & Assert: 超过1的值被限制为1
        task.UpdateProgress(1.5, TestUser);
        Assert.Equal(1d, task.Progress);

        // Act & Assert: 小于0的值被限制为0
        task.UpdateProgress(-0.1, TestUser);
        Assert.Equal(0d, task.Progress);
    }

    [Fact]
    public void UpdateProgress_ShouldFail_WhenQueued()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() =>
            task.UpdateProgress(0.5, TestUser));

        Assert.Contains("仅在校验或执行阶段允许更新进度", ex.Message);
    }

    [Fact]
    public void UpdatePhase_ShouldSucceed()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);

        // Act
        task.UpdatePhase(BackgroundTaskPhases.RebuildingIndexes, TestUser);

        // Assert
        Assert.Equal(BackgroundTaskPhases.RebuildingIndexes, task.Phase);
    }

    [Fact]
    public void UpdatePhase_ShouldUseDefault_WhenPhaseEmpty()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);

        // Act
        task.UpdatePhase("   ", TestUser);

        // Assert
        Assert.Equal(BackgroundTaskPhases.PendingValidation, task.Phase);
    }

    #endregion

    #region 边界与验证测试

    [Fact]
    public void IsCompleted_ShouldReturnTrue_WhenSucceeded()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);
        task.MarkSucceeded(TestUser);

        // Assert
        Assert.True(task.IsCompleted);
    }

    [Fact]
    public void IsCompleted_ShouldReturnTrue_WhenFailed()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);
        task.MarkFailed(TestUser, "测试失败");

        // Assert
        Assert.True(task.IsCompleted);
    }

    [Fact]
    public void IsCompleted_ShouldReturnTrue_WhenCancelled()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.Cancel(TestUser);

        // Assert
        Assert.True(task.IsCompleted);
    }

    [Fact]
    public void IsCompleted_ShouldReturnFalse_WhenRunning()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);

        // Assert
        Assert.False(task.IsCompleted);
    }

    [Fact]
    public void MarkSucceeded_ShouldClearFailureReason()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);

        // Act
        task.MarkSucceeded(TestUser);

        // Assert
        Assert.Null(task.FailureReason);
    }

    [Fact]
    public void MarkSucceeded_ShouldSet_ProgressToOne()
    {
        // Arrange
        var task = BackgroundTask.Create(TestConfigId, TestDataSourceId, TestUser, TestUser);
        task.MarkValidating(TestUser);
        task.MarkQueued(TestUser);
        task.MarkRunning(TestUser);
        task.UpdateProgress(0.7, TestUser);

        // Act
        task.MarkSucceeded(TestUser);

        // Assert
        Assert.Equal(1d, task.Progress);
    }

    #endregion
}
