using DbArchiveTool.Application.Archives;

namespace DbArchiveTool.UnitTests.Archives;

/// <summary>
/// 验证 CronScheduleHelper 对不同 Cron 表达式的处理逻辑。
/// </summary>
public class CronScheduleHelperTests
{
    /// <summary>
    /// 验证不包含秒的 Cron 表达式可以正确计算下一次执行时间。
    /// </summary>
    [Fact]
    public void GetNextOccurrenceUtc_ShouldReturnExpectedOccurrence_WhenCronWithoutSeconds()
    {
        var referenceUtc = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var result = CronScheduleHelper.GetNextOccurrenceUtc("*/5 * * * *", referenceUtc);

        Assert.NotNull(result);
        Assert.Equal(new DateTime(2024, 1, 1, 0, 5, 0, DateTimeKind.Utc), result.Value);
    }

    /// <summary>
    /// 验证包含秒字段的 Cron 表达式可以正确识别下一次执行时间。
    /// </summary>
    [Fact]
    public void GetNextOccurrenceUtc_ShouldSupportCronWithSeconds()
    {
        var referenceUtc = new DateTime(2024, 1, 1, 0, 0, 5, DateTimeKind.Utc);
        var result = CronScheduleHelper.GetNextOccurrenceUtc("*/10 * * * * *", referenceUtc);

        Assert.NotNull(result);
        Assert.Equal(new DateTime(2024, 1, 1, 0, 0, 10, DateTimeKind.Utc), result.Value);
    }

    /// <summary>
    /// 验证当 Cron 表达式无效时会返回 null。
    /// </summary>
    [Fact]
    public void GetNextOccurrenceUtc_ShouldReturnNull_WhenCronInvalid()
    {
        var referenceUtc = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var result = CronScheduleHelper.GetNextOccurrenceUtc("invalid cron", referenceUtc);

        Assert.Null(result);
    }
}
