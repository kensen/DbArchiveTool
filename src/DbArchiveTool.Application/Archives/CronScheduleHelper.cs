using NCrontab;

namespace DbArchiveTool.Application.Archives;

/// <summary>
/// Cron 表达式调度计算工具。
/// </summary>
public static class CronScheduleHelper
{
    /// <summary>
    /// 计算下一次执行时间(UTC)。
    /// </summary>
    /// <param name="cronExpression">Cron 表达式</param>
    /// <param name="referenceUtc">参考时间(UTC)</param>
    /// <returns>下一次执行时间,若不可计算则返回 null</returns>
    public static DateTime? GetNextOccurrenceUtc(string cronExpression, DateTime referenceUtc)
    {
        if (string.IsNullOrWhiteSpace(cronExpression))
        {
            return null;
        }

        try
        {
            var options = new CrontabSchedule.ParseOptions
            {
                IncludingSeconds = HasSeconds(cronExpression)
            };

            var schedule = CrontabSchedule.Parse(cronExpression, options);
            return schedule.GetNextOccurrence(referenceUtc);
        }
        catch
        {
            // 调用方负责处理无效表达式
            return null;
        }
    }

    private static bool HasSeconds(string cronExpression)
    {
        var parts = cronExpression.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return parts.Length >= 6;
    }
}
