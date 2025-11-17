namespace DbArchiveTool.Domain.Entities;

/// <summary>
/// 任务执行状态
/// </summary>
public enum JobExecutionStatus
{
    /// <summary>未开始</summary>
    NotStarted = 0,

    /// <summary>运行中</summary>
    Running = 1,

    /// <summary>成功</summary>
    Success = 2,

    /// <summary>失败</summary>
    Failed = 3,

    /// <summary>跳过(无数据可归档时)</summary>
    Skipped = 4
}
