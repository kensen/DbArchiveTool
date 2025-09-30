namespace DbArchiveTool.Domain.ArchiveTasks;

public enum ArchiveTaskStatus
{
    Pending = 1,
    Running = 2,
    Succeeded = 3,
    Failed = 4,
    Cancelled = 5
}
