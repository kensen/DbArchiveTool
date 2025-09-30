namespace DbArchiveTool.Domain.ArchiveTasks;

public sealed record ArchiveTaskFailedEvent(Guid TaskId, string FailureReason);
