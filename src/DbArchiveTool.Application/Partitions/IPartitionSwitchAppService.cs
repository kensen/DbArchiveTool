using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 定义分区切换业务能力。
/// </summary>
public interface IPartitionSwitchAppService
{
    Task<Result<PartitionSwitchInspectionResultDto>> InspectAsync(SwitchPartitionInspectionRequest request, CancellationToken cancellationToken = default);

    Task<Result<Guid>> ArchiveBySwitchAsync(SwitchPartitionExecuteRequest request, CancellationToken cancellationToken = default);
}
