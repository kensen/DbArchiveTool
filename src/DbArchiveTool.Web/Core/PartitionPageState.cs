using System;

namespace DbArchiveTool.Web.Core;

/// <summary>
/// 在分区执行与管理页面之间传递刷新信号的状态服务。
/// </summary>
public sealed class PartitionPageState
{
    public event Action<Guid, Guid>? DraftsRefreshRequested;

    public void RequestDraftsRefresh(Guid dataSourceId, Guid configurationId)
    {
        DraftsRefreshRequested?.Invoke(dataSourceId, configurationId);
    }
}
