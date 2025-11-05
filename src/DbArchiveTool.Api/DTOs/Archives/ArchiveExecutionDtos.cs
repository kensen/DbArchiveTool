namespace DbArchiveTool.Api.DTOs.Archives;

/// <summary>
/// 批量执行归档任务请求
/// </summary>
public sealed class ExecuteArchiveRequest
{
    /// <summary>
    /// 归档配置ID列表
    /// </summary>
    public List<Guid> ConfigurationIds { get; set; } = new();
}
