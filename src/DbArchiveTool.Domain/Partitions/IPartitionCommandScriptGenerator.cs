using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 定义分区命令脚本生成器，负责根据配置与目标边界生成可执行的 SQL 文本。
/// </summary>
public interface IPartitionCommandScriptGenerator
{
    /// <summary>
    /// 根据目标边界生成拆分脚本。
    /// </summary>
    Result<string> GenerateSplitScript(PartitionConfiguration configuration, IReadOnlyList<PartitionValue> newBoundaries);
}
