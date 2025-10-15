using System.Collections.Generic;
using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 定义分区命令脚本生成器，负责根据配置与目标边界生成可执行的 SQL 文本。
/// </summary>
public interface IPartitionCommandScriptGenerator
{
    /// <summary>生成拆分脚本。</summary>
    Result<string> GenerateSplitScript(PartitionConfiguration configuration, IReadOnlyList<PartitionValue> newBoundaries);

    /// <summary>生成合并脚本，传入需合并的边界键。</summary>
    Result<string> GenerateMergeScript(PartitionConfiguration configuration, string boundaryKey);

    /// <summary>生成 SWITCH OUT 脚本，包含目标表/文件组等参数。</summary>
    Result<string> GenerateSwitchOutScript(PartitionConfiguration configuration, SwitchPayload payload);
}

/// <summary>
/// SWITCH 操作所需的附加参数。
/// </summary>
public sealed record SwitchPayload(
    string SourcePartitionKey,
    string TargetSchema,
    string TargetTable,
    bool CreateStagingTable,
    string? StagingTableName,
    string? FilegroupName,
    IReadOnlyDictionary<string, object>? AdditionalProperties);
