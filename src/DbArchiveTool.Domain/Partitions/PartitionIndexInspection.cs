using System.Collections.Generic;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 描述目标表索引与分区列的对齐情况。
/// </summary>
public sealed record PartitionIndexInspection(
    bool HasClusteredIndex,
    IndexAlignmentInfo? ClusteredIndex,
    IReadOnlyList<IndexAlignmentInfo> UniqueIndexes,
    IReadOnlyList<string> ExternalForeignKeys)
{
    public bool HasExternalForeignKeys => ExternalForeignKeys.Count > 0;

    public IReadOnlyList<IndexAlignmentInfo> IndexesMissingPartitionColumn =>
        GetIndexesMissingPartitionColumn();

    private IReadOnlyList<IndexAlignmentInfo> GetIndexesMissingPartitionColumn()
    {
        var list = new List<IndexAlignmentInfo>();
        if (ClusteredIndex is not null && !ClusteredIndex.ContainsPartitionColumn)
        {
            list.Add(ClusteredIndex);
        }

        foreach (var index in UniqueIndexes)
        {
            if (!index.ContainsPartitionColumn)
            {
                list.Add(index);
            }
        }

        return list;
    }
}

/// <summary>
/// 描述单个索引或约束与分区列的关系。
/// </summary>
public sealed record IndexAlignmentInfo(
    string IndexName,
    bool IsClustered,
    bool IsPrimaryKey,
    bool IsUniqueConstraint,
    bool IsUnique,
    bool ContainsPartitionColumn,
    IReadOnlyList<string> KeyColumns);
