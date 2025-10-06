using System;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 描述分区边界与文件组之间的映射关系，用于在拆分、合并时绑定目标文件组。
/// </summary>
public sealed class PartitionFilegroupMapping : IEquatable<PartitionFilegroupMapping>
{
    /// <summary>分区边界标识，对应 <see cref="PartitionBoundary"/> 的排序键。</summary>
    public string BoundaryKey { get; }

    /// <summary>映射到的文件组名称。</summary>
    public string FilegroupName { get; }

    private PartitionFilegroupMapping(string boundaryKey, string filegroupName)
    {
        BoundaryKey = boundaryKey;
        FilegroupName = filegroupName;
    }

    /// <summary>
    /// 创建新的文件组映射实例。
    /// </summary>
    public static PartitionFilegroupMapping Create(string boundaryKey, string filegroupName)
    {
        if (string.IsNullOrWhiteSpace(boundaryKey))
        {
            throw new ArgumentException("分区边界标识不能为空。", nameof(boundaryKey));
        }

        if (string.IsNullOrWhiteSpace(filegroupName))
        {
            throw new ArgumentException("文件组名称不能为空。", nameof(filegroupName));
        }

        return new PartitionFilegroupMapping(boundaryKey.Trim(), filegroupName.Trim());
    }

    /// <inheritdoc />
    public bool Equals(PartitionFilegroupMapping? other)
    {
        if (other is null)
        {
            return false;
        }

        return BoundaryKey.Equals(other.BoundaryKey, StringComparison.OrdinalIgnoreCase)
            && FilegroupName.Equals(other.FilegroupName, StringComparison.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as PartitionFilegroupMapping);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(BoundaryKey.ToLowerInvariant(), FilegroupName.ToLowerInvariant());
}
