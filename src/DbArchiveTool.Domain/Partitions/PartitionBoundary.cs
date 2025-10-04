namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 表示单个分区边界，包含业务排序键以及原始的分区值。
/// </summary>
public sealed class PartitionBoundary : IComparable<PartitionBoundary>
{
    /// <summary>用于排序的边界标识。</summary>
    public string SortKey { get; }

    /// <summary>实际分区值。</summary>
    public PartitionValue Value { get; }

    /// <summary>
    /// 创建新的分区边界实例。
    /// </summary>
    public PartitionBoundary(string sortKey, PartitionValue value)
    {
        if (string.IsNullOrWhiteSpace(sortKey))
        {
            throw new ArgumentException("边界排序键不能为空", nameof(sortKey));
        }

        SortKey = sortKey.Trim();
        Value = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>
    /// 先比较实际分区值，再比较排序键，确保单调性与稳定性。
    /// </summary>
    public int CompareTo(PartitionBoundary? other)
    {
        if (other is null)
        {
            return 1;
        }

        var valueComparison = Value.CompareTo(other.Value);
        return valueComparison != 0
            ? valueComparison
            : string.CompareOrdinal(SortKey, other.SortKey);
    }
}
