namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 描述分区列的元数据信息，包括列名与值类型。
/// </summary>
public sealed class PartitionColumn
{
    /// <summary>列名。</summary>
    public string Name { get; }

    /// <summary>分区值的数据类型。</summary>
    public PartitionValueKind ValueKind { get; }

    /// <summary>
    /// 构造分区列定义。
    /// </summary>
    public PartitionColumn(string name, PartitionValueKind valueKind)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("分区列名称不能为空", nameof(name));
        }

        Name = name.Trim();
        ValueKind = valueKind;
    }
}
