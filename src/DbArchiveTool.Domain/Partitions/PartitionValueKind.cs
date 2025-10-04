namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 支持的分区列数据类型枚举。
/// </summary>
public enum PartitionValueKind
{
    /// <summary>32 位整数。</summary>
    Int,
    /// <summary>64 位整数。</summary>
    BigInt,
    /// <summary>日期。</summary>
    Date,
    /// <summary>日期时间。</summary>
    DateTime,
    /// <summary>高精度日期时间。</summary>
    DateTime2,
    /// <summary>唯一标识。</summary>
    Guid,
    /// <summary>字符串类型。</summary>
    String
}
