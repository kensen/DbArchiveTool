using System;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 表示分区边界值的抽象基类，提供不同数据类型的字面量转换能力。
/// </summary>
public abstract class PartitionValue
{
    /// <summary>分区值的数据类型。</summary>
    public abstract PartitionValueKind Kind { get; }

    /// <summary>转换为 SQL 字面量，确保安全且可执行。</summary>
    public abstract string ToLiteral();

    /// <summary>用于比较两个分区值的大小关系。</summary>
    public abstract int CompareTo(PartitionValue other);

    /// <summary>创建 int 类型的分区值。</summary>
    public static PartitionValue FromInt(int value) => new IntPartitionValue(value);

    /// <summary>创建 bigint 类型的分区值。</summary>
    public static PartitionValue FromBigInt(long value) => new BigIntPartitionValue(value);

    /// <summary>创建 date 类型的分区值。</summary>
    public static PartitionValue FromDate(DateOnly value) => new DatePartitionValue(value);

    /// <summary>创建 datetime 类型的分区值。</summary>
    public static PartitionValue FromDateTime(DateTime value) => new DateTimePartitionValue(value);

    /// <summary>创建 datetime2 类型的分区值。</summary>
    public static PartitionValue FromDateTime2(DateTime value) => new DateTime2PartitionValue(value);

    /// <summary>创建 guid 类型的分区值。</summary>
    public static PartitionValue FromGuid(Guid value) => new GuidPartitionValue(value);

    /// <summary>创建 string 类型的分区值。</summary>
    public static PartitionValue FromString(string value) => new StringPartitionValue(value);

    private sealed class IntPartitionValue : PartitionValue
    {
        private readonly int value;
        public IntPartitionValue(int value) => this.value = value;
        public override PartitionValueKind Kind => PartitionValueKind.Int;
        public override string ToLiteral() => value.ToString();
        public override int CompareTo(PartitionValue other) => value.CompareTo(((IntPartitionValue)other).value);
    }

    private sealed class BigIntPartitionValue : PartitionValue
    {
        private readonly long value;
        public BigIntPartitionValue(long value) => this.value = value;
        public override PartitionValueKind Kind => PartitionValueKind.BigInt;
        public override string ToLiteral() => value.ToString();
        public override int CompareTo(PartitionValue other) => value.CompareTo(((BigIntPartitionValue)other).value);
    }

    private sealed class DatePartitionValue : PartitionValue
    {
        private readonly DateOnly value;
        public DatePartitionValue(DateOnly value) => this.value = value;
        public override PartitionValueKind Kind => PartitionValueKind.Date;
        public override string ToLiteral() => $"'{value:yyyy-MM-dd}'";
        public override int CompareTo(PartitionValue other) => value.CompareTo(((DatePartitionValue)other).value);
    }

    private sealed class DateTimePartitionValue : PartitionValue
    {
        private readonly DateTime value;
        public DateTimePartitionValue(DateTime value) => this.value = DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
        public override PartitionValueKind Kind => PartitionValueKind.DateTime;
        public override string ToLiteral() => $"'{value:yyyy-MM-dd HH:mm:ss}'";
        public override int CompareTo(PartitionValue other) => value.CompareTo(((DateTimePartitionValue)other).value);
    }

    private sealed class DateTime2PartitionValue : PartitionValue
    {
        private readonly DateTime value;
        public DateTime2PartitionValue(DateTime value) => this.value = DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
        public override PartitionValueKind Kind => PartitionValueKind.DateTime2;
        public override string ToLiteral() => $"'{value:yyyy-MM-dd HH:mm:ss.fffffff}'";
        public override int CompareTo(PartitionValue other) => value.CompareTo(((DateTime2PartitionValue)other).value);
    }

    private sealed class GuidPartitionValue : PartitionValue
    {
        private readonly Guid value;
        public GuidPartitionValue(Guid value) => this.value = value;
        public override PartitionValueKind Kind => PartitionValueKind.Guid;
        public override string ToLiteral() => $"'{value:D}'";
        public override int CompareTo(PartitionValue other) => value.CompareTo(((GuidPartitionValue)other).value);
    }

    private sealed class StringPartitionValue : PartitionValue
    {
        private readonly string value;
        public StringPartitionValue(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException("字符串值不能为空", nameof(value));
            }

            this.value = value;
        }
        public override PartitionValueKind Kind => PartitionValueKind.String;
        public override string ToLiteral() => $"'{value.Replace("'", "''")}'";
        public override int CompareTo(PartitionValue other) => string.CompareOrdinal(value, ((StringPartitionValue)other).value);
    }
}
