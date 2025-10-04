using DbArchiveTool.Domain.Partitions;

namespace DbArchiveTool.UnitTests.Partitions;

/// <summary>
/// 验证分区值对象的字面量转换与比较逻辑。
/// </summary>
public class PartitionValueTests
{
    [Theory]
    [InlineData(5, 5, 0)]
    [InlineData(3, 8, -1)]
    [InlineData(9, 2, 1)]
    public void IntPartitionValue_ShouldCompareCorrectly(int left, int right, int expected)
    {
        var l = PartitionValue.FromInt(left);
        var r = PartitionValue.FromInt(right);
        Assert.Equal(expected, Math.Sign(l.CompareTo(r)));
    }

    [Fact]
    public void GuidPartitionValue_ShouldFormatLiteral()
    {
        var guid = Guid.NewGuid();
        var value = PartitionValue.FromGuid(guid);
        Assert.Equal($"'{guid:D}'", value.ToLiteral());
    }

    [Fact]
    public void StringPartitionValue_ShouldEscapeQuotes()
    {
        var value = PartitionValue.FromString("O'Reilly");
        Assert.Equal("'O''Reilly'", value.ToLiteral());
    }

    [Fact]
    public void DatePartitionValue_ShouldFormatLiteral()
    {
        var date = new DateOnly(2025, 10, 4);
        var value = PartitionValue.FromDate(date);
        Assert.Equal("'2025-10-04'", value.ToLiteral());
    }
}
