using DbArchiveTool.Domain.Partitions;

namespace DbArchiveTool.UnitTests.Partitions;

/// <summary>
/// 验证 PartitionConfiguration 的核心规则。
/// </summary>
public class PartitionConfigurationTests
{
    [Fact]
    public void TryAddBoundary_ShouldRespectRangeRightOrder()
    {
        var config = CreateConfig(isRangeRight: true, existing: new[] { PartitionValue.FromInt(10) });
        var result = config.TryAddBoundary(new PartitionBoundary("0010", PartitionValue.FromInt(8)));
        Assert.False(result.IsSuccess);
    }

    [Fact]
    public void TryAddBoundary_ShouldSucceedForAscendingValue()
    {
        var config = CreateConfig(isRangeRight: true, existing: new[] { PartitionValue.FromInt(10) });
        var result = config.TryAddBoundary(new PartitionBoundary("0011", PartitionValue.FromInt(12)));
        Assert.True(result.IsSuccess);
    }

    private static PartitionConfiguration CreateConfig(bool isRangeRight, IEnumerable<PartitionValue> existing)
    {
        var boundaries = existing.Select((value, index) => new PartitionBoundary(index.ToString("D4"), value)).ToList();
        return new PartitionConfiguration(Guid.NewGuid(), "dbo", "Orders", "pf_orders", "ps_orders", new PartitionColumn("OrderDate", PartitionValueKind.Int), PartitionFilegroupStrategy.Default("PRIMARY"), isRangeRight, null, boundaries);
    }
}
