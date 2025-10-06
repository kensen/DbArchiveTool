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

    [Fact]
    public void TryAssignFilegroup_ShouldBindMapping()
    {
        var config = CreateConfig(isRangeRight: true, existing: new[] { PartitionValue.FromInt(10) });
        var overwrite = config.TryAssignFilegroup("0000", "FG_ARCHIVE");
        Assert.True(overwrite.IsSuccess);
        Assert.Equal("FG_ARCHIVE", config.ResolveFilegroup("0000"));
    }

    [Fact]
    public void ResolveFilegroup_ShouldFallbackToPrimary()
    {
        var config = CreateConfig(isRangeRight: true, existing: new[] { PartitionValue.FromInt(10) });
        Assert.Equal("PRIMARY", config.ResolveFilegroup("0000"));
    }

    [Fact]
    public void UpdateSafetyRule_ShouldReplaceExisting()
    {
        var config = CreateConfig(isRangeRight: true, existing: new[] { PartitionValue.FromInt(10) });
        var rule = new PartitionSafetyRule(true, new[] { "S" }, "00:00-06:00");
        config.UpdateSafetyRule(rule);
        Assert.True(config.SafetyRule?.RequiresEmptyPartition);
        config.ClearSafetyRule();
        Assert.Null(config.SafetyRule);
    }

    private static PartitionConfiguration CreateConfig(bool isRangeRight, IEnumerable<PartitionValue> existing)
    {
        var boundaries = existing.Select((value, index) => new PartitionBoundary(index.ToString("D4"), value)).ToList();
        return new PartitionConfiguration(
            Guid.NewGuid(),
            "dbo",
            "Orders",
            "pf_orders",
            "ps_orders",
            new PartitionColumn("OrderDate", PartitionValueKind.Int),
            PartitionFilegroupStrategy.Default("PRIMARY"),
            isRangeRight,
            null,
            boundaries);
    }
}
