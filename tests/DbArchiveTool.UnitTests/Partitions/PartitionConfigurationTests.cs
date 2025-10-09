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

    [Fact]
    public void ReplaceBoundaries_ShouldSortAndDeduplicate()
    {
        var config = CreateConfig(isRangeRight: true, existing: Array.Empty<PartitionValue>());
        var values = new[]
        {
            PartitionValue.FromInt(30),
            PartitionValue.FromInt(10),
            PartitionValue.FromInt(20),
            PartitionValue.FromInt(10)
        };

        var result = config.ReplaceBoundaries(values);

        Assert.True(result.IsSuccess);
        var ordered = config.Boundaries.Select(b => b.Value.ToInvariantString()).ToArray();
        Assert.Equal(new[] { "10", "20", "30" }, ordered);
    }

    [Fact]
    public void ReplaceBoundaries_ShouldFail_WhenEmpty()
    {
        var config = CreateConfig(isRangeRight: true, existing: Array.Empty<PartitionValue>());

        var result = config.ReplaceBoundaries(Array.Empty<PartitionValue>());

        Assert.False(result.IsSuccess);
        Assert.Equal("分区边界列表不能为空。", result.ErrorMessage);
    }

    [Fact]
    public void StorageSettings_CreateDedicated_ShouldValidateParameters()
    {
        Assert.Throws<ArgumentException>(() => PartitionStorageSettings.CreateDedicated("", "D:/data", "file.ndf", 32, 8));
        Assert.Throws<ArgumentException>(() => PartitionStorageSettings.CreateDedicated("FG1", "", "file.ndf", 32, 8));
        Assert.Throws<ArgumentException>(() => PartitionStorageSettings.CreateDedicated("FG1", "D:/data", "", 32, 8));
        Assert.Throws<ArgumentOutOfRangeException>(() => PartitionStorageSettings.CreateDedicated("FG1", "D:/data", "file.ndf", 0, 8));
        Assert.Throws<ArgumentOutOfRangeException>(() => PartitionStorageSettings.CreateDedicated("FG1", "D:/data", "file.ndf", 32, 0));
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
