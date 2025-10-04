namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 维护分区所使用的文件组策略，可在默认文件组基础上追加更多文件组。
/// </summary>
public sealed class PartitionFilegroupStrategy
{
    private readonly List<string> additionalFilegroups = new();

    /// <summary>主文件组名称。</summary>
    public string PrimaryFilegroup { get; }

    /// <summary>附加文件组集合。</summary>
    public IReadOnlyList<string> AdditionalFilegroups => additionalFilegroups.AsReadOnly();

    private PartitionFilegroupStrategy(string primaryFilegroup)
    {
        PrimaryFilegroup = string.IsNullOrWhiteSpace(primaryFilegroup)
            ? throw new ArgumentException("主文件组不能为空", nameof(primaryFilegroup))
            : primaryFilegroup.Trim();
    }

    /// <summary>
    /// 创建只包含主文件组的策略实例。
    /// </summary>
    public static PartitionFilegroupStrategy Default(string filegroupName) => new(filegroupName);

    /// <summary>
    /// 向策略中加入额外文件组，自动去重。
    /// </summary>
    public PartitionFilegroupStrategy AddFilegroup(string filegroup)
    {
        if (string.IsNullOrWhiteSpace(filegroup))
        {
            throw new ArgumentException("文件组名称不能为空", nameof(filegroup));
        }

        var trimmed = filegroup.Trim();
        if (!additionalFilegroups.Contains(trimmed, StringComparer.OrdinalIgnoreCase))
        {
            additionalFilegroups.Add(trimmed);
        }

        return this;
    }
}
