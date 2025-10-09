using System.Text.RegularExpressions;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 封装分区配置的数据文件与文件组存放策略。
/// </summary>
public sealed class PartitionStorageSettings
{
    private const string FilegroupPattern = "^[A-Za-z_][A-Za-z0-9_]*$";
    private const string FileNamePattern = "^[A-Za-z0-9_\\-\\.]+$";

    private PartitionStorageSettings(
        PartitionStorageMode mode,
        string filegroupName,
        string? dataFileDirectory,
        string? dataFileName,
        int? initialSizeMb,
        int? autoGrowthMb)
    {
        Mode = mode;
        FilegroupName = filegroupName;
        DataFileDirectory = dataFileDirectory;
        DataFileName = dataFileName;
        InitialSizeMb = initialSizeMb;
        AutoGrowthMb = autoGrowthMb;
    }

    /// <summary>存放模式。</summary>
    public PartitionStorageMode Mode { get; }

    /// <summary>目标文件组名称。</summary>
    public string FilegroupName { get; }

    /// <summary>数据文件目录，仅在创建独立文件时有效。</summary>
    public string? DataFileDirectory { get; }

    /// <summary>数据文件名称，仅在创建独立文件时有效。</summary>
    public string? DataFileName { get; }

    /// <summary>数据文件初始大小（MB），仅在创建独立文件时有效。</summary>
    public int? InitialSizeMb { get; }

    /// <summary>数据文件自动增长大小（MB），仅在创建独立文件时有效。</summary>
    public int? AutoGrowthMb { get; }

    /// <summary>
    /// 创建采用主文件组的存储设置。
    /// </summary>
    public static PartitionStorageSettings UsePrimary(string primaryFilegroup)
    {
        if (string.IsNullOrWhiteSpace(primaryFilegroup))
        {
            throw new ArgumentException("主文件组名称不能为空。", nameof(primaryFilegroup));
        }

        return new PartitionStorageSettings(
            PartitionStorageMode.PrimaryFilegroup,
            primaryFilegroup.Trim(),
            null,
            null,
            null,
            null);
    }

    /// <summary>
    /// 创建独立文件组 + 单数据文件的存储设置。
    /// </summary>
    public static PartitionStorageSettings CreateDedicated(
        string filegroupName,
        string dataFileDirectory,
        string dataFileName,
        int initialSizeMb,
        int autoGrowthMb)
    {
        if (string.IsNullOrWhiteSpace(filegroupName))
        {
            throw new ArgumentException("文件组名称不能为空。", nameof(filegroupName));
        }

        if (!Regex.IsMatch(filegroupName.Trim(), FilegroupPattern))
        {
            throw new ArgumentException("文件组名称仅支持字母、数字与下划线，并且需以字母或下划线开头。", nameof(filegroupName));
        }

        if (string.IsNullOrWhiteSpace(dataFileDirectory))
        {
            throw new ArgumentException("数据文件目录不能为空。", nameof(dataFileDirectory));
        }

        if (string.IsNullOrWhiteSpace(dataFileName))
        {
            throw new ArgumentException("数据文件名称不能为空。", nameof(dataFileName));
        }

        if (!Regex.IsMatch(dataFileName.Trim(), FileNamePattern))
        {
            throw new ArgumentException("数据文件名称仅支持字母、数字、下划线、短横线与点号。", nameof(dataFileName));
        }

        if (initialSizeMb <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(initialSizeMb), "初始大小必须大于 0。");
        }

        if (autoGrowthMb <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(autoGrowthMb), "自动增长大小必须大于 0。");
        }

        return new PartitionStorageSettings(
            PartitionStorageMode.DedicatedFilegroupSingleFile,
            filegroupName.Trim(),
            dataFileDirectory.Trim(),
            dataFileName.Trim(),
            initialSizeMb,
            autoGrowthMb);
    }
}

