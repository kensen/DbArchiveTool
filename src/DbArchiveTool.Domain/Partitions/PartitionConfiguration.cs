using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 描述 SQL Server 分区表的配置、边界集合与安全约束。负责在领域层保证新增/删除边界的规则正确性。
/// </summary>
public sealed class PartitionConfiguration : AggregateRoot
{
    /// <summary>
    /// 内部维护的分区边界列表，始终保持按 SortKey 升序排列。
    /// </summary>
    private readonly List<PartitionBoundary> boundaries = new();

    /// <summary>归档数据源标识。</summary>
    public Guid ArchiveDataSourceId { get; }

    /// <summary>表的架构名。</summary>
    public string SchemaName { get; }

    /// <summary>被分区的表名。</summary>
    public string TableName { get; }

    /// <summary>关联的分区函数名称。</summary>
    public string PartitionFunctionName { get; }

    /// <summary>关联的分区方案名称。</summary>
    public string PartitionSchemeName { get; }

    /// <summary>分区列定义。</summary>
    public PartitionColumn PartitionColumn { get; }

    /// <summary>文件组策略描述。</summary>
    public PartitionFilegroupStrategy FilegroupStrategy { get; }

    /// <summary>分区保留策略。</summary>
    public PartitionRetentionPolicy? RetentionPolicy { get; }

    /// <summary>指示是否为 Range Right 分区函数。</summary>
    public bool IsRangeRight { get; }

    /// <summary>当前注册的分区边界集合。</summary>
    public IReadOnlyList<PartitionBoundary> Boundaries => new ReadOnlyCollection<PartitionBoundary>(boundaries);

    /// <summary>
    /// 构造新的分区配置实例，并根据已存在的边界初始化内部状态。
    /// </summary>
    public PartitionConfiguration(
        Guid archiveDataSourceId,
        string schemaName,
        string tableName,
        string partitionFunctionName,
        string partitionSchemeName,
        PartitionColumn partitionColumn,
        PartitionFilegroupStrategy filegroupStrategy,
        bool isRangeRight,
        PartitionRetentionPolicy? retentionPolicy = null,
        IEnumerable<PartitionBoundary>? existingBoundaries = null)
    {
        ArchiveDataSourceId = archiveDataSourceId != Guid.Empty ? archiveDataSourceId : throw new ArgumentException("数据源标识不能为空", nameof(archiveDataSourceId));
        SchemaName = string.IsNullOrWhiteSpace(schemaName) ? throw new ArgumentException("架构名不能为空", nameof(schemaName)) : schemaName.Trim();
        TableName = string.IsNullOrWhiteSpace(tableName) ? throw new ArgumentException("表名不能为空", nameof(tableName)) : tableName.Trim();
        PartitionFunctionName = string.IsNullOrWhiteSpace(partitionFunctionName) ? throw new ArgumentException("分区函数名不能为空", nameof(partitionFunctionName)) : partitionFunctionName.Trim();
        PartitionSchemeName = string.IsNullOrWhiteSpace(partitionSchemeName) ? throw new ArgumentException("分区方案名不能为空", nameof(partitionSchemeName)) : partitionSchemeName.Trim();
        PartitionColumn = partitionColumn ?? throw new ArgumentNullException(nameof(partitionColumn));
        FilegroupStrategy = filegroupStrategy ?? throw new ArgumentNullException(nameof(filegroupStrategy));
        IsRangeRight = isRangeRight;
        RetentionPolicy = retentionPolicy;

        if (existingBoundaries is not null)
        {
            foreach (var boundary in existingBoundaries)
            {
                AddBoundaryInternal(boundary, skipValidation: true);
            }

            EnsureSortedOrder();
            EnsureMonotonicGrowth();
        }
    }

    /// <summary>
    /// 尝试追加新的分区边界，确保遵循 RangeRight/RangeLeft 的单调性要求。
    /// </summary>
    public PartitionOperationResult TryAddBoundary(PartitionBoundary boundary)
    {
        if (boundary is null)
        {
            return PartitionOperationResult.Failure("边界信息不能为空。");
        }

        var validation = ValidateNewBoundary(boundary);
        if (!validation.IsSatisfied)
        {
            return PartitionOperationResult.Failure(validation.ErrorMessage);
        }

        AddBoundaryInternal(boundary, skipValidation: false);
        Touch("PARTITION-MANAGER");
        return PartitionOperationResult.Success();
    }

    /// <summary>
    /// 尝试移除指定分区边界，限制至少保留一个边界以保障配置有效。
    /// </summary>
    public PartitionOperationResult TryRemoveBoundary(string boundaryKey)
    {
        if (string.IsNullOrWhiteSpace(boundaryKey))
        {
            return PartitionOperationResult.Failure("边界标识不能为空。");
        }

        var index = boundaries.FindIndex(x => x.SortKey.Equals(boundaryKey, StringComparison.Ordinal));
        if (index < 0)
        {
            return PartitionOperationResult.Failure("未找到指定边界。");
        }

        if (boundaries.Count <= 1)
        {
            return PartitionOperationResult.Failure("至少保留一个分区边界。");
        }

        boundaries.RemoveAt(index);
        Touch("PARTITION-MANAGER");
        return PartitionOperationResult.Success();
    }

    /// <summary>
    /// 在内部写操作中添加边界，可选择跳过校验用于初始化。
    /// </summary>
    private void AddBoundaryInternal(PartitionBoundary boundary, bool skipValidation)
    {
        if (boundary is null)
        {
            throw new ArgumentNullException(nameof(boundary));
        }

        if (!skipValidation)
        {
            var validation = ValidateNewBoundary(boundary);
            if (!validation.IsSatisfied)
            {
                throw new InvalidOperationException(validation.ErrorMessage);
            }
        }

        boundaries.Add(boundary);
        EnsureSortedOrder();
    }

    /// <summary>
    /// 校验待新增的边界是否满足单调性与唯一性要求。
    /// </summary>
    private PartitionBoundaryValidation ValidateNewBoundary(PartitionBoundary boundary)
    {
        if (boundaries.Any(x => x.SortKey.Equals(boundary.SortKey, StringComparison.Ordinal)))
        {
            return PartitionBoundaryValidation.NotSatisfied("已存在相同的分区边界。");
        }

        if (boundaries.Count == 0)
        {
            return PartitionBoundaryValidation.Satisfied();
        }

        var sorted = boundaries.OrderBy(x => x).ToList();

        if (IsRangeRight)
        {
            var max = sorted.Last();
            if (boundary.CompareTo(max) <= 0)
            {
                return PartitionBoundaryValidation.NotSatisfied("Range Right 分区新增的边界必须大于当前最大值。");
            }
        }
        else
        {
            var min = sorted.First();
            if (boundary.CompareTo(min) >= 0)
            {
                return PartitionBoundaryValidation.NotSatisfied("Range Left 分区新增的边界必须小于当前最小值。");
            }
        }

        return PartitionBoundaryValidation.Satisfied();
    }

    /// <summary>
    /// 保证内部边界列表始终保持升序。
    /// </summary>
    private void EnsureSortedOrder() => boundaries.Sort();

    /// <summary>
    /// 在初始化时校验提供的边界集合是否严格单调递增。
    /// </summary>
    private void EnsureMonotonicGrowth()
    {
        if (boundaries.Count <= 1)
        {
            return;
        }

        var pairs = boundaries.Zip(boundaries.Skip(1), (prev, next) => (prev, next));
        foreach (var (prev, next) in pairs)
        {
            if (prev.CompareTo(next) >= 0)
            {
                throw new InvalidOperationException("提供的初始分区边界顺序无效。");
            }
        }
    }
}

/// <summary>
/// 封装领域层内的操作结果，携带失败原因。
/// </summary>
public sealed record PartitionOperationResult(bool IsSuccess, string? ErrorMessage)
{
    /// <summary>创建成功结果。</summary>
    public static PartitionOperationResult Success() => new(true, null);

    /// <summary>创建失败结果。</summary>
    public static PartitionOperationResult Failure(string message) => new(false, message);
}

/// <summary>
/// 表示校验边界时的判定结果，包含是否通过与原因。
/// </summary>
public readonly struct PartitionBoundaryValidation
{
    /// <summary>是否满足校验条件。</summary>
    public bool IsSatisfied { get; }

    /// <summary>当失败时的提示信息。</summary>
    public string ErrorMessage { get; }

    private PartitionBoundaryValidation(bool isSatisfied, string errorMessage)
    {
        IsSatisfied = isSatisfied;
        ErrorMessage = errorMessage;
    }

    /// <summary>构造通过的校验结果。</summary>
    public static PartitionBoundaryValidation Satisfied() => new(true, string.Empty);

    /// <summary>构造失败的校验结果。</summary>
    public static PartitionBoundaryValidation NotSatisfied(string message) => new(false, message);
}
