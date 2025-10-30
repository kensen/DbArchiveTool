using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using DbArchiveTool.Domain.Abstractions;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// PartitionConfiguration 聚合根，用于描述 SQL Server 分区表的配置、边界集合、文件组策略与安全规则。
/// 负责在领域层验证新增/删除边界的合法性，并提供文件组映射与安全规则等聚合行为。
/// </summary>
public sealed class PartitionConfiguration : AggregateRoot
{
    private const string DefaultAuditUser = "PARTITION-MANAGER";

    /// <summary>内部维护的分区边界列表，始终按 SortKey 升序排序。</summary>
    private readonly List<PartitionBoundary> boundaries = new();

    /// <summary>维护分区边界到文件组的映射列表。</summary>
    private readonly List<PartitionFilegroupMapping> filegroupMappings = new();

    /// <summary>关联的归档数据源标识。</summary>
    public Guid ArchiveDataSourceId { get; }

    /// <summary>目标表架构名称。</summary>
    public string SchemaName { get; }

    /// <summary>目标表名称。</summary>
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

    /// <summary>分区数据存放设置。</summary>
    public PartitionStorageSettings StorageSettings { get; private set; }

    /// <summary>目标表设置。</summary>
    public PartitionTargetTable? TargetTable { get; private set; }

    /// <summary>是否要求分区列在执行脚本前转为 NOT NULL。</summary>
    public bool RequirePartitionColumnNotNull { get; private set; }

    /// <summary>配置备注。</summary>
    public string? Remarks { get; private set; }

    /// <summary>是否为 Range Right 分区函数。</summary>
    public bool IsRangeRight { get; }

    /// <summary>指示该配置是否已经执行到数据库(一旦执行将不允许修改基础信息)。</summary>
    public bool IsCommitted { get; private set; }

    /// <summary>分区操作的安全规则。</summary>
    public PartitionSafetyRule? SafetyRule { get; private set; }

    /// <summary>当前执行阶段(用于向导流程跟踪)。</summary>
    public string? ExecutionStage { get; private set; }

    /// <summary>最后一次执行任务的ID(用于关联执行日志)。</summary>
    public Guid? LastExecutionTaskId { get; private set; }

    /// <summary>当前注册的分区边界集合。</summary>
    public IReadOnlyList<PartitionBoundary> Boundaries => new ReadOnlyCollection<PartitionBoundary>(boundaries);

    /// <summary>
    /// 根据分区号查找对应的边界排序键。
    /// </summary>
    /// <remarks>
    /// Range Right: 分区号 = 边界索引 + 1；Range Left: 分区号 = 边界索引。
    /// </remarks>
    public string? FindBoundaryKeyByPartitionNumber(int partitionNumber)
    {
        if (partitionNumber <= 0)
        {
            return null;
        }

        if (boundaries.Count == 0)
        {
            return null;
        }

        var index = IsRangeRight ? partitionNumber - 1 : partitionNumber;
        if (index < 0 || index >= boundaries.Count)
        {
            return null;
        }

        return boundaries[index].SortKey;
    }

    /// <summary>当前的文件组映射集合。</summary>
    public IReadOnlyList<PartitionFilegroupMapping> FilegroupMappings => new ReadOnlyCollection<PartitionFilegroupMapping>(filegroupMappings);

    /// <summary>
    /// 构造新的分区配置实例，并根据数据库读取到的边界、文件组映射、安全规则初始化聚合状态。
    /// </summary>
    /// <param name="archiveDataSourceId">归档数据源标识。</param>
    /// <param name="schemaName">目标架构名称。</param>
    /// <param name="tableName">目标表名称。</param>
    /// <param name="partitionFunctionName">分区函数名称。</param>
    /// <param name="partitionSchemeName">分区方案名称。</param>
    /// <param name="partitionColumn">分区列定义。</param>
    /// <param name="filegroupStrategy">文件组策略。</param>
    /// <param name="isRangeRight">是否为 Range Right。</param>
    /// <param name="retentionPolicy">分区保留策略。</param>
    /// <param name="existingBoundaries">数据库中已存在的分区边界集合。</param>
    /// <param name="existingFilegroupMappings">数据库中已存在的文件组映射。</param>
    /// <param name="safetyRule">安全规则配置。</param>
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
        IEnumerable<PartitionBoundary>? existingBoundaries = null,
        IEnumerable<PartitionFilegroupMapping>? existingFilegroupMappings = null,
        PartitionSafetyRule? safetyRule = null,
        PartitionStorageSettings? storageSettings = null,
        PartitionTargetTable? targetTable = null,
        bool requirePartitionColumnNotNull = false,
        string? remarks = null,
        bool isCommitted = false)
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
        SafetyRule = safetyRule;
        StorageSettings = storageSettings ?? PartitionStorageSettings.UsePrimary(filegroupStrategy.PrimaryFilegroup);
        TargetTable = targetTable;
        RequirePartitionColumnNotNull = requirePartitionColumnNotNull;
        Remarks = string.IsNullOrWhiteSpace(remarks) ? null : remarks.Trim();
        IsCommitted = isCommitted;

        if (existingBoundaries is not null)
        {
            foreach (var boundary in existingBoundaries)
            {
                AddBoundaryInternal(boundary, skipValidation: true);
            }
        }

        if (existingFilegroupMappings is not null)
        {
            foreach (var mapping in existingFilegroupMappings)
            {
                filegroupMappings.Add(mapping);
            }
        }

        EnsureSortedOrder();
        EnsureMonotonicGrowth();
    }

    /// <summary>
    /// 将配置标记为已执行，后续不允许修改基础信息。
    /// </summary>
    /// <param name="user">操作人。</param>
    public void MarkCommitted(string user)
    {
        if (IsCommitted)
        {
            return;
        }

        IsCommitted = true;
        ExecutionStage = "Completed";
        Touch(string.IsNullOrWhiteSpace(user) ? DefaultAuditUser : user);
    }

    /// <summary>
    /// 更新执行阶段信息(向导流程使用)。
    /// </summary>
    /// <param name="stage">当前阶段名称(例如: PendingValidation/Validating/Queued/Running)</param>
    /// <param name="executionTaskId">关联的执行任务ID</param>
    /// <param name="user">操作人</param>
    public void UpdateExecutionStage(string stage, Guid executionTaskId, string user)
    {
        ExecutionStage = stage;
        LastExecutionTaskId = executionTaskId;
        Touch(string.IsNullOrWhiteSpace(user) ? DefaultAuditUser : user);
    }

    /// <summary>
    /// 将配置恢复为草稿状态（仅用于撤销执行的场景）。
    /// </summary>
    /// <param name="user">操作人。</param>
    public void MarkDraft(string user)
    {
        if (!IsCommitted)
        {
            return;
        }

        IsCommitted = false;
        Touch(string.IsNullOrWhiteSpace(user) ? DefaultAuditUser : user);
    }

    /// <summary>
    /// 尝试新增分区边界，确保遵循 RangeRight/RangeLeft 的单调性要求。
    /// </summary>
    /// <param name="boundary">待新增的分区边界。</param>
    public PartitionOperationResult TryAddBoundary(PartitionBoundary boundary)
    {
        if (boundary is null)
        {
            return PartitionOperationResult.Failure("边界信息不能为空。");
        }

        var validation = ValidateAppendBoundary(boundary);
        if (!validation.IsSatisfied)
        {
            return PartitionOperationResult.Failure(validation.ErrorMessage);
        }

        AddBoundaryInternal(boundary, skipValidation: false);
        Touch(DefaultAuditUser);
        return PartitionOperationResult.Success();
    }

    /// <summary>
    /// 尝试在现有边界之间插入新的分区边界（用于拆分场景）。
    /// </summary>
    public PartitionOperationResult TryInsertBoundary(PartitionBoundary boundary)
    {
        if (boundary is null)
        {
            return PartitionOperationResult.Failure("边界信息不能为空。");
        }

        var validation = ValidateInsertBoundary(boundary);
        if (!validation.IsSatisfied)
        {
            return PartitionOperationResult.Failure(validation.ErrorMessage);
        }

        AddBoundaryInternal(boundary, skipValidation: true);
        Touch(DefaultAuditUser);
        return PartitionOperationResult.Success();
    }

    /// <summary>
    /// 尝试移除指定分区边界，要求至少保留一个边界以保证配置有效。
    /// </summary>
    /// <param name="boundaryKey">分区边界排序键。</param>
    public PartitionOperationResult TryRemoveBoundary(string boundaryKey)
    {
        if (string.IsNullOrWhiteSpace(boundaryKey))
        {
            return PartitionOperationResult.Failure("边界标识不能为空。");
        }

        var index = boundaries.FindIndex(x => x.SortKey.Equals(boundaryKey, StringComparison.Ordinal));
        if (index < 0)
        {
            return PartitionOperationResult.Failure("未找到指定的分区边界。");
        }

        if (boundaries.Count <= 1)
        {
            return PartitionOperationResult.Failure("至少保留一个分区边界。");
        }

        boundaries.RemoveAt(index);
        RemoveFilegroupMapping(boundaryKey);
        Touch(DefaultAuditUser);
        return PartitionOperationResult.Success();
    }

    /// <summary>
    /// 为指定边界设置文件组映射，若已存在映射则覆盖。
    /// </summary>
    /// <param name="boundaryKey">分区边界排序键。</param>
    /// <param name="filegroupName">目标文件组名称。</param>
    public PartitionOperationResult TryAssignFilegroup(string boundaryKey, string filegroupName)
    {
        if (string.IsNullOrWhiteSpace(boundaryKey))
        {
            return PartitionOperationResult.Failure("分区边界标识不能为空。");
        }

        if (string.IsNullOrWhiteSpace(filegroupName))
        {
            return PartitionOperationResult.Failure("文件组名称不能为空。");
        }

        var exists = boundaries.Any(x => x.SortKey.Equals(boundaryKey, StringComparison.Ordinal));
        if (!exists)
        {
            return PartitionOperationResult.Failure("未找到对应的分区边界，无法设置文件组。");
        }

        RemoveFilegroupMapping(boundaryKey);
        filegroupMappings.Add(PartitionFilegroupMapping.Create(boundaryKey, filegroupName));
        Touch(DefaultAuditUser);
        return PartitionOperationResult.Success();
    }

    /// <summary>
    /// 更新分区存放策略。
    /// </summary>
    /// <param name="settings">新的存放设置。</param>
    public void UpdateStorageSettings(PartitionStorageSettings settings)
    {
        StorageSettings = settings ?? throw new ArgumentNullException(nameof(settings));
        Touch(DefaultAuditUser);
    }

    /// <summary>
    /// 更新目标表信息。
    /// </summary>
    /// <param name="targetTable">新的目标表配置。</param>
    public void UpdateTargetTable(PartitionTargetTable targetTable)
    {
        TargetTable = targetTable ?? throw new ArgumentNullException(nameof(targetTable));
        Touch(DefaultAuditUser);
    }

    /// <summary>
    /// 更新分区列 NOT NULL 要求。
    /// </summary>
    public void SetPartitionColumnNotNullRequirement(bool requireNotNull)
    {
        RequirePartitionColumnNotNull = requireNotNull;
        Touch(DefaultAuditUser);
    }

    /// <summary>
    /// 更新配置备注信息。
    /// </summary>
    /// <param name="remarks">备注内容，可为空。</param>
    public void UpdateRemarks(string? remarks)
    {
        Remarks = string.IsNullOrWhiteSpace(remarks) ? null : remarks.Trim();
        Touch(DefaultAuditUser);
    }

    /// <summary>
    /// 根据分区边界解析目标文件组，若无映射则返回默认文件组。
    /// </summary>
    /// <param name="boundaryKey">分区边界排序键。</param>
    public string ResolveFilegroup(string boundaryKey)
    {
        if (string.IsNullOrWhiteSpace(boundaryKey))
        {
            throw new ArgumentException("分区边界标识不能为空。", nameof(boundaryKey));
        }

        var mapping = filegroupMappings.FirstOrDefault(x => x.BoundaryKey.Equals(boundaryKey, StringComparison.Ordinal));
        return mapping?.FilegroupName ?? StorageSettings.FilegroupName;
    }

    /// <summary>
    /// 用新的边界集合替换当前配置，操作前会清空已有映射。
    /// </summary>
    /// <param name="values">按增序排列的分区值。</param>
    public PartitionOperationResult ReplaceBoundaries(IReadOnlyList<PartitionValue> values)
    {
        if (values is null || values.Count == 0)
        {
            return PartitionOperationResult.Failure("分区边界列表不能为空。");
        }

        var ordered = new List<PartitionValue>(values.Count);
        foreach (var value in values)
        {
            ordered.Add(value ?? throw new ArgumentException("分区边界值不能为空。", nameof(values)));
        }

        ordered.Sort((left, right) => left.CompareTo(right));

        if (ordered.Count == 0)
        {
            return PartitionOperationResult.Failure("分区边界列表不能为空。");
        }

        var unique = new List<PartitionValue>(ordered.Count);
        foreach (var value in ordered)
        {
            if (unique.Count == 0 || unique[^1].CompareTo(value) != 0)
            {
                unique.Add(value);
            }
        }

        boundaries.Clear();
        filegroupMappings.Clear();

        for (var index = 0; index < unique.Count; index++)
        {
            var boundaryKey = (index + 1).ToString("D4", System.Globalization.CultureInfo.InvariantCulture);
            boundaries.Add(new PartitionBoundary(boundaryKey, unique[index]));
        }

        EnsureSortedOrder();
        Touch(DefaultAuditUser);
        return PartitionOperationResult.Success();
    }

    /// <summary>
    /// 更新安全规则配置。
    /// </summary>
    /// <param name="safetyRule">安全规则对象。</param>
    public void UpdateSafetyRule(PartitionSafetyRule safetyRule)
    {
        SafetyRule = safetyRule ?? throw new ArgumentNullException(nameof(safetyRule));
        Touch(DefaultAuditUser);
    }

    /// <summary>
    /// 清空安全规则配置。
    /// </summary>
    public void ClearSafetyRule()
    {
        SafetyRule = null;
        Touch(DefaultAuditUser);
    }

    /// <summary>
    /// 在内部写操作中添加边界，可选择跳过验证（用于初始化）。
    /// </summary>
    /// <param name="boundary">分区边界对象。</param>
    /// <param name="skipValidation">是否跳过验证。</param>
    private void AddBoundaryInternal(PartitionBoundary boundary, bool skipValidation)
    {
        if (boundary is null)
        {
            throw new ArgumentNullException(nameof(boundary));
        }

        if (!skipValidation)
        {
            var validation = ValidateAppendBoundary(boundary);
            if (!validation.IsSatisfied)
            {
                throw new InvalidOperationException(validation.ErrorMessage);
            }
        }

        boundaries.Add(boundary);
        EnsureSortedOrder();
    }

    /// <summary>
    /// 移除与指定边界相关的文件组映射。
    /// </summary>
    /// <param name="boundaryKey">分区边界排序键。</param>
    private void RemoveFilegroupMapping(string boundaryKey)
    {
        var index = filegroupMappings.FindIndex(x => x.BoundaryKey.Equals(boundaryKey, StringComparison.Ordinal));
        if (index >= 0)
        {
            filegroupMappings.RemoveAt(index);
        }
    }

    /// <summary>
    /// 校验待新增的分区边界是否满足单调性与唯一性要求。
    /// </summary>
    /// <param name="boundary">待校验的分区边界。</param>
    private PartitionBoundaryValidation ValidateAppendBoundary(PartitionBoundary boundary)
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
    /// 校验在拆分场景下插入新的分区边界是否满足严格递增要求。
    /// </summary>
    private PartitionBoundaryValidation ValidateInsertBoundary(PartitionBoundary boundary)
    {
        if (boundaries.Any(x => x.SortKey.Equals(boundary.SortKey, StringComparison.Ordinal)))
        {
            return PartitionBoundaryValidation.NotSatisfied("已存在相同的分区边界。");
        }

        if (boundaries.Count == 0)
        {
            return PartitionBoundaryValidation.Satisfied();
        }

        const string invalidMessage = "分区边界值必须严格递增，且不能与现有边界相同。";
        var sorted = boundaries.OrderBy(x => x).ToList();

        for (var i = 0; i < sorted.Count; i++)
        {
            var comparison = boundary.CompareTo(sorted[i]);
            if (comparison == 0)
            {
                return PartitionBoundaryValidation.NotSatisfied("分区边界值已存在。");
            }

            if (comparison < 0)
            {
                if (i > 0 && boundary.CompareTo(sorted[i - 1]) <= 0)
                {
                    return PartitionBoundaryValidation.NotSatisfied(invalidMessage);
                }

                return PartitionBoundaryValidation.Satisfied();
            }
        }

        // 大于当前所有边界，直接通过
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
/// 表示领域层内的操作结果，携带失败原因。
/// </summary>
public sealed record PartitionOperationResult(bool IsSuccess, string? ErrorMessage)
{
    /// <summary>创建成功结果。</summary>
    public static PartitionOperationResult Success() => new(true, null);

    /// <summary>创建失败结果。</summary>
    public static PartitionOperationResult Failure(string message) => new(false, message);
}

/// <summary>
/// 表示校验边界时的判断结果，包含是否通过与提示信息。
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
