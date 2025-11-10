using DbArchiveTool.Domain.Abstractions;
using DbArchiveTool.Shared.Archive;

namespace DbArchiveTool.Domain.ArchiveConfigurations;

/// <summary>
/// 归档配置实体,定义如何归档特定表的数据
/// 支持独立于分区配置的归档流程
/// </summary>
public sealed class ArchiveConfiguration : AggregateRoot
{
    /// <summary>归档配置名称</summary>
    public string Name { get; private set; } = string.Empty;

    /// <summary>归档配置描述</summary>
    public string? Description { get; private set; }

    /// <summary>数据源ID (关联 ArchiveDataSource)</summary>
    public Guid DataSourceId { get; private set; }

    /// <summary>源表所属架构名称</summary>
    public string SourceSchemaName { get; private set; } = "dbo";

    /// <summary>源表名称</summary>
    public string SourceTableName { get; private set; } = string.Empty;

    /// <summary>目标表架构,为空时默认使用源表架构</summary>
    public string? TargetSchemaName { get; private set; }

    /// <summary>目标表名称,为空时默认使用源表名称</summary>
    public string? TargetTableName { get; private set; }

    /// <summary>源表是否为分区表</summary>
    public bool IsPartitionedTable { get; private set; }

    /// <summary>分区配置ID (可选,仅当 IsPartitionedTable=true 且需要关联工具管理的分区配置时填写)</summary>
    public Guid? PartitionConfigurationId { get; private set; }

    /// <summary>归档过滤列名 (用于构建 WHERE 条件,如 CreateDate)</summary>
    public string? ArchiveFilterColumn { get; private set; }

    /// <summary>归档过滤条件 (如 &lt; DATEADD(year, -1, GETDATE()))</summary>
    public string? ArchiveFilterCondition { get; private set; }

    /// <summary>归档方法</summary>
    public ArchiveMethod ArchiveMethod { get; private set; }

    /// <summary>是否在归档后删除源数据</summary>
    public bool DeleteSourceDataAfterArchive { get; private set; } = true;

    /// <summary>批次大小</summary>
    public int BatchSize { get; private set; } = 10000;

    /// <summary>是否启用定时归档</summary>
    public bool EnableScheduledArchive { get; private set; }

    /// <summary>Cron 表达式</summary>
    public string? CronExpression { get; private set; }

    /// <summary>下一次归档时间(UTC)</summary>
    public DateTime? NextArchiveAtUtc { get; private set; }

    /// <summary>是否启用</summary>
    public bool IsEnabled { get; private set; } = true;

    /// <summary>上次执行时间 (UTC)</summary>
    public DateTime? LastExecutionTimeUtc { get; private set; }

    /// <summary>上次执行状态 (Success/Failed)</summary>
    public string? LastExecutionStatus { get; private set; }

    /// <summary>上次归档行数</summary>
    public long? LastArchivedRowCount { get; private set; }

    /// <summary>仅供 ORM 使用的无参构造函数</summary>
    private ArchiveConfiguration() { }

    /// <summary>创建归档配置</summary>
    public ArchiveConfiguration(
        string name,
        string? description,
        Guid dataSourceId,
        string sourceSchemaName,
        string sourceTableName,
        bool isPartitionedTable,
        string? archiveFilterColumn,
        string? archiveFilterCondition,
        ArchiveMethod archiveMethod,
        bool deleteSourceDataAfterArchive = true,
        int batchSize = 10000,
        Guid? partitionConfigurationId = null,
        string? targetSchemaName = null,
        string? targetTableName = null,
        bool enableScheduledArchive = false,
        string? cronExpression = null,
        DateTime? nextArchiveAtUtc = null)
    {
        Update(
            name,
            description,
            dataSourceId,
            sourceSchemaName,
            sourceTableName,
            isPartitionedTable,
            archiveFilterColumn,
            archiveFilterCondition,
            archiveMethod,
            deleteSourceDataAfterArchive,
            batchSize,
            partitionConfigurationId,
            targetSchemaName,
            targetTableName,
            enableScheduledArchive,
            cronExpression,
            nextArchiveAtUtc);
    }

    /// <summary>更新归档配置</summary>
    public void Update(
        string name,
        string? description,
        Guid dataSourceId,
        string sourceSchemaName,
        string sourceTableName,
        bool isPartitionedTable,
        string? archiveFilterColumn,
        string? archiveFilterCondition,
        ArchiveMethod archiveMethod,
        bool deleteSourceDataAfterArchive = true,
        int batchSize = 10000,
        Guid? partitionConfigurationId = null,
        string? targetSchemaName = null,
        string? targetTableName = null,
        bool enableScheduledArchive = false,
        string? cronExpression = null,
        DateTime? nextArchiveAtUtc = null,
        string operatorName = "SYSTEM")
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("归档配置名称不能为空", nameof(name));
        }

        if (dataSourceId == Guid.Empty)
        {
            throw new ArgumentException("数据源ID不能为空", nameof(dataSourceId));
        }

        if (string.IsNullOrWhiteSpace(sourceSchemaName))
        {
            throw new ArgumentException("源架构名称不能为空", nameof(sourceSchemaName));
        }

        if (string.IsNullOrWhiteSpace(sourceTableName))
        {
            throw new ArgumentException("源表名称不能为空", nameof(sourceTableName));
        }

        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "批次大小必须大于 0");
        }

        // 如果不是分区表,使用 Bcp 或 BulkCopy 方法,必须提供过滤条件
        if (!isPartitionedTable && archiveMethod != ArchiveMethod.PartitionSwitch)
        {
            if (string.IsNullOrWhiteSpace(archiveFilterColumn) || string.IsNullOrWhiteSpace(archiveFilterCondition))
            {
                throw new ArgumentException("非分区表归档必须提供过滤列和过滤条件");
            }
        }

        // 如果是分区表且使用 PartitionSwitch 方法,必须关联分区配置
        if (isPartitionedTable && archiveMethod == ArchiveMethod.PartitionSwitch && !partitionConfigurationId.HasValue)
        {
            throw new ArgumentException("分区表使用 PartitionSwitch 方法必须关联分区配置", nameof(partitionConfigurationId));
        }

        if (enableScheduledArchive && string.IsNullOrWhiteSpace(cronExpression))
        {
            throw new ArgumentException("启用定时归档时必须提供 Cron 表达式", nameof(cronExpression));
        }

        if (!string.IsNullOrWhiteSpace(targetSchemaName))
        {
            if (targetSchemaName.Length > 128)
            {
                throw new ArgumentException("目标架构名称长度不能超过 128", nameof(targetSchemaName));
            }
        }

        if (!string.IsNullOrWhiteSpace(targetTableName))
        {
            if (targetTableName.Length > 128)
            {
                throw new ArgumentException("目标表名称长度不能超过 128", nameof(targetTableName));
            }
        }

        Name = name.Trim();
        Description = string.IsNullOrWhiteSpace(description) ? null : description.Trim();
        DataSourceId = dataSourceId;
        SourceSchemaName = sourceSchemaName.Trim();
        SourceTableName = sourceTableName.Trim();
        IsPartitionedTable = isPartitionedTable;
        PartitionConfigurationId = partitionConfigurationId;
        ArchiveFilterColumn = string.IsNullOrWhiteSpace(archiveFilterColumn) ? null : archiveFilterColumn.Trim();
        ArchiveFilterCondition = string.IsNullOrWhiteSpace(archiveFilterCondition) ? null : archiveFilterCondition.Trim();
        ArchiveMethod = archiveMethod;
        DeleteSourceDataAfterArchive = deleteSourceDataAfterArchive;
        BatchSize = batchSize;
        TargetSchemaName = string.IsNullOrWhiteSpace(targetSchemaName) ? null : targetSchemaName.Trim();
        TargetTableName = string.IsNullOrWhiteSpace(targetTableName) ? null : targetTableName.Trim();
        EnableScheduledArchive = enableScheduledArchive;
        CronExpression = enableScheduledArchive ? cronExpression?.Trim() : null;
        NextArchiveAtUtc = enableScheduledArchive ? nextArchiveAtUtc : null;

        Touch(operatorName);
    }

    /// <summary>更新最后执行信息</summary>
    public void UpdateLastExecution(
        DateTime executionTimeUtc,
        string status,
        long archivedRowCount,
        string operatorName = "SYSTEM",
        DateTime? nextArchiveAtUtc = null)
    {
        LastExecutionTimeUtc = executionTimeUtc;
        LastExecutionStatus = status;
        LastArchivedRowCount = archivedRowCount;
        if (EnableScheduledArchive)
        {
            NextArchiveAtUtc = nextArchiveAtUtc;
        }
        Touch(operatorName);
    }

    /// <summary>启用配置</summary>
    public void Enable(string operatorName = "SYSTEM")
    {
        IsEnabled = true;
        Touch(operatorName);
    }

    /// <summary>禁用配置</summary>
    public void Disable(string operatorName = "SYSTEM")
    {
        IsEnabled = false;
        Touch(operatorName);
    }
}
