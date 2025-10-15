using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 定义分区管理相关的应用服务契约。
/// </summary>
public interface IPartitionManagementAppService
{
    /// <summary>获取目标表的分区概览信息。</summary>
    Task<Result<PartitionOverviewDto>> GetOverviewAsync(PartitionOverviewRequest request, CancellationToken cancellationToken = default);

    /// <summary>查询指定分区边界的安全状态。</summary>
    Task<Result<PartitionBoundarySafetyDto>> GetBoundarySafetyAsync(PartitionBoundarySafetyRequest request, CancellationToken cancellationToken = default);

    /// <summary>获取目标已分区表的元数据信息(分区列、类型、边界值、文件组映射等)。</summary>
    Task<Result<PartitionMetadataDto>> GetPartitionMetadataAsync(PartitionMetadataRequest request, CancellationToken cancellationToken = default);

    /// <summary>为已分区表添加新的分区边界值。</summary>
    Task<Result> AddBoundaryToPartitionedTableAsync(AddBoundaryToPartitionedTableRequest request, CancellationToken cancellationToken = default);
}

/// <summary>
/// 分区概览请求。
/// </summary>
public sealed record PartitionOverviewRequest(Guid DataSourceId, string SchemaName, string TableName);

/// <summary>
/// 分区概览返回 DTO。
/// </summary>
public sealed record PartitionOverviewDto(
    string TableName,
    bool IsRangeRight,
    IReadOnlyList<PartitionBoundaryItemDto> Boundaries);

/// <summary>
/// 分区概览中的单个边界项。
/// </summary>
public sealed record PartitionBoundaryItemDto(string Key, string LiteralValue);

/// <summary>
/// 分区安全请求。
/// </summary>
public sealed record PartitionBoundarySafetyRequest(Guid DataSourceId, string SchemaName, string TableName, string BoundaryKey);

/// <summary>
/// 分区安全返回 DTO。
/// </summary>
public sealed record PartitionBoundarySafetyDto(string BoundaryKey, long RowCount, bool HasData, bool SuggestSwitch);

/// <summary>
/// 分区元数据请求。
/// </summary>
public sealed record PartitionMetadataRequest(Guid DataSourceId, string SchemaName, string TableName);

/// <summary>
/// 分区元数据返回DTO,包含列名、类型、边界值列表、文件组映射等信息。
/// </summary>
public sealed record PartitionMetadataDto(
    /// <summary>分区列名称</summary>
    string ColumnName,
    /// <summary>分区列的数据类型(如int, datetime, uniqueidentifier等)</summary>
    string ColumnType,
    /// <summary>列是否可为空</summary>
    bool IsNullable,
    /// <summary>是否为RIGHT分区(true=RIGHT, false=LEFT)</summary>
    bool IsRangeRight,
    /// <summary>分区函数名称</summary>
    string PartitionFunctionName,
    /// <summary>分区方案名称</summary>
    string PartitionSchemeName,
    /// <summary>所有分区边界值列表</summary>
    IReadOnlyList<PartitionBoundaryItemDto> Boundaries,
    /// <summary>分区到文件组的映射关系</summary>
    IReadOnlyList<PartitionFilegroupMappingDto> FilegroupMappings);

/// <summary>
/// 分区文件组映射项。
/// </summary>
public sealed record PartitionFilegroupMappingDto(
    /// <summary>分区号(从1开始)</summary>
    int PartitionNumber,
    /// <summary>文件组名称</summary>
    string FilegroupName);

/// <summary>
/// 为已分区表添加边界值请求。
/// </summary>
public sealed record AddBoundaryToPartitionedTableRequest(
    /// <summary>数据源ID</summary>
    Guid DataSourceId,
    /// <summary>架构名</summary>
    string SchemaName,
    /// <summary>表名</summary>
    string TableName,
    /// <summary>边界值(字符串格式,需解析)</summary>
    string BoundaryValue,
    /// <summary>文件组名称(可选)</summary>
    string? FilegroupName,
    /// <summary>操作人</summary>
    string RequestedBy,
    /// <summary>备注</summary>
    string? Notes);
