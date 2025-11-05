namespace DbArchiveTool.Application.Archives;

/// <summary>
/// 分区元数据服务接口
/// 查询 SQL Server 系统视图获取分区信息,支持自动检测分区表
/// </summary>
public interface IPartitionMetadataService
{
    /// <summary>
    /// 检查表是否为分区表
    /// </summary>
    /// <param name="connectionString">数据库连接字符串</param>
    /// <param name="schemaName">架构名称</param>
    /// <param name="tableName">表名称</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>是否为分区表</returns>
    Task<bool> IsPartitionedTableAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取分区表的详细信息
    /// </summary>
    /// <param name="connectionString">数据库连接字符串</param>
    /// <param name="schemaName">架构名称</param>
    /// <param name="tableName">表名称</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>分区信息,如果不是分区表则返回 null</returns>
    Task<PartitionInfo?> GetPartitionInfoAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取分区表的所有分区详情
    /// </summary>
    /// <param name="connectionString">数据库连接字符串</param>
    /// <param name="schemaName">架构名称</param>
    /// <param name="tableName">表名称</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>分区详情列表</returns>
    Task<List<PartitionDetail>> GetPartitionDetailsAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 根据条件查找需要归档的分区号列表
    /// </summary>
    /// <param name="connectionString">数据库连接字符串</param>
    /// <param name="schemaName">架构名称</param>
    /// <param name="tableName">表名称</param>
    /// <param name="boundaryCondition">边界条件 (如: value < '2024-01-01')</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>符合条件的分区号列表</returns>
    Task<List<int>> GetPartitionsToArchiveAsync(
        string connectionString,
        string schemaName,
        string tableName,
        string boundaryCondition,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 分区信息
/// </summary>
public class PartitionInfo
{
    /// <summary>架构名称</summary>
    public string SchemaName { get; set; } = string.Empty;

    /// <summary>表名称</summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>分区函数名称</summary>
    public string PartitionFunction { get; set; } = string.Empty;

    /// <summary>分区方案名称</summary>
    public string PartitionScheme { get; set; } = string.Empty;

    /// <summary>分区列名称</summary>
    public string PartitionColumn { get; set; } = string.Empty;

    /// <summary>分区列数据类型</summary>
    public string DataType { get; set; } = string.Empty;

    /// <summary>分区范围类型 (LEFT/RIGHT)</summary>
    public string RangeType { get; set; } = string.Empty;

    /// <summary>总分区数</summary>
    public int TotalPartitions { get; set; }
}

/// <summary>
/// 分区详情
/// </summary>
public class PartitionDetail
{
    /// <summary>分区号</summary>
    public int PartitionNumber { get; set; }

    /// <summary>边界值</summary>
    public string BoundaryValue { get; set; } = string.Empty;

    /// <summary>范围类型 (LEFT/RIGHT)</summary>
    public string RangeType { get; set; } = string.Empty;

    /// <summary>行数</summary>
    public long RowCount { get; set; }

    /// <summary>占用空间 (MB)</summary>
    public decimal TotalSpaceMB { get; set; }

    /// <summary>数据压缩方式</summary>
    public string DataCompression { get; set; } = string.Empty;

    /// <summary>文件组名称</summary>
    public string FileGroupName { get; set; } = string.Empty;
}
