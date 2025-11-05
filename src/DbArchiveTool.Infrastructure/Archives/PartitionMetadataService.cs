using DbArchiveTool.Application.Archives;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Archives;

/// <summary>
/// 分区元数据服务实现
/// 查询 SQL Server 系统视图获取分区信息,支持自动检测分区表
/// </summary>
public class PartitionMetadataService : IPartitionMetadataService
{
    private readonly ISqlExecutor _sqlExecutor;
    private readonly ILogger<PartitionMetadataService> _logger;

    public PartitionMetadataService(
        ISqlExecutor sqlExecutor,
        ILogger<PartitionMetadataService> logger)
    {
        _sqlExecutor = sqlExecutor;
        _logger = logger;
    }

    /// <summary>检查表是否为分区表</summary>
    public async Task<bool> IsPartitionedTableAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        const string sql = @"
SELECT 
    CASE WHEN EXISTS (
        SELECT 1
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id <= 1
        INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
        WHERE s.name = @SchemaName AND t.name = @TableName
    ) THEN 1 ELSE 0 END AS IsPartitioned";

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var result = await _sqlExecutor.QuerySingleAsync<int>(
            connection,
            sql,
            new { SchemaName = schemaName, TableName = tableName });

        var isPartitioned = result == 1;

        _logger.LogDebug(
            "检查分区表: {Schema}.{Table} = {IsPartitioned}",
            schemaName, tableName, isPartitioned);

        return isPartitioned;
    }

    /// <summary>获取分区表的详细信息</summary>
    public async Task<PartitionInfo?> GetPartitionInfoAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        const string sql = @"
SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    pf.name AS PartitionFunction,
    ps.name AS PartitionScheme,
    c.name AS PartitionColumn,
    ty.name AS DataType,
    CASE WHEN pf.boundary_value_on_right = 1 THEN 'RIGHT' ELSE 'LEFT' END AS RangeType,
    COUNT(DISTINCT p.partition_number) AS TotalPartitions
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id <= 1
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND ic.partition_ordinal = 1
INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND i.index_id = p.index_id
WHERE s.name = @SchemaName AND t.name = @TableName
GROUP BY 
    s.name, t.name, pf.name, ps.name, c.name, ty.name, pf.boundary_value_on_right";

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var result = await _sqlExecutor.QuerySingleAsync<PartitionInfo>(
            connection,
            sql,
            new { SchemaName = schemaName, TableName = tableName });

        if (result != null)
        {
            _logger.LogInformation(
                "获取分区信息: {Schema}.{Table}, 分区函数={Function}, 分区数={PartitionCount}",
                schemaName, tableName, result.PartitionFunction, result.TotalPartitions);
        }
        else
        {
            _logger.LogDebug(
                "表 {Schema}.{Table} 不是分区表或不存在",
                schemaName, tableName);
        }

        return result;
    }

    /// <summary>获取分区表的所有分区详情</summary>
    public async Task<List<PartitionDetail>> GetPartitionDetailsAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        const string sql = @"
SELECT 
    p.partition_number AS PartitionNumber,
    ISNULL(CAST(prv.value AS NVARCHAR(100)), 'N/A') AS BoundaryValue,
    CASE WHEN pf.boundary_value_on_right = 1 THEN 'RIGHT' ELSE 'LEFT' END AS RangeType,
    p.rows AS RowCount,
    CAST(COALESCE(SUM(au.total_pages), 0) * 8.0 / 1024 AS DECIMAL(18,2)) AS TotalSpaceMB,
    ISNULL(p.data_compression_desc, 'NONE') AS DataCompression,
    fg.name AS FileGroupName
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id <= 1
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND i.index_id = p.index_id
LEFT JOIN sys.allocation_units au ON p.partition_id = au.container_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id AND p.partition_number = prv.boundary_id
INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id AND p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE s.name = @SchemaName AND t.name = @TableName
GROUP BY 
    p.partition_number, prv.value, pf.boundary_value_on_right, p.rows, p.data_compression_desc, fg.name
ORDER BY p.partition_number";

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var result = await _sqlExecutor.QueryAsync<PartitionDetail>(
            connection,
            sql,
            new { SchemaName = schemaName, TableName = tableName });

        var partitions = result.ToList();

        _logger.LogInformation(
            "获取分区详情: {Schema}.{Table}, 共 {PartitionCount} 个分区",
            schemaName, tableName, partitions.Count);

        return partitions;
    }

    /// <summary>根据条件查找需要归档的分区号列表</summary>
    public async Task<List<int>> GetPartitionsToArchiveAsync(
        string connectionString,
        string schemaName,
        string tableName,
        string boundaryCondition,
        CancellationToken cancellationToken = default)
    {
        // 注意:这个方法需要根据具体的分区函数数据类型动态构建 SQL
        // 这里提供基础实现,实际使用时可能需要根据数据类型调整
        var sql = $@"
SELECT DISTINCT p.partition_number AS PartitionNumber
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id <= 1
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id AND p.partition_number = prv.boundary_id
WHERE s.name = @SchemaName 
  AND t.name = @TableName
  AND prv.value IS NOT NULL
  AND {boundaryCondition}
ORDER BY p.partition_number";

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var result = await _sqlExecutor.QueryAsync<int>(
            connection,
            sql,
            new { SchemaName = schemaName, TableName = tableName });

        var partitions = result.ToList();

        _logger.LogInformation(
            "查找需要归档的分区: {Schema}.{Table}, 条件={Condition}, 找到 {Count} 个分区",
            schemaName, tableName, boundaryCondition, partitions.Count);

        return partitions;
    }
}
