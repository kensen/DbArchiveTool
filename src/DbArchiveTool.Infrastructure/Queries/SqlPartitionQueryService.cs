using System.Data;
using Dapper;
using DbArchiveTool.Infrastructure.Persistence;
using Microsoft.Data.SqlClient;

namespace DbArchiveTool.Infrastructure.Queries;

/// <summary>
/// SQL Server分区信息查询服务
/// </summary>
public class SqlPartitionQueryService
{
    /// <summary>
    /// 查询数据库中所有分区表信息
    /// </summary>
    public async Task<List<PartitionTableDto>> GetPartitionTablesAsync(string connectionString)
    {
        const string sql = @"
SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    pf.name AS PartitionFunction,
    ps.name AS PartitionScheme,
    c.name AS PartitionColumn,
    ty.name AS DataType,
    pf.boundary_value_on_right AS IsRangeRight,
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
GROUP BY 
    s.name, t.name, pf.name, ps.name, c.name, ty.name, pf.boundary_value_on_right
ORDER BY s.name, t.name";

        using var connection = new SqlConnection(connectionString);
        var result = await connection.QueryAsync<PartitionTableDto>(sql);
        return result.ToList();
    }

    /// <summary>
    /// 查询指定分区表的分区边界值明细
    /// </summary>
    public async Task<List<PartitionDetailDto>> GetPartitionDetailsAsync(
        string connectionString, 
        string schemaName, 
        string tableName)
    {
        const string sql = @"
SELECT 
    p.partition_number AS PartitionNumber,
    ISNULL(CAST(prv.value AS NVARCHAR(100)), 'N/A') AS BoundaryValue,
    CASE WHEN pf.boundary_value_on_right = 1 THEN 'RIGHT' ELSE 'LEFT' END AS RangeType,
    fg.name AS FilegroupName,
    p.rows AS RowCount,
    CAST(SUM(au.total_pages) * 8.0 / 1024 AS DECIMAL(18,2)) AS TotalSpaceMB,
    p.data_compression_desc AS DataCompression
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id <= 1
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units au ON p.partition_id = au.container_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id AND p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
LEFT JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id AND p.partition_number = prv.boundary_id
WHERE s.name = @SchemaName AND t.name = @TableName
GROUP BY 
    p.partition_number, prv.value, pf.boundary_value_on_right, fg.name, p.rows, p.data_compression_desc
ORDER BY p.partition_number";

        using var connection = new SqlConnection(connectionString);
        var result = await connection.QueryAsync<PartitionDetailDto>(sql, new { SchemaName = schemaName, TableName = tableName });
        return result.ToList();
    }
}

/// <summary>
/// 分区表信息DTO
/// </summary>
public class PartitionTableDto
{
    /// <summary>架构名</summary>
    public string SchemaName { get; set; } = "";
    /// <summary>表名</summary>
    public string TableName { get; set; } = "";
    /// <summary>分区函数</summary>
    public string PartitionFunction { get; set; } = "";
    /// <summary>分区方案</summary>
    public string PartitionScheme { get; set; } = "";
    /// <summary>分区列</summary>
    public string PartitionColumn { get; set; } = "";
    /// <summary>数据类型</summary>
    public string DataType { get; set; } = "";
    /// <summary>是否RIGHT分区</summary>
    public bool IsRangeRight { get; set; }
    /// <summary>分区总数</summary>
    public int TotalPartitions { get; set; }
}

/// <summary>
/// 分区明细信息DTO
/// </summary>
public class PartitionDetailDto
{
    /// <summary>分区号</summary>
    public int PartitionNumber { get; set; }
    /// <summary>边界值</summary>
    public string BoundaryValue { get; set; } = "";
    /// <summary>范围类型</summary>
    public string RangeType { get; set; } = "";
    /// <summary>文件组名称</summary>
    public string FilegroupName { get; set; } = "";
    /// <summary>行数</summary>
    public long RowCount { get; set; }
    /// <summary>占用空间(MB)</summary>
    public decimal TotalSpaceMB { get; set; }
    /// <summary>数据压缩方式</summary>
    public string DataCompression { get; set; } = "NONE";
}
