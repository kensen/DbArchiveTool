using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;
using Microsoft.Data.SqlClient;

namespace DbArchiveTool.Infrastructure.Queries;

/// <summary>
/// SQL Server 分区与元数据查询。
/// </summary>
public class SqlPartitionQueryService
{
    /// <summary>
    /// 查询数据库中已有的分区表信息。
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

        await using var connection = new SqlConnection(connectionString);
        var result = await connection.QueryAsync<PartitionTableDto>(sql);
        return result.ToList();
    }

    /// <summary>
    /// 查询数据库中所有用户表信息。
    /// </summary>
    public async Task<List<DatabaseTableDto>> GetDatabaseTablesAsync(string connectionString)
    {
        const string sql = @"
SELECT 
    s.name AS SchemaName,
    t.name AS TableName
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name";

        await using var connection = new SqlConnection(connectionString);
        var result = await connection.QueryAsync<DatabaseTableDto>(sql);
        return result.ToList();
    }

    /// <summary>
    /// 查询指定表的分区边界详情。
    /// </summary>
    public async Task<List<PartitionDetailDto>> GetPartitionDetailsAsync(
        string connectionString,
        string schemaName,
        string tableName)
    {
        const string detailSql = @"
SELECT 
    p.partition_number AS PartitionNumber,
    ISNULL(CAST(prv.value AS NVARCHAR(100)), 'N/A') AS BoundaryValue,
    CASE WHEN pf.boundary_value_on_right = 1 THEN 'RIGHT' ELSE 'LEFT' END AS RangeType,
    p.rows AS [RowCount],
    CAST(COALESCE(SUM(au.total_pages), 0) * 8.0 / 1024 AS DECIMAL(18,2)) AS TotalSpaceMB,
    ISNULL(p.data_compression_desc, 'NONE') AS DataCompression
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id <= 1
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND i.index_id = p.index_id
LEFT JOIN sys.allocation_units au ON p.partition_id = au.container_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id AND p.partition_number = prv.boundary_id
WHERE s.name = @SchemaName AND t.name = @TableName
GROUP BY 
    p.partition_number, prv.value, pf.boundary_value_on_right, p.rows, p.data_compression_desc
ORDER BY p.partition_number";

        const string detailFallbackSql = @"
SELECT 
    p.partition_number AS PartitionNumber,
    ISNULL(CAST(prv.value AS NVARCHAR(100)), 'N/A') AS BoundaryValue,
    CASE WHEN pf.boundary_value_on_right = 1 THEN 'RIGHT' ELSE 'LEFT' END AS RangeType,
    p.rows AS [RowCount],
    ISNULL(p.data_compression_desc, 'NONE') AS DataCompression
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id <= 1
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id AND p.partition_number = prv.boundary_id
WHERE s.name = @SchemaName AND t.name = @TableName
GROUP BY 
    p.partition_number, prv.value, pf.boundary_value_on_right, p.rows, p.data_compression_desc
ORDER BY p.partition_number";

        const string filegroupSql = @"
SELECT DISTINCT
    p.partition_number AS PartitionNumber,
    fg.name AS FilegroupName
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id <= 1
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id AND p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE s.name = @SchemaName AND t.name = @TableName";

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        List<PartitionDetailRow> detailRows;
        try
        {
            detailRows = (await connection.QueryAsync<PartitionDetailRow>(detailSql, new { SchemaName = schemaName, TableName = tableName })).ToList();
        }
        catch (SqlException)
        {
            // SQL Server can deny access to allocation units without VIEW DATABASE STATE; fall back to a lightweight query.
            var fallbackRows = (await connection.QueryAsync<PartitionDetailFallbackRow>(detailFallbackSql, new { SchemaName = schemaName, TableName = tableName })).ToList();
            detailRows = fallbackRows.Select(row => new PartitionDetailRow
            {
                PartitionNumber = row.PartitionNumber,
                BoundaryValue = row.BoundaryValue,
                RangeType = row.RangeType,
                RowCount = row.RowCount,
                DataCompression = row.DataCompression,
                TotalSpaceMB = 0m
            }).ToList();
        }

        IReadOnlyDictionary<int, string> filegroupLookup = new Dictionary<int, string>();
        try
        {
            var filegroupRows = await connection.QueryAsync<PartitionFilegroupRow>(filegroupSql, new { SchemaName = schemaName, TableName = tableName });
            filegroupLookup = filegroupRows
                .GroupBy(row => row.PartitionNumber)
                .ToDictionary(group => group.Key, group => group.First().FilegroupName);
        }
        catch (SqlException ex) when (ex.Number is 229 or 297)
        {
            // Lack of permission to inspect destination_data_spaces should not block partition details.
            filegroupLookup = new Dictionary<int, string>();
        }

        return detailRows.Select(row =>
        {
            filegroupLookup.TryGetValue(row.PartitionNumber, out var filegroup);
            return new PartitionDetailDto
            {
                PartitionNumber = row.PartitionNumber,
                BoundaryValue = row.BoundaryValue,
                RangeType = row.RangeType,
                FilegroupName = filegroup ?? "N/A",
                RowCount = row.RowCount,
                TotalSpaceMB = row.TotalSpaceMB,
                DataCompression = row.DataCompression ?? "NONE"
            };
        }).ToList();
    }

    /// <summary>
    /// 查询指定表的列信息。
    /// </summary>
    public async Task<List<PartitionTableColumnDto>> GetTableColumnsAsync(
        string connectionString,
        string schemaName,
        string tableName)
    {
        const string sql = @"
SELECT 
    c.name AS ColumnName,
    t.name AS DataType,
    c.is_nullable AS IsNullable,
    c.max_length AS MaxLength,
    c.precision AS [Precision],
    c.scale AS Scale
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id AND t.is_table_type = 0
WHERE c.object_id = OBJECT_ID(@FullName)
ORDER BY c.column_id;";

        var fullName = BuildFullName(schemaName, tableName);

        await using var connection = new SqlConnection(connectionString);
        var columns = await connection.QueryAsync<PartitionTableColumnDto>(sql, new { FullName = fullName });
        return columns.Select(column =>
        {
            column.DisplayType = BuildDisplayType(column.DataType, column.MaxLength, column.Precision, column.Scale);
            return column;
        }).ToList();
    }

    /// <summary>
    /// 查询指定列的最小值、最大值等统计数据。
    /// </summary>
    public async Task<PartitionColumnStatisticsDto> GetColumnStatisticsAsync(
        string connectionString,
        string schemaName,
        string tableName,
        string columnName)
    {
        // 🚀 性能优化：分阶段查询，避免大表全表扫描
        
        // 第一步：快速获取行数估算（使用系统表，秒级响应）
        const string rowCountSql = @"
SELECT SUM(p.rows) AS EstimatedRows
FROM sys.partitions p
INNER JOIN sys.tables t ON p.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName 
  AND t.name = @TableName
  AND p.index_id IN (0, 1);";

        await using var connection = new SqlConnection(connectionString);
        var estimatedRows = await connection.ExecuteScalarAsync<long?>(rowCountSql, new
        {
            SchemaName = schemaName,
            TableName = tableName
        }) ?? 0;

        // 第二步：根据表大小选择不同的统计策略
        if (estimatedRows > 5_000_000)
        {
            // 🚀 超大表（500万+）：使用采样估算，避免全表扫描
            // 采样策略：随机采样 10000 行进行边界估算
            var stmt = $@"
-- 采样模式：快速估算（适用于超大表）
SELECT 
    MIN(CONVERT(NVARCHAR(4000), {QuoteIdentifier(columnName)})) AS MinValue,
    MAX(CONVERT(NVARCHAR(4000), {QuoteIdentifier(columnName)})) AS MaxValue
FROM (
    SELECT TOP 10000 {QuoteIdentifier(columnName)}
    FROM {BuildFullName(schemaName, tableName)} WITH (NOLOCK)
    WHERE {QuoteIdentifier(columnName)} IS NOT NULL
    ORDER BY NEWID()  -- 随机采样
) AS SampleData;";

            try
            {
                // 设置 5 秒超时：如果采样也超时，说明表太大或锁太多
                using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(5));
                
                var bounds = await connection.QuerySingleAsync<MinMaxResult>(new CommandDefinition(
                    commandText: stmt,
                    cancellationToken: cts.Token
                ));

                return new PartitionColumnStatisticsDto
                {
                    MinValue = bounds.MinValue != null ? $"~{bounds.MinValue} (采样)" : "无法计算",
                    MaxValue = bounds.MaxValue != null ? $"~{bounds.MaxValue} (采样)" : "无法计算",
                    TotalRows = estimatedRows,
                    DistinctRows = null
                };
            }
            catch (Exception)
            {
                // 采样失败：返回占位符
                return new PartitionColumnStatisticsDto
                {
                    MinValue = "表过大，无法计算",
                    MaxValue = "表过大，无法计算",
                    TotalRows = estimatedRows,
                    DistinctRows = null
                };
            }
        }
        else if (estimatedRows > 1_000_000)
        {
            // 🚀 大表（100万-500万）：尝试索引快速查询，超时则降级
            var stmt = $@"
-- 尝试利用索引快速查询
SELECT 
    (SELECT TOP 1 CONVERT(NVARCHAR(4000), {QuoteIdentifier(columnName)}) 
     FROM {BuildFullName(schemaName, tableName)} WITH (NOLOCK)
     WHERE {QuoteIdentifier(columnName)} IS NOT NULL 
     ORDER BY {QuoteIdentifier(columnName)} ASC) AS MinValue,
    (SELECT TOP 1 CONVERT(NVARCHAR(4000), {QuoteIdentifier(columnName)}) 
     FROM {BuildFullName(schemaName, tableName)} WITH (NOLOCK)
     WHERE {QuoteIdentifier(columnName)} IS NOT NULL 
     ORDER BY {QuoteIdentifier(columnName)} DESC) AS MaxValue;";

            try
            {
                // 设置 10 秒超时：如果没有索引会很慢
                using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(10));
                
                var bounds = await connection.QuerySingleAsync<MinMaxResult>(new CommandDefinition(
                    commandText: stmt,
                    cancellationToken: cts.Token
                ));

                return new PartitionColumnStatisticsDto
                {
                    MinValue = bounds.MinValue,
                    MaxValue = bounds.MaxValue,
                    TotalRows = estimatedRows,
                    DistinctRows = null
                };
            }
            catch (Exception)
            {
                // 超时或失败：降级到采样模式
                var fallbackStmt = $@"
SELECT 
    MIN(CONVERT(NVARCHAR(4000), {QuoteIdentifier(columnName)})) AS MinValue,
    MAX(CONVERT(NVARCHAR(4000), {QuoteIdentifier(columnName)})) AS MaxValue
FROM (
    SELECT TOP 5000 {QuoteIdentifier(columnName)}
    FROM {BuildFullName(schemaName, tableName)} WITH (NOLOCK)
    WHERE {QuoteIdentifier(columnName)} IS NOT NULL
) AS SampleData;";

                try
                {
                    using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(5));
                    var bounds = await connection.QuerySingleAsync<MinMaxResult>(new CommandDefinition(
                        commandText: fallbackStmt,
                        cancellationToken: cts.Token
                    ));

                    return new PartitionColumnStatisticsDto
                    {
                        MinValue = bounds.MinValue != null ? $"~{bounds.MinValue} (采样)" : "无法计算",
                        MaxValue = bounds.MaxValue != null ? $"~{bounds.MaxValue} (采样)" : "无法计算",
                        TotalRows = estimatedRows,
                        DistinctRows = null
                    };
                }
                catch (Exception)
                {
                    return new PartitionColumnStatisticsDto
                    {
                        MinValue = "查询超时",
                        MaxValue = "查询超时",
                        TotalRows = estimatedRows,
                        DistinctRows = null
                    };
                }
            }
        }
        else
        {
            // ✅ 小表/中表（100万以下）：使用精确统计
            var stmt = $@"
SELECT 
    MIN(CONVERT(NVARCHAR(4000), {QuoteIdentifier(columnName)})) AS MinValue,
    MAX(CONVERT(NVARCHAR(4000), {QuoteIdentifier(columnName)})) AS MaxValue,
    COUNT_BIG(*) AS TotalRows,
    COUNT_BIG(DISTINCT {QuoteIdentifier(columnName)}) AS DistinctRows
FROM {BuildFullName(schemaName, tableName)} WITH (NOLOCK);";

            var stats = await connection.QuerySingleAsync<ColumnStatsRow>(stmt);

            return new PartitionColumnStatisticsDto
            {
                MinValue = stats.MinValue,
                MaxValue = stats.MaxValue,
                TotalRows = stats.TotalRows,
                DistinctRows = stats.DistinctRows
            };
        }
    }

    /// <summary>
    /// 获取服务器上的用户数据库列表。
    /// </summary>
    public async Task<List<TargetDatabaseDto>> GetDatabasesAsync(string connectionString, string? currentDatabaseName = null)
    {
        const string sql = @"
SELECT name AS Name, database_id AS DatabaseId
FROM sys.databases
WHERE database_id > 4 AND state = 0
ORDER BY name;";

        await using var connection = new SqlConnection(connectionString);
        var rows = await connection.QueryAsync<TargetDatabaseDto>(sql);
        return rows.Select(row =>
        {
            row.IsCurrent = !string.IsNullOrWhiteSpace(currentDatabaseName) &&
                            string.Equals(row.Name, currentDatabaseName, StringComparison.OrdinalIgnoreCase);
            return row;
        }).ToList();
    }

    /// <summary>
    /// 获取当前数据库默认数据文件目录。
    /// </summary>
    public async Task<string?> GetDefaultFilePathAsync(string connectionString)
    {
        const string sql = @"
SELECT TOP 1
    LEFT(physical_name, LEN(physical_name) - CHARINDEX('\', REVERSE(physical_name))) AS DirectoryPath
FROM sys.database_files
WHERE type_desc = 'ROWS'
ORDER BY file_id;";

        await using var connection = new SqlConnection(connectionString);
        return await connection.ExecuteScalarAsync<string?>(sql);
    }

    private static string BuildFullName(string schemaName, string tableName)
        => $"{QuoteIdentifier(schemaName)}.{QuoteIdentifier(tableName)}";

    private static string QuoteIdentifier(string value)
        => $"[{value.Replace("]", "]]", StringComparison.Ordinal)}]";

    private static string BuildDisplayType(string dataType, int maxLength, int precision, int scale)
    {
        if (string.Equals(dataType, "decimal", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(dataType, "numeric", StringComparison.OrdinalIgnoreCase))
        {
            return $"{dataType}({precision},{scale})";
        }

        if (string.Equals(dataType, "datetime2", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(dataType, "time", StringComparison.OrdinalIgnoreCase))
        {
            return $"{dataType}({scale})";
        }

        if (maxLength == -1)
        {
            return $"{dataType}(max)";
        }

        if (maxLength <= 0)
        {
            return dataType;
        }

        var normalizedLength = dataType switch
        {
            "nchar" or "nvarchar" => maxLength / 2,
            _ => maxLength
        };

        return $"{dataType}({normalizedLength.ToString(CultureInfo.InvariantCulture)})";
    }
}

internal sealed class PartitionDetailRow
{
    public int PartitionNumber { get; set; }
    public string BoundaryValue { get; set; } = string.Empty;
    public string RangeType { get; set; } = string.Empty;
    public long RowCount { get; set; }
    public decimal TotalSpaceMB { get; set; }
    public string? DataCompression { get; set; }
}

internal sealed class PartitionDetailFallbackRow
{
    public int PartitionNumber { get; set; }
    public string BoundaryValue { get; set; } = string.Empty;
    public string RangeType { get; set; } = string.Empty;
    public long RowCount { get; set; }
    public string DataCompression { get; set; } = "NONE";
}

internal sealed class PartitionFilegroupRow
{
    public int PartitionNumber { get; set; }
    public string FilegroupName { get; set; } = string.Empty;
}

/// <summary>
/// 分区表概览 DTO。
/// </summary>
public class PartitionTableDto
{
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string PartitionFunction { get; set; } = string.Empty;
    public string PartitionScheme { get; set; } = string.Empty;
    public string PartitionColumn { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsRangeRight { get; set; }
    public int TotalPartitions { get; set; }
}

/// <summary>
/// 分区边界详情 DTO。
/// </summary>
public class PartitionDetailDto
{
    public int PartitionNumber { get; set; }
    public string BoundaryValue { get; set; } = string.Empty;
    public string RangeType { get; set; } = string.Empty;
    public string FilegroupName { get; set; } = string.Empty;
    public long RowCount { get; set; }
    public decimal TotalSpaceMB { get; set; }
    public string DataCompression { get; set; } = "NONE";
}

/// <summary>
/// 表列下拉项 DTO。
/// </summary>
public class PartitionTableColumnDto
{
    public string ColumnName { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public int MaxLength { get; set; }
    public int Precision { get; set; }
    public int Scale { get; set; }
    public string DisplayType { get; set; } = string.Empty;
}

/// <summary>
/// 列统计信息 DTO。
/// </summary>
public class PartitionColumnStatisticsDto
{
    public string? MinValue { get; set; }
    public string? MaxValue { get; set; }
    public long? TotalRows { get; set; }
    public long? DistinctRows { get; set; }
}

/// <summary>
/// 目标数据库 DTO。
/// </summary>
public class TargetDatabaseDto
{
    public string Name { get; set; } = string.Empty;
    public int DatabaseId { get; set; }
    public bool IsCurrent { get; set; }
}

/// <summary>
/// 数据库表 DTO。
/// </summary>
public class DatabaseTableDto
{
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
}

internal sealed class ColumnStatsRow
{
    public string? MinValue { get; set; }
    public string? MaxValue { get; set; }
    public long? TotalRows { get; set; }
    public long? DistinctRows { get; set; }
}

internal sealed class MinMaxResult
{
    public string? MinValue { get; set; }
    public string? MaxValue { get; set; }
}
