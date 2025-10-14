using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.SqlExecution;
using DbArchiveTool.Infrastructure.Models;

namespace DbArchiveTool.Infrastructure.Partitions;

/// <summary>
/// 通过系统视图读取 SQL Server 分区表的元数据、文件组映射与安全规则。
/// </summary>
internal sealed class SqlServerPartitionMetadataRepository : IPartitionMetadataRepository
{
    private readonly IDbConnectionFactory connectionFactory;

    public SqlServerPartitionMetadataRepository(IDbConnectionFactory connectionFactory)
    {
        this.connectionFactory = connectionFactory;
    }

    /// <inheritdoc />
    public async Task<PartitionConfiguration?> GetConfigurationAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default)
    {
        await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
        const string sql = @"SELECT TOP 1 ps.name AS PartitionSchemeName,
                           pf.name AS PartitionFunctionName,
                           c.name AS ColumnName,
                           t.name AS ColumnType,
                           c.is_nullable AS IsNullable,
                           pf.boundary_value_on_right AS RangeType
                    FROM sys.indexes i
                    INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                    INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                    WHERE i.object_id = OBJECT_ID(@FullName) AND i.index_id IN (0, 1);";

        var result = await connection.QuerySingleOrDefaultAsync(sql, new { FullName = $"[{schemaName}].[{tableName}]" });
        if (result is null)
        {
            return null;
        }

        var valueKind = MapToValueKind((string)result.ColumnType);
        var isNullable = Convert.ToBoolean(result.IsNullable);
        var partitionColumn = new PartitionColumn(result.ColumnName, valueKind, isNullable);
        var filegroupStrategy = PartitionFilegroupStrategy.Default("PRIMARY");
        var isRangeRight = ((int)result.RangeType) == 1;

        var boundaries = await ListBoundariesAsync(dataSourceId, schemaName, tableName, cancellationToken);
        var mappings = await ListFilegroupMappingsAsync(dataSourceId, schemaName, tableName, cancellationToken);
        var safetyRule = await GetSafetyRuleAsync(dataSourceId, schemaName, tableName, cancellationToken);

        return new PartitionConfiguration(
            dataSourceId,
            schemaName,
            tableName,
            result.PartitionFunctionName,
            result.PartitionSchemeName,
            partitionColumn,
            filegroupStrategy,
            isRangeRight,
            null,
            boundaries,
            mappings,
            safetyRule);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PartitionBoundary>> ListBoundariesAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default)
    {
        await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
        const string sql = @"WITH PartitionInfo AS (
                        SELECT
                            p.partition_number,
                            TYPE_NAME(pp.system_type_id) AS ValueType,
                            ROW_NUMBER() OVER (ORDER BY p.partition_number) AS SortKey,
                            prv.value AS BoundaryValue
                        FROM sys.partitions p
                        INNER JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
                        INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                        INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                        INNER JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
                        OUTER APPLY (
                            SELECT prv_inner.value
                            FROM sys.partition_range_values prv_inner
                            WHERE prv_inner.function_id = pf.function_id
                              AND prv_inner.boundary_id = CASE WHEN pf.boundary_value_on_right = 1 THEN p.partition_number - 1 ELSE p.partition_number END
                        ) prv
                        WHERE p.object_id = OBJECT_ID(@FullName) AND i.index_id IN (0, 1)
                    )
                    SELECT SortKey, BoundaryValue, ValueType FROM PartitionInfo ORDER BY SortKey;";

        var rows = await connection.QueryAsync(sql, new { FullName = $"[{schemaName}].[{tableName}]" });
        var boundaries = new List<PartitionBoundary>();
        foreach (var row in rows)
        {
            var key = $"{row.SortKey:D4}";
            var value = CreatePartitionValue(row.BoundaryValue, (string)row.ValueType);
            if (value is null)
            {
                continue;
            }

            boundaries.Add(new PartitionBoundary(key, value));
        }

        return boundaries;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PartitionFilegroupMapping>> ListFilegroupMappingsAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default)
    {
        await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
        const string sql = @"SELECT
                        ROW_NUMBER() OVER (ORDER BY ds2.data_space_id) AS SortKey,
                        ds2.name AS FilegroupName
                    FROM sys.indexes i
                    INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                    INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                    INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id
                    INNER JOIN sys.data_spaces ds2 ON dds.destination_id = ds2.data_space_id
                    WHERE i.object_id = OBJECT_ID(@FullName) AND i.index_id = 1;";

        var rows = await connection.QueryAsync(sql, new { FullName = $"[{schemaName}].[{tableName}]" });
        var mappings = new List<PartitionFilegroupMapping>();
        foreach (var row in rows)
        {
            var boundaryKey = $"{row.SortKey:D4}";
            var filegroupName = (string)row.FilegroupName;
            mappings.Add(PartitionFilegroupMapping.Create(boundaryKey, filegroupName));
        }

        return mappings;
    }

    /// <inheritdoc />
    public async Task<PartitionSafetyRule?> GetSafetyRuleAsync(Guid dataSourceId, string schemaName, string tableName, CancellationToken cancellationToken = default)
    {
        await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
        const string sql = @"SELECT TOP 1 RequiresEmptyPartition, ExecutionWindowHint
                    FROM PartitionSafetyRules
                    WHERE SchemaName = @SchemaName AND TableName = @TableName";

        var rule = await connection.QuerySingleOrDefaultAsync(sql, new { SchemaName = schemaName, TableName = tableName });
        if (rule is null)
        {
            return null;
        }

        var allowedLockModes = new List<string> { "S" };
        return new PartitionSafetyRule(rule.RequiresEmptyPartition, allowedLockModes, rule.ExecutionWindowHint ?? string.Empty);
    }

    /// <inheritdoc />
    public async Task<PartitionSafetySnapshot> GetSafetySnapshotAsync(Guid dataSourceId, string schemaName, string tableName, string boundaryKey, CancellationToken cancellationToken = default)
    {
        await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);
        const string sql = @"SELECT p.partition_number,
                           SUM(a.total_pages) * 8 AS UsedKb,
                           SUM(p.rows) AS RowCount
                    FROM sys.partitions p
                    INNER JOIN sys.allocation_units a ON p.hobt_id = a.container_id
                    WHERE p.object_id = OBJECT_ID(@FullName)
                    GROUP BY p.partition_number;";

        var rows = await connection.QueryAsync(sql, new { FullName = $"[{schemaName}].[{tableName}]" });
        foreach (var row in rows)
        {
            var key = $"{row.partition_number:D4}";
            if (key.Equals(boundaryKey, StringComparison.Ordinal))
            {
                var rowCount = (long)row.RowCount;
                var hasData = rowCount > 0;
                return new PartitionSafetySnapshot(boundaryKey, rowCount, hasData, hasData, hasData ? "分区仍包含数据，建议先使用 SWITCH 导出。" : null);
            }
        }

            return new PartitionSafetySnapshot(boundaryKey, 0, false, false, "未找到分区信息，可能已被合并或表未分区。");
        }

        /// <inheritdoc />
        public async Task<PartitionIndexInspection> GetIndexInspectionAsync(
            Guid dataSourceId,
            string schemaName,
            string tableName,
            string partitionColumn,
            CancellationToken cancellationToken = default)
        {
            await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);

            const string indexSql = @"
SELECT 
    i.index_id AS IndexId,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    i.is_unique_constraint AS IsUniqueConstraint,
    kc.name AS ConstraintName,
    kc.type_desc AS ConstraintType,
    STUFF((
        SELECT ', ' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id 
          AND ic.index_id = i.index_id 
          AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS KeyColumns,
    STUFF((
        SELECT ', ' + QUOTENAME(c.name)
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id 
          AND ic.index_id = i.index_id 
          AND ic.is_included_column = 1
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS IncludedColumns,
    i.filter_definition AS FilterDefinition,
    CASE WHEN EXISTS (
        SELECT 1 FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id 
          AND ic.index_id = i.index_id 
          AND c.name = @PartitionColumn
          AND ic.is_included_column = 0
    ) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS ContainsPartitionColumn
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
LEFT JOIN sys.key_constraints kc ON i.object_id = kc.parent_object_id 
    AND i.index_id = kc.unique_index_id
WHERE SCHEMA_NAME(t.schema_id) = @SchemaName
  AND t.name = @TableName
  AND i.type IN (1, 2)
ORDER BY 
    CASE WHEN i.is_primary_key = 1 THEN 1 
         WHEN i.type_desc = 'CLUSTERED' THEN 2
         WHEN i.is_unique = 1 THEN 3 
         ELSE 4 END,
    i.index_id;";

            var indexes = (await connection.QueryAsync<TableIndexDefinition>(
                indexSql,
                new
                {
                    SchemaName = schemaName,
                    TableName = tableName,
                    PartitionColumn = partitionColumn
                })).ToList();

            var clusteredIndexDefinition = indexes.FirstOrDefault(i => i.IsClustered);

            IndexAlignmentInfo? clusteredIndex = null;
            if (clusteredIndexDefinition is not null)
            {
                clusteredIndex = MapToAlignmentInfo(clusteredIndexDefinition);
            }

            var uniqueIndexes = indexes
                .Where(i => i.IsPrimaryKey || i.IsUniqueConstraint || i.IsUnique)
                .Select(MapToAlignmentInfo)
                .ToList();

            const string foreignKeySql = @"
SELECT fk.name
FROM sys.foreign_keys fk
WHERE fk.referenced_object_id = OBJECT_ID(@FullName)
  AND fk.parent_object_id <> fk.referenced_object_id
  AND fk.is_disabled = 0;";

            var foreignKeys = (await connection.QueryAsync<string>(
                foreignKeySql,
                new { FullName = $"[{schemaName}].[{tableName}]" }))
                .ToList();

            return new PartitionIndexInspection(
                clusteredIndexDefinition is not null,
                clusteredIndex,
                uniqueIndexes,
                foreignKeys);
        }

        private static IndexAlignmentInfo MapToAlignmentInfo(TableIndexDefinition definition)
        {
            var keyColumns = string.IsNullOrWhiteSpace(definition.KeyColumns)
                ? new List<string>()
                : definition.KeyColumns
                    .Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(column => column.Trim())
                    .ToList();

            return new IndexAlignmentInfo(
                definition.IndexName,
                definition.IsClustered,
                definition.IsPrimaryKey,
                definition.IsUniqueConstraint,
                definition.IsUnique,
                definition.ContainsPartitionColumn,
                keyColumns);
        }

        private static PartitionValueKind MapToValueKind(string sqlType)
        {
            return sqlType.ToLowerInvariant() switch
            {
                "int" => PartitionValueKind.Int,
            "bigint" => PartitionValueKind.BigInt,
            "date" => PartitionValueKind.Date,
            "datetime" => PartitionValueKind.DateTime,
            "datetime2" => PartitionValueKind.DateTime2,
            "uniqueidentifier" => PartitionValueKind.Guid,
            _ => PartitionValueKind.String
        };
    }

    private static PartitionValue? CreatePartitionValue(object? boundaryValue, string valueType)
    {
        if (boundaryValue is null || boundaryValue is DBNull)
        {
            return null;
        }

        return valueType.ToLowerInvariant() switch
        {
            "int" => PartitionValue.FromInt(Convert.ToInt32(boundaryValue)),
            "bigint" => PartitionValue.FromBigInt(Convert.ToInt64(boundaryValue)),
            "date" => PartitionValue.FromDate(DateOnly.FromDateTime(Convert.ToDateTime(boundaryValue))),
            "datetime" => PartitionValue.FromDateTime(Convert.ToDateTime(boundaryValue)),
            "datetime2" => PartitionValue.FromDateTime2(Convert.ToDateTime(boundaryValue)),
            "uniqueidentifier" => PartitionValue.FromGuid((Guid)boundaryValue),
            _ => PartitionValue.FromString(boundaryValue.ToString() ?? string.Empty)
        };
    }
}
