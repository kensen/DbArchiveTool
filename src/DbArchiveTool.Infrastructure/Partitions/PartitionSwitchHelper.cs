using System.Text;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Partitions;

/// <summary>
/// 分区切换辅助类
/// 用于 BCP/BulkCopy 归档时的分区优化方案：先 SWITCH 到临时表，再归档临时表
/// </summary>
public class PartitionSwitchHelper
{
    private readonly ISqlExecutor _sqlExecutor;
    private readonly ILogger<PartitionSwitchHelper> _logger;

    public PartitionSwitchHelper(
        ISqlExecutor sqlExecutor,
        ILogger<PartitionSwitchHelper> logger)
    {
        _sqlExecutor = sqlExecutor;
        _logger = logger;
    }

    /// <summary>
    /// 检查表是否为分区表
    /// </summary>
    public async Task<bool> IsPartitionedTableAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        const string sql = @"
            SELECT CAST(CASE WHEN EXISTS (
                SELECT 1
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                WHERE t.schema_id = SCHEMA_ID(@SchemaName)
                  AND t.name = @TableName
                  AND i.index_id IN (0, 1)  -- 堆或聚集索引
            ) THEN 1 ELSE 0 END AS BIT)";

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@SchemaName", schemaName);
        cmd.Parameters.AddWithValue("@TableName", tableName);

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is bool b && b;
    }

    /// <summary>
    /// 获取分区信息（分区号、边界值、行数）
    /// </summary>
    public async Task<PartitionInfo?> GetPartitionInfoAsync(
        string connectionString,
        string schemaName,
        string tableName,
        string partitionKey,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("【分区查询】开始查询分区信息: PartitionKey='{PartitionKey}' (类型:{Type})", 
            partitionKey, partitionKey?.GetType().Name ?? "null");

        // 尝试将 partitionKey 解析为整数（分区号）
        var isPartitionNumber = int.TryParse(partitionKey, out var partitionNumber);
        
        _logger.LogInformation("【分区查询】解析结果: IsPartitionNumber={IsNumber}, ParsedValue={Value}", 
            isPartitionNumber, isPartitionNumber ? partitionNumber : "N/A");

        string sql;
        if (isPartitionNumber)
        {
            // 按分区号精确匹配
            // 关键修复: 使用子查询获取唯一的边界值,避免 LEFT JOIN 产生多行
            sql = @"
                SELECT 
                    p.partition_number AS PartitionNumber,
                    fg.name AS FileGroupName,
                    (SELECT TOP 1 prv.value 
                     FROM sys.partition_range_values prv 
                     WHERE prv.function_id = pf.function_id 
                       AND p.partition_number = prv.boundary_id + CASE WHEN pf.boundary_value_on_right = 1 THEN 1 ELSE 0 END
                    ) AS BoundaryValue,
                    p.rows AS PartitionRows
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units au ON p.partition_id = au.container_id
                INNER JOIN sys.filegroups fg ON au.data_space_id = fg.data_space_id
                INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                WHERE t.schema_id = SCHEMA_ID(@SchemaName)
                  AND t.name = @TableName
                  AND p.partition_number = @PartitionNumber";
            
            _logger.LogInformation("【分区查询】使用分区号查询: PartitionNumber={Number}", partitionNumber);
        }
        else
        {
            // 按边界值匹配
            sql = @"
                SELECT 
                    p.partition_number AS PartitionNumber,
                    fg.name AS FileGroupName,
                    prv.value AS BoundaryValue,
                    p.rows AS PartitionRows
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units au ON p.partition_id = au.container_id
                INNER JOIN sys.filegroups fg ON au.data_space_id = fg.data_space_id
                INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                INNER JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id 
                    AND p.partition_number = prv.boundary_id + CASE WHEN pf.boundary_value_on_right = 1 THEN 1 ELSE 0 END
                WHERE t.schema_id = SCHEMA_ID(@SchemaName)
                  AND t.name = @TableName
                  AND CAST(prv.value AS NVARCHAR(100)) = @PartitionKey";
            
            _logger.LogInformation("【分区查询】使用边界值查询: PartitionKey={Key}", partitionKey);
        }

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@SchemaName", schemaName);
        cmd.Parameters.AddWithValue("@TableName", tableName);
        
        if (isPartitionNumber)
        {
            cmd.Parameters.AddWithValue("@PartitionNumber", partitionNumber);
        }
        else
        {
            cmd.Parameters.AddWithValue("@PartitionKey", partitionKey);
        }

        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            // 安全地读取 RowCount，处理可能的类型不匹配
            long rowCount = 0;
            if (!reader.IsDBNull(3))
            {
                var rowCountValue = reader.GetValue(3);
                rowCount = Convert.ToInt64(rowCountValue);
            }

            var result = new PartitionInfo
            {
                PartitionNumber = reader.GetInt32(0),
                FileGroupName = reader.GetString(1),
                BoundaryValue = reader.IsDBNull(2) ? null : reader.GetValue(2),
                RowCount = rowCount
            };

            _logger.LogInformation(
                "【分区查询】✓ 匹配成功: 分区号={PartitionNumber}, 边界值={BoundaryValue}, 行数={RowCount}, 文件组={FileGroup}",
                result.PartitionNumber, result.BoundaryValue, result.RowCount, result.FileGroupName);

            return result;
        }

        _logger.LogWarning(
            "【分区查询】✗ 未找到匹配的分区: 表={Schema}.{Table}, 查询条件={PartitionKey}",
            schemaName, tableName, partitionKey);

        return null;
    }

    /// <summary>
    /// 创建临时表（用于 SWITCH 分区）
    /// </summary>
    public async Task<string> CreateTempTableForSwitchAsync(
        string connectionString,
        string schemaName,
        string tableName,
        PartitionInfo partitionInfo,
        CancellationToken cancellationToken = default)
    {
        var tempTableName = $"{tableName}_Temp_{DateTime.Now:yyyyMMddHHmmss}";
        
        // 1. 获取表结构并在指定文件组上创建临时表
        // 注意：不能使用 SELECT INTO ... ON 语法（SQL Server 不支持）
        // 必须先获取表结构，然后使用 CREATE TABLE ... ON 语法
        
        // 1.1 获取列定义
        var getColumnsSql = $@"
            SELECT 
                c.name AS ColumnName,
                t.name AS DataType,
                c.max_length AS MaxLength,
                c.precision AS Precision,
                c.scale AS Scale,
                c.is_nullable AS IsNullable,
                c.is_identity AS IsIdentity
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            INNER JOIN sys.tables tbl ON c.object_id = tbl.object_id
            WHERE tbl.schema_id = SCHEMA_ID(@SchemaName)
              AND tbl.name = @TableName
            ORDER BY c.column_id";

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        using var cmd = new SqlCommand(getColumnsSql, conn);
        cmd.Parameters.AddWithValue("@SchemaName", schemaName);
        cmd.Parameters.AddWithValue("@TableName", tableName);

        var columns = new List<string>();
        using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var columnName = reader.GetString(0);
                var dataType = reader.GetString(1);
                var maxLength = reader.GetInt16(2);
                var precision = reader.GetByte(3);
                var scale = reader.GetByte(4);
                var isNullable = reader.GetBoolean(5);
                var isIdentity = reader.GetBoolean(6);

                // 构建列定义
                var columnDef = $"[{columnName}] {FormatDataType(dataType, maxLength, precision, scale)}";
                
                // 临时表不需要 IDENTITY 属性，因为会通过 SWITCH 填充数据
                // if (isIdentity) columnDef += " IDENTITY(1,1)";
                
                if (!isNullable) columnDef += " NOT NULL";
                
                columns.Add(columnDef);
            }
        }

        // 1.2 在指定文件组上创建临时表
        var createTableSql = $@"
            CREATE TABLE [{schemaName}].[{tempTableName}] (
                {string.Join(",\n                ", columns)}
            ) ON [{partitionInfo.FileGroupName}]";

        using var createCmd = new SqlCommand(createTableSql, conn);
        createCmd.CommandTimeout = 300;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation(
            "创建临时表成功: [{Schema}].[{TempTable}] on {FileGroup}",
            schemaName, tempTableName, partitionInfo.FileGroupName);

        // 2. 读取源表的所有索引定义
        var indexes = await GetTableIndexesAsync(
            connectionString,
            schemaName,
            tableName,
            cancellationToken);

        _logger.LogInformation(
            "源表 [{Schema}].[{Table}] 共有 {Count} 个索引",
            schemaName, tableName, indexes.Count);

        // 注意: SWITCH 到普通表(非分区表)时,只需要聚集索引匹配即可
        // 过滤出聚集索引 (主键或 IndexId = 1)
        var clusteredIndexes = indexes
            .Where(i => i.IsPrimaryKey || i.IndexId == 1 || i.IndexType == "CLUSTERED")
            .ToList();

        _logger.LogInformation(
            "  其中聚集索引 {Count} 个将复制到临时表 (SWITCH 只需要聚集索引匹配)",
            clusteredIndexes.Count);

        if (clusteredIndexes.Count == 0)
        {
            _logger.LogWarning(
                "警告：源表 [{Schema}].[{Table}] 没有找到聚集索引！这将导致 SWITCH PARTITION 失败。",
                schemaName, tableName);
        }

        // 3. 在临时表上重建聚集索引（使用相同文件组）
        foreach (var index in clusteredIndexes.OrderBy(i => i.IndexId)) // 按 IndexId 排序
        {
            _logger.LogInformation(
                "  准备创建索引: {IndexName} (IndexId={IndexId}, Type={Type}, IsPrimaryKey={IsPK}, KeyColumns={Keys})",
                index.IndexName, index.IndexId, index.IndexType, index.IsPrimaryKey, index.KeyColumns);

            var createIndexSql = GenerateCreateIndexSqlForTempTable(
                schemaName,
                tempTableName,
                index,
                partitionInfo.FileGroupName);

            _logger.LogInformation("  执行 SQL: {Sql}", createIndexSql);

            try
            {
                await _sqlExecutor.ExecuteAsync(
                    connectionString,
                    createIndexSql,
                    timeoutSeconds: 300);

                _logger.LogInformation(
                    "  ✓ 已在临时表上创建索引: {IndexName} ({Type})",
                    index.IndexName, index.IndexType);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "  ✗ 创建索引 {IndexName} 失败: {Message}\n  SQL: {Sql}",
                    index.IndexName, ex.Message, createIndexSql);

                // 对于聚集索引或主键，必须抛出异常，因为没有它们 SWITCH 会失败
                if (index.IsClustered || index.IsPrimaryKey)
                {
                    throw new InvalidOperationException(
                        $"创建关键索引 {index.IndexName} 失败，无法继续。SQL: {createIndexSql}", ex);
                }

                // 其他唯一索引失败记录警告但继续
                if (index.IsUnique)
                {
                    _logger.LogWarning(
                        "  ⚠ 唯一索引 {IndexName} 创建失败，将跳过",
                        index.IndexName);
                }
                else
                {
                    throw new InvalidOperationException(
                        $"创建临时表索引 {index.IndexName} 失败: {ex.Message}", ex);
                }
            }
        }

        // 4. 添加 CHECK 约束（必需，以确保 SWITCH 成功）
        if (partitionInfo.BoundaryValue != null)
        {
            // TODO: 需要知道分区列名才能创建约束
            // 这里需要从 sys.partition_scheme_parameters 查询分区列
            _logger.LogWarning(
                "临时表创建完成，但未添加 CHECK 约束。分区列检测待实现。");
        }

        return tempTableName;
    }

    /// <summary>
    /// 执行分区 SWITCH
    /// </summary>
    public async Task SwitchPartitionAsync(
        string connectionString,
        string sourceSchema,
        string sourceTable,
        int partitionNumber,
        string targetSchema,
        string targetTable,
        CancellationToken cancellationToken = default)
    {
        var switchSql = $@"
            ALTER TABLE [{sourceSchema}].[{sourceTable}]
            SWITCH PARTITION {partitionNumber}
            TO [{targetSchema}].[{targetTable}]";

        await _sqlExecutor.ExecuteAsync(
            connectionString,
            switchSql);

        _logger.LogInformation(
            "分区 SWITCH 成功: [{SourceSchema}].[{SourceTable}] Partition {PartitionNum} -> [{TargetSchema}].[{TargetTable}]",
            sourceSchema, sourceTable, partitionNumber, targetSchema, targetTable);
    }

    /// <summary>
    /// 获取分区函数和分区列信息（用于 $PARTITION 函数）
    /// </summary>
    public async Task<PartitionFunctionInfo?> GetPartitionFunctionInfoAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        const string sql = @"
            SELECT 
                pf.name AS PartitionFunctionName,
                c.name AS PartitionColumnName
            FROM sys.tables t
            INNER JOIN sys.indexes i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
            INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE t.schema_id = SCHEMA_ID(@SchemaName)
              AND t.name = @TableName
              AND ic.partition_ordinal = 1";

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@SchemaName", schemaName);
        cmd.Parameters.AddWithValue("@TableName", tableName);

        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            var result = new PartitionFunctionInfo
            {
                PartitionFunctionName = reader.GetString(0),
                PartitionColumnName = reader.GetString(1)
            };

            _logger.LogInformation(
                "获取分区函数信息成功: 函数={FunctionName}, 列={ColumnName}",
                result.PartitionFunctionName, result.PartitionColumnName);

            return result;
        }

        _logger.LogWarning("表 {Schema}.{Table} 不是分区表或未找到分区函数信息", schemaName, tableName);
        return null;
    }

    /// <summary>
    /// 删除临时表
    /// </summary>
    public async Task DropTempTableAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        // 兼容 SQL Server 2014 及更早版本，先检查表是否存在
        var dropSql = $@"
            IF OBJECT_ID('[{schemaName}].[{tableName}]', 'U') IS NOT NULL
            BEGIN
                DROP TABLE [{schemaName}].[{tableName}]
            END";

        await _sqlExecutor.ExecuteAsync(
            connectionString,
            dropSql);

        _logger.LogInformation(
            "临时表已删除: [{Schema}].[{Table}]",
            schemaName, tableName);
    }

    /// <summary>
    /// 获取表的行数统计
    /// </summary>
    public async Task<long> GetTableRowCountAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        var sql = $@"
            SELECT SUM(p.rows)
            FROM sys.tables t
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            WHERE t.schema_id = SCHEMA_ID(@SchemaName)
              AND t.name = @TableName
              AND p.index_id IN (0, 1)";

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@SchemaName", schemaName);
        cmd.Parameters.AddWithValue("@TableName", tableName);

        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is long count ? count : 0;
    }

    /// <summary>
    /// 获取表的所有索引定义
    /// </summary>
    private async Task<List<TempTableIndexDefinition>> GetTableIndexesAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        const string sql = @"
            SELECT 
                i.index_id AS IndexId,
                i.name AS IndexName,
                i.type_desc AS IndexType,
                i.is_unique AS IsUnique,
                i.is_primary_key AS IsPrimaryKey,
                i.is_unique_constraint AS IsUniqueConstraint,
                kc.name AS ConstraintName,
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
                i.filter_definition AS FilterDefinition
            FROM sys.indexes i
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            LEFT JOIN sys.key_constraints kc ON i.object_id = kc.parent_object_id 
                AND i.index_id = kc.unique_index_id
            WHERE SCHEMA_NAME(t.schema_id) = @SchemaName
              AND t.name = @TableName
              AND i.type IN (1, 2)  -- 聚集索引和非聚集索引
            ORDER BY i.index_id";

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@SchemaName", schemaName);
        cmd.Parameters.AddWithValue("@TableName", tableName);

        var indexes = new List<TempTableIndexDefinition>();
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            indexes.Add(new TempTableIndexDefinition
            {
                IndexId = reader.GetInt32(0),
                IndexName = reader.GetString(1),
                IndexType = reader.GetString(2),
                IsUnique = reader.GetBoolean(3),
                IsPrimaryKey = reader.GetBoolean(4),
                IsUniqueConstraint = reader.GetBoolean(5),
                ConstraintName = reader.IsDBNull(6) ? null : reader.GetString(6),
                KeyColumns = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                IncludedColumns = reader.IsDBNull(8) ? null : reader.GetString(8),
                FilterDefinition = reader.IsDBNull(9) ? null : reader.GetString(9)
            });
        }

        return indexes;
    }

    /// <summary>
    /// 生成为临时表创建索引的 SQL
    /// </summary>
    /// <remarks>
    /// 临时表不是分区表，但为了执行 ALTER TABLE SWITCH，必须将索引创建在与源分区相同的文件组上。
    /// 约束名称需要添加临时表后缀以避免与源表的约束名称冲突。
    /// </remarks>
    private string GenerateCreateIndexSqlForTempTable(
        string schemaName,
        string tableName,
        TempTableIndexDefinition index,
        string fileGroupName)
    {
        var sb = new StringBuilder();

        // 主键约束
        // 注意：约束名称需要加上临时表后缀，避免与源表约束名称冲突
        // 注意：必须指定文件组为源分区所在的文件组，否则 SWITCH 会失败
        if (index.IsPrimaryKey && !string.IsNullOrWhiteSpace(index.ConstraintName))
        {
            var tempConstraintName = $"{index.ConstraintName}_Temp";
            sb.Append($"ALTER TABLE [{schemaName}].[{tableName}] ");
            sb.Append($"ADD CONSTRAINT [{tempConstraintName}] PRIMARY KEY ");
            sb.Append(index.IsClustered ? "CLUSTERED " : "NONCLUSTERED ");
            sb.Append($"({index.KeyColumns}) ");
            sb.Append($"ON [{fileGroupName}]");  // 必须指定文件组
        }
        // 唯一约束
        // 注意：约束名称需要加上临时表后缀，避免与源表约束名称冲突
        // 注意：必须指定文件组为源分区所在的文件组，否则 SWITCH 会失败
        else if (index.IsUniqueConstraint && !string.IsNullOrWhiteSpace(index.ConstraintName))
        {
            var tempConstraintName = $"{index.ConstraintName}_Temp";
            sb.Append($"ALTER TABLE [{schemaName}].[{tableName}] ");
            sb.Append($"ADD CONSTRAINT [{tempConstraintName}] UNIQUE ");
            sb.Append(index.IsClustered ? "CLUSTERED " : "NONCLUSTERED ");
            sb.Append($"({index.KeyColumns}) ");
            sb.Append($"ON [{fileGroupName}]");  // 必须指定文件组
        }
        // 普通索引
        // 注意：必须指定文件组为源分区所在的文件组，否则 SWITCH 会失败
        else
        {
            sb.Append("CREATE ");
            if (index.IsUnique) sb.Append("UNIQUE ");
            sb.Append(index.IsClustered ? "CLUSTERED " : "NONCLUSTERED ");
            sb.Append($"INDEX [{index.IndexName}] ");
            sb.Append($"ON [{schemaName}].[{tableName}] ({index.KeyColumns})");

            // INCLUDE 列
            if (!string.IsNullOrWhiteSpace(index.IncludedColumns))
            {
                sb.Append($" INCLUDE ({index.IncludedColumns})");
            }

            // WHERE 筛选条件
            if (!string.IsNullOrWhiteSpace(index.FilterDefinition))
            {
                sb.Append($" WHERE {index.FilterDefinition}");
            }

            sb.Append($" ON [{fileGroupName}]");  // 必须指定文件组
        }

        sb.Append(';');
        return sb.ToString();
    }

    /// <summary>
    /// 格式化数据类型字符串（用于 CREATE TABLE）
    /// </summary>
    private string FormatDataType(string dataType, short maxLength, byte precision, byte scale)
    {
        // 处理特殊类型
        switch (dataType.ToLowerInvariant())
        {
            case "nvarchar":
            case "nchar":
                if (maxLength == -1)
                    return $"{dataType}(max)";
                return $"{dataType}({maxLength / 2})"; // Unicode 字符占 2 字节
            
            case "varchar":
            case "char":
            case "binary":
            case "varbinary":
                if (maxLength == -1)
                    return $"{dataType}(max)";
                return $"{dataType}({maxLength})";
            
            case "decimal":
            case "numeric":
                return $"{dataType}({precision},{scale})";
            
            case "datetime2":
            case "time":
            case "datetimeoffset":
                if (scale > 0)
                    return $"{dataType}({scale})";
                return dataType;
            
            // 固定长度类型，不需要参数
            case "int":
            case "bigint":
            case "smallint":
            case "tinyint":
            case "bit":
            case "float":
            case "real":
            case "datetime":
            case "smalldatetime":
            case "date":
            case "money":
            case "smallmoney":
            case "uniqueidentifier":
            case "xml":
            case "text":
            case "ntext":
            case "image":
                return dataType;
            
            default:
                _logger.LogWarning("未识别的数据类型: {DataType}, 使用默认格式", dataType);
                return dataType;
        }
    }
}

/// <summary>
/// 分区信息
/// </summary>
public class PartitionInfo
{
    /// <summary>分区号</summary>
    public int PartitionNumber { get; set; }

    /// <summary>文件组名</summary>
    public string FileGroupName { get; set; } = string.Empty;

    /// <summary>边界值</summary>
    public object? BoundaryValue { get; set; }

    /// <summary>行数</summary>
    public long RowCount { get; set; }
}

/// <summary>
/// 分区函数信息（用于 $PARTITION 函数查询）
/// </summary>
public class PartitionFunctionInfo
{
    /// <summary>分区函数名</summary>
    public string PartitionFunctionName { get; set; } = string.Empty;

    /// <summary>分区列名</summary>
    public string PartitionColumnName { get; set; } = string.Empty;
}

/// <summary>
/// 临时表索引定义（用于复制索引到临时表）
/// </summary>
internal class TempTableIndexDefinition
{
    /// <summary>索引 ID</summary>
    public int IndexId { get; set; }

    /// <summary>索引名称</summary>
    public string IndexName { get; set; } = string.Empty;

    /// <summary>索引类型（CLUSTERED 或 NONCLUSTERED）</summary>
    public string IndexType { get; set; } = string.Empty;

    /// <summary>是否唯一索引</summary>
    public bool IsUnique { get; set; }

    /// <summary>是否主键</summary>
    public bool IsPrimaryKey { get; set; }

    /// <summary>是否唯一约束</summary>
    public bool IsUniqueConstraint { get; set; }

    /// <summary>约束名称</summary>
    public string? ConstraintName { get; set; }

    /// <summary>键列（包含排序方向）</summary>
    public string KeyColumns { get; set; } = string.Empty;

    /// <summary>INCLUDE 列</summary>
    public string? IncludedColumns { get; set; }

    /// <summary>筛选条件</summary>
    public string? FilterDefinition { get; set; }

    /// <summary>是否为聚集索引</summary>
    public bool IsClustered => string.Equals(IndexType, "CLUSTERED", StringComparison.OrdinalIgnoreCase);
}
