using System.Text;
using DbArchiveTool.Application.Archives;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Archives;

/// <summary>
/// 表管理服务实现
/// </summary>
internal sealed class TableManagementService : ITableManagementService
{
    private readonly ISqlExecutor _sqlExecutor;
    private readonly ILogger<TableManagementService> _logger;

    public TableManagementService(
        ISqlExecutor sqlExecutor,
        ILogger<TableManagementService> logger)
    {
        _sqlExecutor = sqlExecutor;
        _logger = logger;
    }

    /// <summary>
    /// 检查目标表是否存在
    /// </summary>
    public async Task<bool> CheckTableExistsAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var sql = @"
                SELECT COUNT(1)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = @SchemaName
                  AND TABLE_NAME = @TableName";

            var count = await _sqlExecutor.QuerySingleAsync<int>(
                connectionString,
                sql,
                new { SchemaName = schemaName, TableName = tableName });

            var exists = count > 0;

            _logger.LogDebug(
                "表存在性检查: {Schema}.{Table} = {Exists}",
                schemaName, tableName, exists);

            return exists;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "检查表是否存在时发生异常: {Schema}.{Table}",
                schemaName, tableName);
            throw;
        }
    }

    /// <summary>
    /// 创建目标表(根据源表结构)
    /// </summary>
    public async Task<TableCreationResult> CreateTargetTableAsync(
        string sourceConnectionString,
        string targetConnectionString,
        string sourceSchemaName,
        string sourceTableName,
        string targetSchemaName,
        string targetTableName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation(
                "开始创建目标表: {TargetSchema}.{TargetTable} (基于 {SourceSchema}.{SourceTable})",
                targetSchemaName, targetTableName, sourceSchemaName, sourceTableName);

            // 1. 检查源表是否存在
            var sourceExists = await CheckTableExistsAsync(
                sourceConnectionString,
                sourceSchemaName,
                sourceTableName,
                cancellationToken);

            if (!sourceExists)
            {
                return TableCreationResult.Failed(
                    $"源表 {sourceSchemaName}.{sourceTableName} 不存在");
            }

            // 2. 获取源表的列定义
            var columns = await GetTableColumnsAsync(
                sourceConnectionString,
                sourceSchemaName,
                sourceTableName,
                cancellationToken);

            if (columns.Count == 0)
            {
                return TableCreationResult.Failed(
                    $"无法获取源表 {sourceSchemaName}.{sourceTableName} 的列定义");
            }

            // 3. 生成 CREATE TABLE 脚本
            var script = GenerateCreateTableScript(
                targetSchemaName,
                targetTableName,
                columns);

            _logger.LogDebug("生成的 CREATE TABLE 脚本:\n{Script}", script);

            // 4. 确保目标架构存在
            await EnsureSchemaExistsAsync(
                targetConnectionString,
                targetSchemaName,
                cancellationToken);

            // 5. 执行创建表脚本
            await _sqlExecutor.ExecuteAsync(
                targetConnectionString,
                script,
                timeoutSeconds: 60);

            _logger.LogInformation(
                "目标表创建成功: {Schema}.{Table}, 列数={ColumnCount}",
                targetSchemaName, targetTableName, columns.Count);

            return TableCreationResult.Succeeded(script, columns.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "创建目标表失败: {TargetSchema}.{TargetTable}",
                targetSchemaName, targetTableName);

            return TableCreationResult.Failed(ex.Message);
        }
    }

    /// <summary>
    /// 获取表的列定义
    /// </summary>
    private async Task<List<ColumnDefinition>> GetTableColumnsAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        var sql = @"
            SELECT 
                c.COLUMN_NAME AS ColumnName,
                c.DATA_TYPE AS DataType,
                c.CHARACTER_MAXIMUM_LENGTH AS MaxLength,
                c.NUMERIC_PRECISION AS NumericPrecision,
                c.NUMERIC_SCALE AS NumericScale,
                c.IS_NULLABLE AS IsNullable,
                c.COLUMN_DEFAULT AS DefaultValue,
                CASE 
                    WHEN pk.COLUMN_NAME IS NOT NULL THEN 1
                    ELSE 0
                END AS IsPrimaryKey,
                COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IsIdentity,
                IDENT_SEED(c.TABLE_SCHEMA + '.' + c.TABLE_NAME) AS IdentitySeed,
                IDENT_INCR(c.TABLE_SCHEMA + '.' + c.TABLE_NAME) AS IdentityIncrement
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                    ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    AND ku.TABLE_SCHEMA = tc.TABLE_SCHEMA
                    AND ku.TABLE_NAME = tc.TABLE_NAME
            ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                 AND c.TABLE_NAME = pk.TABLE_NAME
                 AND c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = @SchemaName
              AND c.TABLE_NAME = @TableName
            ORDER BY c.ORDINAL_POSITION";

        var result = await _sqlExecutor.QueryAsync<ColumnDefinition>(
            connectionString,
            sql,
            new { SchemaName = schemaName, TableName = tableName });

        return result.ToList();
    }

    /// <summary>
    /// 生成 CREATE TABLE 脚本
    /// </summary>
    private string GenerateCreateTableScript(
        string schemaName,
        string tableName,
        List<ColumnDefinition> columns)
    {
        var script = new StringBuilder();
        script.AppendLine($"CREATE TABLE [{schemaName}].[{tableName}] (");

        var columnDefinitions = new List<string>();
        var primaryKeyColumns = new List<string>();

        foreach (var column in columns)
        {
            var columnDef = new StringBuilder();
            columnDef.Append($"    [{column.ColumnName}] ");

            // 数据类型
            columnDef.Append(BuildDataTypeDefinition(column));

            // IDENTITY
            // 说明：定时归档（BulkCopy）场景需要保留源表主键值用于对账/关联。
            // 若目标表主键列保留 IDENTITY，会导致 BulkCopy 写入时由目标表重新生成主键值，造成 ID 变化。
            // 因此：当列同时为主键且为 IDENTITY 时，目标表不复制 IDENTITY 属性。
            if (column.IsIdentity == 1)
            {
                var shouldSkipIdentity = column.IsPrimaryKey == 1;
                if (!shouldSkipIdentity)
                {
                    var seed = column.IdentitySeed ?? 1;
                    var increment = column.IdentityIncrement ?? 1;
                    columnDef.Append($" IDENTITY({seed},{increment})");
                }
            }

            // NULL/NOT NULL
            columnDef.Append(column.IsNullable == "YES" ? " NULL" : " NOT NULL");

            // DEFAULT 约束
            if (!string.IsNullOrWhiteSpace(column.DefaultValue))
            {
                columnDef.Append($" DEFAULT {column.DefaultValue}");
            }

            columnDefinitions.Add(columnDef.ToString());

            // 收集主键列
            if (column.IsPrimaryKey == 1)
            {
                primaryKeyColumns.Add(column.ColumnName);
            }
        }

        script.AppendLine(string.Join(",\n", columnDefinitions));

        // 添加主键约束
        if (primaryKeyColumns.Count > 0)
        {
            var pkColumns = string.Join(", ", primaryKeyColumns.Select(c => $"[{c}]"));
            script.AppendLine($",    CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ({pkColumns})");
        }

        script.AppendLine(");");

        return script.ToString();
    }

    /// <summary>
    /// 构建数据类型定义
    /// </summary>
    private string BuildDataTypeDefinition(ColumnDefinition column)
    {
        var dataType = column.DataType.ToLower();

        return dataType switch
        {
            // 字符串类型
            "char" or "varchar" or "nchar" or "nvarchar" => 
                column.MaxLength.HasValue && column.MaxLength.Value > 0
                    ? $"[{column.DataType}]({column.MaxLength})"
                    : $"[{column.DataType}](MAX)",

            // 精确数值类型
            "decimal" or "numeric" =>
                column.NumericPrecision.HasValue && column.NumericScale.HasValue
                    ? $"[{column.DataType}]({column.NumericPrecision},{column.NumericScale})"
                    : $"[{column.DataType}](18,0)",

            // 浮点数类型
            "float" =>
                column.NumericPrecision.HasValue
                    ? $"[{column.DataType}]({column.NumericPrecision})"
                    : $"[{column.DataType}]",

            // 二进制类型
            "binary" or "varbinary" =>
                column.MaxLength.HasValue && column.MaxLength.Value > 0
                    ? $"[{column.DataType}]({column.MaxLength})"
                    : $"[{column.DataType}](MAX)",

            // 日期时间类型
            "datetime2" or "datetimeoffset" or "time" =>
                column.NumericScale.HasValue
                    ? $"[{column.DataType}]({column.NumericScale})"
                    : $"[{column.DataType}]",

            // 其他类型(不需要长度/精度)
            _ => $"[{column.DataType}]"
        };
    }

    /// <summary>
    /// 确保架构存在
    /// </summary>
    private async Task EnsureSchemaExistsAsync(
        string connectionString,
        string schemaName,
        CancellationToken cancellationToken)
    {
        // 跳过默认架构
        if (schemaName.Equals("dbo", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var checkSql = @"
            SELECT COUNT(1)
            FROM sys.schemas
            WHERE name = @SchemaName";

        var exists = await _sqlExecutor.QuerySingleAsync<int>(
            connectionString,
            checkSql,
            new { SchemaName = schemaName });

        if (exists == 0)
        {
            _logger.LogInformation("创建架构: {Schema}", schemaName);

            var createSql = $"CREATE SCHEMA [{schemaName}]";
            await _sqlExecutor.ExecuteAsync(connectionString, createSql);
        }
    }

    /// <summary>
    /// 对比源表和目标表的结构是否一致(用于归档前的预检查)
    /// </summary>
    public async Task<TableSchemaComparisonResult> CompareTableSchemasAsync(
        string sourceConnectionString,
        string sourceSchemaName,
        string sourceTableName,
        string? targetConnectionString,
        string? targetDatabaseName,
        string targetSchemaName,
        string targetTableName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation(
                "开始对比表结构: 源={SourceSchema}.{SourceTable}, 目标={TargetDatabase}.{TargetSchema}.{TargetTable}",
                sourceSchemaName, sourceTableName, targetDatabaseName ?? "(same)", targetSchemaName, targetTableName);

            const string columnQuery = @"
                SELECT 
                    COLUMN_NAME,
                    ORDINAL_POSITION,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @Schema
                  AND TABLE_NAME = @Table
                ORDER BY ORDINAL_POSITION";

            // 查询源表结构
            var sourceColumns = new List<ColumnInfo>();
            using (var sourceConn = new SqlConnection(sourceConnectionString))
            {
                await sourceConn.OpenAsync(cancellationToken);
                using var cmd = new SqlCommand(columnQuery, sourceConn);
                cmd.Parameters.AddWithValue("@Schema", sourceSchemaName);
                cmd.Parameters.AddWithValue("@Table", sourceTableName);

                using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    sourceColumns.Add(new ColumnInfo
                    {
                        Name = reader.GetString(0),
                        Position = reader.GetInt32(1),
                        DataType = reader.GetString(2),
                        MaxLength = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                        NumericPrecision = reader.IsDBNull(4) ? null : (byte?)reader.GetByte(4),
                        NumericScale = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                        IsNullable = reader.GetString(6) == "YES",
                        DefaultValue = reader.IsDBNull(7) ? null : reader.GetString(7)
                    });
                }
            }

            if (sourceColumns.Count == 0)
            {
                return new TableSchemaComparisonResult
                {
                    TargetTableExists = false,
                    IsCompatible = false,
                    DifferenceDescription = $"源表 [{sourceSchemaName}].[{sourceTableName}] 不存在或没有列",
                    SourceColumnCount = 0
                };
            }

            // 构建目标连接字符串
            var effectiveTargetConnString = targetConnectionString ?? sourceConnectionString;
            var targetConnBuilder = new SqlConnectionStringBuilder(effectiveTargetConnString);

            // 仅在指定了不同的目标数据库时才切换 InitialCatalog
            if (!string.IsNullOrWhiteSpace(targetDatabaseName))
            {
                var sourceDb = new SqlConnectionStringBuilder(sourceConnectionString).InitialCatalog;
                if (!string.Equals(targetDatabaseName, sourceDb, StringComparison.OrdinalIgnoreCase))
                {
                    targetConnBuilder.InitialCatalog = targetDatabaseName;
                }
            }

            // 查询目标表结构
            var targetColumns = new List<ColumnInfo>();
            bool targetTableExists = false;

            using (var targetConn = new SqlConnection(targetConnBuilder.ConnectionString))
            {
                await targetConn.OpenAsync(cancellationToken);

                // 检查目标表是否存在
                const string checkTableSql = @"
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = @Schema
                      AND TABLE_NAME = @Table";

                using (var checkCmd = new SqlCommand(checkTableSql, targetConn))
                {
                    checkCmd.Parameters.AddWithValue("@Schema", targetSchemaName);
                    checkCmd.Parameters.AddWithValue("@Table", targetTableName);

                    targetTableExists = (int)(await checkCmd.ExecuteScalarAsync(cancellationToken))! > 0;
                }

                if (!targetTableExists)
                {
                    _logger.LogWarning(
                        "目标表不存在: {Database}.{Schema}.{Table}",
                        targetConnBuilder.InitialCatalog, targetSchemaName, targetTableName);

                    return new TableSchemaComparisonResult
                    {
                        TargetTableExists = false,
                        IsCompatible = false,
                        DifferenceDescription = $"目标表 [{targetConnBuilder.InitialCatalog}].[{targetSchemaName}].[{targetTableName}] 不存在",
                        SourceColumnCount = sourceColumns.Count,
                        TargetColumnCount = null
                    };
                }

                // 查询目标表列信息
                using var cmd = new SqlCommand(columnQuery, targetConn);
                cmd.Parameters.AddWithValue("@Schema", targetSchemaName);
                cmd.Parameters.AddWithValue("@Table", targetTableName);

                using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    targetColumns.Add(new ColumnInfo
                    {
                        Name = reader.GetString(0),
                        Position = reader.GetInt32(1),
                        DataType = reader.GetString(2),
                        MaxLength = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                        NumericPrecision = reader.IsDBNull(4) ? null : (byte?)reader.GetByte(4),
                        NumericScale = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                        IsNullable = reader.GetString(6) == "YES",
                        DefaultValue = reader.IsDBNull(7) ? null : reader.GetString(7)
                    });
                }
            }

            // 对比列结构
            var missingColumns = new List<string>();
            var typeMismatchColumns = new List<string>();
            var lengthInsufficientColumns = new List<string>();
            var precisionInsufficientColumns = new List<string>();
            var allDifferences = new List<string>();

            var sourceColDict = sourceColumns.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);
            var targetColDict = targetColumns.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

            // 检查源表中有但目标表中没有的列（新增的列）
            foreach (var srcCol in sourceColumns)
            {
                if (!targetColDict.ContainsKey(srcCol.Name))
                {
                    var msg = $"源表新增列: [{srcCol.Name}] {srcCol.DataType}" +
                        (srcCol.MaxLength.HasValue ? $"({srcCol.MaxLength})" : "") +
                        " - 目标表缺少此列";
                    missingColumns.Add(srcCol.Name);
                    allDifferences.Add($"❌ {msg}");
                }
                else
                {
                    var tgtCol = targetColDict[srcCol.Name];

                    // 检查数据类型
                    if (!string.Equals(srcCol.DataType, tgtCol.DataType, StringComparison.OrdinalIgnoreCase))
                    {
                        var msg = $"列 [{srcCol.Name}] 类型不匹配: 源表={srcCol.DataType}, 目标表={tgtCol.DataType}";
                        typeMismatchColumns.Add(srcCol.Name);
                        allDifferences.Add($"❌ {msg}");
                    }
                    // 检查字符类型的长度
                    else if (srcCol.MaxLength.HasValue && tgtCol.MaxLength.HasValue)
                    {
                        // varchar/nvarchar: 源表长度 > 目标表长度 = 可能截断
                        if (srcCol.MaxLength.Value > tgtCol.MaxLength.Value)
                        {
                            var msg = $"列 [{srcCol.Name}] {srcCol.DataType} 长度不足: 源表={srcCol.MaxLength}, 目标表={tgtCol.MaxLength} (可能导致字符串截断)";
                            lengthInsufficientColumns.Add(srcCol.Name);
                            allDifferences.Add($"⚠️ {msg}");
                        }
                    }
                    // 检查数值类型的精度
                    else if (srcCol.NumericPrecision.HasValue && tgtCol.NumericPrecision.HasValue)
                    {
                        if (srcCol.NumericPrecision.Value > tgtCol.NumericPrecision.Value ||
                            srcCol.NumericScale.GetValueOrDefault() > tgtCol.NumericScale.GetValueOrDefault())
                        {
                            var msg = $"列 [{srcCol.Name}] {srcCol.DataType} 精度不足: " +
                                $"源表=({srcCol.NumericPrecision},{srcCol.NumericScale}), " +
                                $"目标表=({tgtCol.NumericPrecision},{tgtCol.NumericScale})";
                            precisionInsufficientColumns.Add(srcCol.Name);
                            allDifferences.Add($"⚠️ {msg}");
                        }
                    }
                }
            }

            // 检查目标表中有但源表中没有的列（已删除的列）
            foreach (var tgtCol in targetColumns)
            {
                if (!sourceColDict.ContainsKey(tgtCol.Name))
                {
                    allDifferences.Add($"ℹ️ 目标表旧列: [{tgtCol.Name}] {tgtCol.DataType}" +
                        (tgtCol.MaxLength.HasValue ? $"({tgtCol.MaxLength})" : "") +
                        " - 源表已删除此列（BCP 导入时会忽略）");
                }
            }

            if (allDifferences.Count > 0)
            {
                var differenceDescription = $"源表和目标表结构不一致，发现 {allDifferences.Count} 处差异:\n\n" +
                    string.Join("\n", allDifferences) +
                    $"\n\n源表: [{sourceSchemaName}].[{sourceTableName}] ({sourceColumns.Count} 列)" +
                    $"\n目标表: [{targetConnBuilder.InitialCatalog}].[{targetSchemaName}].[{targetTableName}] ({targetColumns.Count} 列)" +
                    "\n\n建议:\n" +
                    "1. 使用 SSMS 对比两个表的架构 (右键 → 生成脚本)\n" +
                    "2. 如果源表新增了列，请在目标表中添加对应列\n" +
                    "3. 如果源表字段长度增加，请调整目标表对应列的长度\n" +
                    "4. 确保目标表结构与源表完全一致后重新提交任务";

                _logger.LogWarning(
                    "表结构不一致: 源={SourceSchema}.{SourceTable} ({SourceCols}列), 目标={TargetDb}.{TargetSchema}.{TargetTable} ({TargetCols}列), 差异={DiffCount}",
                    sourceSchemaName, sourceTableName, sourceColumns.Count,
                    targetConnBuilder.InitialCatalog, targetSchemaName, targetTableName, targetColumns.Count,
                    allDifferences.Count);

                return new TableSchemaComparisonResult
                {
                    TargetTableExists = true,
                    IsCompatible = false,
                    DifferenceDescription = differenceDescription,
                    SourceColumnCount = sourceColumns.Count,
                    TargetColumnCount = targetColumns.Count,
                    MissingColumns = missingColumns,
                    TypeMismatchColumns = typeMismatchColumns,
                    LengthInsufficientColumns = lengthInsufficientColumns,
                    PrecisionInsufficientColumns = precisionInsufficientColumns
                };
            }

            _logger.LogInformation(
                "表结构一致: 源={SourceSchema}.{SourceTable}, 目标={TargetDb}.{TargetSchema}.{TargetTable}, 共 {ColCount} 列",
                sourceSchemaName, sourceTableName,
                targetConnBuilder.InitialCatalog, targetSchemaName, targetTableName,
                sourceColumns.Count);

            return new TableSchemaComparisonResult
            {
                TargetTableExists = true,
                IsCompatible = true,
                DifferenceDescription = null,
                SourceColumnCount = sourceColumns.Count,
                TargetColumnCount = targetColumns.Count
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "对比表结构时发生异常");

            return new TableSchemaComparisonResult
            {
                TargetTableExists = false,
                IsCompatible = false,
                DifferenceDescription = $"对比表结构时发生异常: {ex.Message}",
                SourceColumnCount = 0,
                TargetColumnCount = null
            };
        }
    }

    /// <summary>
    /// 列信息（用于表结构对比）
    /// </summary>
    private class ColumnInfo
    {
        public string Name { get; set; } = string.Empty;
        public int Position { get; set; }
        public string DataType { get; set; } = string.Empty;
        public int? MaxLength { get; set; }
        public byte? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public bool IsNullable { get; set; }
        public string? DefaultValue { get; set; }
    }

    /// <summary>
    /// 列定义
    /// </summary>
    private class ColumnDefinition
    {
        public string ColumnName { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public int? MaxLength { get; set; }
        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public string IsNullable { get; set; } = "YES";
        public string? DefaultValue { get; set; }
        public int IsPrimaryKey { get; set; }
        public int? IsIdentity { get; set; }
        public decimal? IdentitySeed { get; set; }
        public decimal? IdentityIncrement { get; set; }
    }
}
