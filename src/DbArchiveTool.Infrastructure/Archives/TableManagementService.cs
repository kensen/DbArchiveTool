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
            if (column.IsIdentity == 1)
            {
                var seed = column.IdentitySeed ?? 1;
                var increment = column.IdentityIncrement ?? 1;
                columnDef.Append($" IDENTITY({seed},{increment})");
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
