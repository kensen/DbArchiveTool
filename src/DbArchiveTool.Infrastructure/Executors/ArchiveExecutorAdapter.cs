using DbArchiveTool.Application.Archives;
using DbArchiveTool.Domain.ArchiveConfigurations;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Shared.Archive;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 归档执行器适配器
/// 将 OptimizedPartitionArchiveExecutor 适配为 IArchiveExecutor 接口
/// </summary>
public sealed class ArchiveExecutorAdapter : IArchiveExecutor
{
    private readonly OptimizedPartitionArchiveExecutor _partitionExecutor;
    private readonly IDbConnectionFactory _connectionFactory;
    private readonly ILogger<ArchiveExecutorAdapter> _logger;

    public ArchiveExecutorAdapter(
        OptimizedPartitionArchiveExecutor partitionExecutor,
        IDbConnectionFactory connectionFactory,
        ILogger<ArchiveExecutorAdapter> logger)
    {
        _partitionExecutor = partitionExecutor;
        _connectionFactory = connectionFactory;
        _logger = logger;
    }

    /// <summary>
    /// 执行归档
    /// </summary>
    public async Task<ArchiveExecutionResult> ExecuteAsync(
        ArchiveConfiguration config,
        ArchiveDataSource dataSource,
        string targetConnectionString,
        int? partitionNumber = null,
        Action<ArchiveProgressInfo>? progressCallback = null,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;

        try
        {
            // 验证分区表归档必须指定分区号
            if (config.IsPartitionedTable && !partitionNumber.HasValue)
            {
                return new ArchiveExecutionResult
                {
                    Success = false,
                    ConfigurationId = config.Id,
                    ConfigurationName = config.Name,
                    SourceSchemaName = config.SourceSchemaName,
                    SourceTableName = config.SourceTableName,
                    ArchiveMethod = config.ArchiveMethod,
                    Message = "分区表归档必须指定分区号",
                    StartTimeUtc = startTime,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            // 普通表归档：用于“定时归档任务”的小批量持续归档
            if (!config.IsPartitionedTable)
            {
                return await ExecuteNormalTableArchiveAsync(
                    config,
                    dataSource,
                    targetConnectionString,
                    progressCallback,
                    startTime,
                    cancellationToken);
            }

            // 执行优化的分区归档
            var result = await _partitionExecutor.ExecuteAsync(
                config,
                dataSource,
                targetConnectionString,
                partitionNumber!.Value,
                progress =>
                {
                    progressCallback?.Invoke(new ArchiveProgressInfo
                    {
                        Message = progress.Message,
                        ProgressPercentage = progress.ProgressPercentage,
                        RowsProcessed = progress.RowsProcessed
                    });
                },
                cancellationToken);

            // 转换结果
            return new ArchiveExecutionResult
            {
                Success = result.Success,
                ConfigurationId = config.Id,
                ConfigurationName = config.Name,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                PartitionNumber = partitionNumber,
                ArchiveMethod = config.ArchiveMethod,
                RowsArchived = result.RowsArchived,
                StartTimeUtc = result.StartTime,
                EndTimeUtc = result.EndTime,
                Message = result.Message,
                ErrorDetails = result.ErrorDetails
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "归档执行失败: ConfigId={ConfigId}", config.Id);

            return new ArchiveExecutionResult
            {
                Success = false,
                ConfigurationId = config.Id,
                ConfigurationName = config.Name,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                PartitionNumber = partitionNumber,
                ArchiveMethod = config.ArchiveMethod,
                Message = $"归档执行失败: {ex.Message}",
                ErrorDetails = ex.ToString(),
                StartTimeUtc = startTime,
                EndTimeUtc = DateTime.UtcNow
            };
        }
    }

    /// <summary>
    /// 执行普通表归档（小批量持续归档）
    /// </summary>
    private async Task<ArchiveExecutionResult> ExecuteNormalTableArchiveAsync(
        ArchiveConfiguration config,
        ArchiveDataSource dataSource,
        string targetConnectionString,
        Action<ArchiveProgressInfo>? progressCallback,
        DateTime startTimeUtc,
        CancellationToken cancellationToken)
    {
        if (config.ArchiveMethod == ArchiveMethod.PartitionSwitch)
        {
            return new ArchiveExecutionResult
            {
                Success = false,
                ConfigurationId = config.Id,
                ConfigurationName = config.Name,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                ArchiveMethod = config.ArchiveMethod,
                Message = "普通表归档不支持 PartitionSwitch 方法，请改用 BulkCopy 或 Bcp",
                StartTimeUtc = startTimeUtc,
                EndTimeUtc = DateTime.UtcNow
            };
        }

        if (string.IsNullOrWhiteSpace(config.ArchiveFilterColumn) || string.IsNullOrWhiteSpace(config.ArchiveFilterCondition))
        {
            return new ArchiveExecutionResult
            {
                Success = false,
                ConfigurationId = config.Id,
                ConfigurationName = config.Name,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                ArchiveMethod = config.ArchiveMethod,
                Message = "普通表归档必须配置过滤列与过滤条件",
                StartTimeUtc = startTimeUtc,
                EndTimeUtc = DateTime.UtcNow
            };
        }

        // 简单防注入：过滤条件不允许包含明显的多语句/注释分隔符
        var filterCondition = config.ArchiveFilterCondition.Trim();
        if (filterCondition.Contains(';') || filterCondition.Contains("--") || filterCondition.Contains("/*") || filterCondition.Contains("*/"))
        {
            return new ArchiveExecutionResult
            {
                Success = false,
                ConfigurationId = config.Id,
                ConfigurationName = config.Name,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                ArchiveMethod = config.ArchiveMethod,
                Message = "过滤条件包含不允许的字符（; 或注释符），请仅填写表达式片段，例如：< DATEADD(day, -7, GETDATE())",
                StartTimeUtc = startTimeUtc,
                EndTimeUtc = DateTime.UtcNow
            };
        }

        ValidateSqlIdentifier(config.SourceSchemaName, "源表架构名");
        ValidateSqlIdentifier(config.SourceTableName, "源表名称");
        ValidateSqlIdentifier(string.IsNullOrWhiteSpace(config.TargetSchemaName) ? config.SourceSchemaName : config.TargetSchemaName, "目标表架构名");
        ValidateSqlIdentifier(string.IsNullOrWhiteSpace(config.TargetTableName) ? config.SourceTableName : config.TargetTableName, "目标表名称");
        ValidateSqlIdentifier(config.ArchiveFilterColumn!, "归档过滤列名");

        // 通过连接工厂构建源库连接字符串（自动处理密码解密），避免密文写入 SqlConnectionStringBuilder 导致长度超限
        var sourceConnectionString = _connectionFactory.BuildConnectionString(dataSource);
        var sourceSchema = config.SourceSchemaName;
        var sourceTable = config.SourceTableName;
        var targetSchema = string.IsNullOrWhiteSpace(config.TargetSchemaName) ? sourceSchema : config.TargetSchemaName;
        var targetTable = string.IsNullOrWhiteSpace(config.TargetTableName) ? sourceTable : config.TargetTableName;
        var filterColumn = config.ArchiveFilterColumn!;
        var batchSize = Math.Max(1, config.BatchSize);
        var wherePredicate = BuildWherePredicate(filterColumn, filterCondition);

        string? selectSqlForLog = null;
        string? deleteSqlForLog = null;
        string? createTableSqlForLog = null;
        string? tempKeySqlForLog = null;

        try
        {
            progressCallback?.Invoke(new ArchiveProgressInfo { Message = "准备普通表归档", ProgressPercentage = 0, RowsProcessed = 0 });

            using var sourceConnection = new SqlConnection(sourceConnectionString);
            using var targetConnection = new SqlConnection(targetConnectionString);
            await sourceConnection.OpenAsync(cancellationToken);
            await targetConnection.OpenAsync(cancellationToken);

            var columns = await GetUserInsertableColumnsAsync(sourceConnection, sourceSchema, sourceTable, cancellationToken);
            if (columns.Count == 0)
            {
                return new ArchiveExecutionResult
                {
                    Success = false,
                    ConfigurationId = config.Id,
                    ConfigurationName = config.Name,
                    SourceSchemaName = sourceSchema,
                    SourceTableName = sourceTable,
                    ArchiveMethod = config.ArchiveMethod,
                    Message = "未能获取可归档的列信息（可能是表不存在或所有列均不可插入）",
                    StartTimeUtc = startTimeUtc,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            progressCallback?.Invoke(new ArchiveProgressInfo { Message = "检查并创建目标表结构", ProgressPercentage = 5, RowsProcessed = 0 });
            createTableSqlForLog = BuildCreateTableSql(targetSchema, targetTable, columns);
            await EnsureSchemaAndTableExistsAsync(
                targetConnection,
                targetSchema,
                targetTable,
                columns,
                _logger,
                cancellationToken);

            // READPAST 锁提示在 READ COMMITTED / REPEATABLE READ 隔离级别下才允许。
            // 这里显式使用 RepeatableRead，以保留“跳过锁行”的并发语义并避免 SQL Server 报错。
            using var tx = sourceConnection.BeginTransaction(IsolationLevel.RepeatableRead);

            var columnList = string.Join(", ", columns.Select(c => $"[{c.ColumnName}]"));
            var sourceTableFull = $"[{sourceSchema}].[{sourceTable}]";
            var targetTableFull = $"[{targetSchema}].[{targetTable}]";

            // 读取与删除的一致性：
            // - 当开启“归档后删除源数据”时，必须确保删除的行与本次归档写入的行完全一致。
            // - 去掉 ORDER BY 后，不能再用“再次 TOP(N) 查询”来删除，否则可能删到不同的行。
            // - 这里采用源库临时表保存本批次主键 -> 依据临时表读取 -> 依据临时表删除。

            long rowsCopied;

            if (!config.DeleteSourceDataAfterArchive)
            {
                var lockHint = "WITH (READPAST, ROWLOCK)";
                var selectSql = $@"
SELECT TOP (@BatchSize)
    {columnList}
FROM {sourceTableFull} {lockHint}
WHERE ({wherePredicate})";

                selectSqlForLog = selectSql;
                _logger.LogInformation(
                    "普通表归档-查询SQL: Source={Source}, Sql={Sql}",
                    $"{sourceSchema}.{sourceTable}",
                    selectSql);

                progressCallback?.Invoke(new ArchiveProgressInfo { Message = "读取源数据并写入目标表", ProgressPercentage = 20, RowsProcessed = 0 });

                using (var selectCommand = new SqlCommand(selectSql, sourceConnection, tx))
                {
                    selectCommand.CommandTimeout = 300;
                    selectCommand.Parameters.AddWithValue("@BatchSize", batchSize);

                    using var reader = await selectCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);
                    using var bulkCopy = new SqlBulkCopy(
                        targetConnection,
                        SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction,
                        null)
                    {
                        DestinationTableName = targetTableFull,
                        BatchSize = batchSize,
                        BulkCopyTimeout = 300,
                        EnableStreaming = true
                    };

                    foreach (var col in columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }

                    await bulkCopy.WriteToServerAsync(reader, cancellationToken);
                    rowsCopied = bulkCopy.RowsCopied;
                }
            }
            else
            {
                // 删除一致性（关键）：
                // - 最稳健的方式是：在源库同一事务内执行 DELETE TOP(@BatchSize) ... OUTPUT DELETED.<columns>
                // - 这样“读取用于写入目标表的数据”和“删除源表的数据”天然是一致的，不依赖临时表，也不要求主键。
                // - BulkCopy 若失败，回滚源库事务即可恢复删除，避免数据丢失。

                var deleteWithOutputSql = $@"
DELETE TOP (@BatchSize) s
OUTPUT
    DELETED.{string.Join(",\n    DELETED.", columns.Select(c => $"[{c.ColumnName}]"))}
FROM {sourceTableFull} s WITH (READPAST, UPDLOCK, ROWLOCK)
WHERE ({wherePredicate});";

                // 审计：该语句同时承担“选取本批次数据”和“删除本批次数据”
                selectSqlForLog = deleteWithOutputSql;
                deleteSqlForLog = "(本批次通过 DELETE...OUTPUT 在查询阶段已同时完成删除)";

                _logger.LogInformation(
                    "普通表归档-删除并输出SQL: Source={Source}, Sql={Sql}",
                    $"{sourceSchema}.{sourceTable}",
                    deleteWithOutputSql);

                progressCallback?.Invoke(new ArchiveProgressInfo { Message = "读取源数据并写入目标表", ProgressPercentage = 20, RowsProcessed = 0 });

                using (var deleteOutputCommand = new SqlCommand(deleteWithOutputSql, sourceConnection, tx))
                {
                    deleteOutputCommand.CommandTimeout = 300;
                    deleteOutputCommand.Parameters.AddWithValue("@BatchSize", batchSize);

                    using var reader = await deleteOutputCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);
                    using var bulkCopy = new SqlBulkCopy(
                        targetConnection,
                        SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction,
                        null)
                    {
                        DestinationTableName = targetTableFull,
                        BatchSize = batchSize,
                        BulkCopyTimeout = 300,
                        EnableStreaming = true
                    };

                    foreach (var col in columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }

                    await bulkCopy.WriteToServerAsync(reader, cancellationToken);
                    rowsCopied = bulkCopy.RowsCopied;
                }
            }

            if (rowsCopied <= 0)
            {
                tx.Commit();
                var auditMarkdown = BuildAuditMarkdown(selectSqlForLog, deleteSqlForLog, createTableSqlForLog, null, tempKeySqlForLog);
                return new ArchiveExecutionResult
                {
                    Success = true,
                    ConfigurationId = config.Id,
                    ConfigurationName = config.Name,
                    SourceSchemaName = sourceSchema,
                    SourceTableName = sourceTable,
                    ArchiveMethod = config.ArchiveMethod,
                    RowsArchived = 0,
                    Message = "无可归档数据",
                    AuditMarkdown = auditMarkdown,
                    StartTimeUtc = startTimeUtc,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            progressCallback?.Invoke(new ArchiveProgressInfo
            {
                Message = config.DeleteSourceDataAfterArchive ? "删除源数据" : "保留源数据（仅复制）",
                ProgressPercentage = 80,
                RowsProcessed = rowsCopied
            });

            // 删除逻辑已在“临时表锁定主键”分支中完成，这里无需重复执行。

            tx.Commit();

            progressCallback?.Invoke(new ArchiveProgressInfo { Message = "普通表归档完成", ProgressPercentage = 100, RowsProcessed = rowsCopied });

            var successAuditMarkdown = BuildAuditMarkdown(selectSqlForLog, deleteSqlForLog, createTableSqlForLog, null, tempKeySqlForLog);
            return new ArchiveExecutionResult
            {
                Success = true,
                ConfigurationId = config.Id,
                ConfigurationName = config.Name,
                SourceSchemaName = sourceSchema,
                SourceTableName = sourceTable,
                ArchiveMethod = config.ArchiveMethod,
                RowsArchived = rowsCopied,
                Message = $"普通表归档成功: 已处理 {rowsCopied} 行",
                AuditMarkdown = successAuditMarkdown,
                StartTimeUtc = startTimeUtc,
                EndTimeUtc = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "普通表归档失败: Source={SourceSchema}.{SourceTable}, 查询SQL={SelectSql}, 删除SQL={DeleteSql}",
                config.SourceSchemaName,
                config.SourceTableName,
                selectSqlForLog ?? "(未生成)",
                deleteSqlForLog ?? "(未生成)");

            var errorDetailsMarkdown = $@"### 执行SQL（查询）

```sql
{selectSqlForLog ?? "(未生成)"}
```

### 执行SQL（删除）

```sql
{deleteSqlForLog ?? "(未生成)"}
```

### 异常

```text
{ex}
```";

            var failureAuditMarkdown = BuildAuditMarkdown(selectSqlForLog, deleteSqlForLog, createTableSqlForLog, ex, tempKeySqlForLog);

            return new ArchiveExecutionResult
            {
                Success = false,
                ConfigurationId = config.Id,
                ConfigurationName = config.Name,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                ArchiveMethod = config.ArchiveMethod,
                Message = $"普通表归档失败: {ex.Message}",
                ErrorDetails = errorDetailsMarkdown,
                AuditMarkdown = failureAuditMarkdown,
                StartTimeUtc = startTimeUtc,
                EndTimeUtc = DateTime.UtcNow
            };
        }
    }

    private static string BuildCreateTableSql(string targetSchema, string targetTable, List<UserColumnDefinition> columns)
    {
        var columnDefs = columns
            .Select(c => $"[{c.ColumnName}] {FormatDataType(c.DataType, c.MaxLength, c.Precision, c.Scale)}{(c.IsNullable ? string.Empty : " NOT NULL")}")
            .ToList();

        return $@"CREATE TABLE [{targetSchema}].[{targetTable}] (
    {string.Join(",\n    ", columnDefs)}
);";
    }

    private static string BuildAuditMarkdown(string? selectSql, string? deleteSql, string? createTableSql, Exception? ex, string? tempKeySql)
    {
        var sb = new System.Text.StringBuilder();

        if (!string.IsNullOrWhiteSpace(createTableSql))
        {
            sb.AppendLine("### 执行SQL（建表，若目标表不存在才会执行）");
            sb.AppendLine();
            sb.AppendLine("```sql");
            sb.AppendLine(createTableSql);
            sb.AppendLine("```");
            sb.AppendLine();
        }

        if (!string.IsNullOrWhiteSpace(tempKeySql))
        {
            sb.AppendLine("### 执行SQL（临时表锁定本批次主键，仅开启‘归档后删除源数据’才会执行）");
            sb.AppendLine();
            sb.AppendLine("```sql");
            sb.AppendLine(tempKeySql);
            sb.AppendLine("```");
            sb.AppendLine();
        }

        sb.AppendLine("### 执行SQL（查询）");
        sb.AppendLine();
        sb.AppendLine("```sql");
        sb.AppendLine(selectSql ?? "(未生成)");
        sb.AppendLine("```");
        sb.AppendLine();

        sb.AppendLine("### 执行SQL（删除，仅开启‘归档后删除源数据’才会执行）");
        sb.AppendLine();
        sb.AppendLine("```sql");
        sb.AppendLine(deleteSql ?? "(未生成)");
        sb.AppendLine("```");

        if (ex != null)
        {
            sb.AppendLine();
            sb.AppendLine("### 异常");
            sb.AppendLine();
            sb.AppendLine("```text");
            sb.AppendLine(ex.ToString());
            sb.AppendLine("```");
        }

        return sb.ToString();
    }

    private static string BuildWherePredicate(string filterColumn, string filterCondition)
    {
        // 兼容两种输入：
        // 1) 条件片段（推荐）："< DATEADD(day, -7, GETDATE())"，会拼成 "[CreateDate] < ..."
        // 2) 完整谓词："CreateDate < DATEADD(...)" 或 "[CreateDate] < ..."，则直接使用，避免重复列名导致语法错误
        var trimmed = filterCondition.Trim();
        if (string.IsNullOrWhiteSpace(trimmed))
        {
            throw new ArgumentException("归档过滤条件不能为空", nameof(filterCondition));
        }

        if (ContainsColumnReference(trimmed, filterColumn))
        {
            return trimmed;
        }

        return $"[{filterColumn}] {trimmed}";
    }

    private static bool ContainsColumnReference(string predicate, string filterColumn)
    {
        // 简单判定是否已包含列名：支持 "CreateDate" / "[CreateDate]" / "dbo.CreateDate" 等常见写法
        // 目的仅为避免重复拼接，不做完整 SQL 解析。
        var p = predicate;
        if (p.IndexOf($"[{filterColumn}]", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            return true;
        }

        // 词边界匹配，避免命中 "CreateDate2" 之类
        return System.Text.RegularExpressions.Regex.IsMatch(
            p,
            $@"\b{System.Text.RegularExpressions.Regex.Escape(filterColumn)}\b",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    private static string BuildSourceConnectionString(ArchiveDataSource dataSource)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = dataSource.ServerPort == 1433
                ? dataSource.ServerAddress
                : $"{dataSource.ServerAddress},{dataSource.ServerPort}",
            InitialCatalog = dataSource.DatabaseName,
            TrustServerCertificate = true,
            ConnectTimeout = 30,
            Encrypt = false
        };

        if (dataSource.UseIntegratedSecurity)
        {
            builder.IntegratedSecurity = true;
        }
        else
        {
            builder.IntegratedSecurity = false;
            builder.UserID = dataSource.UserName;
            builder.Password = dataSource.Password;
        }

        return builder.ConnectionString;
    }

    private static void ValidateSqlIdentifier(string value, string displayName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{displayName}不能为空");
        }

        foreach (var ch in value)
        {
            if (!(char.IsLetterOrDigit(ch) || ch == '_'))
            {
                throw new ArgumentException($"{displayName}包含非法字符: {value}");
            }
        }
    }

    private sealed record UserColumnDefinition(
        string ColumnName,
        string DataType,
        short MaxLength,
        byte Precision,
        byte Scale,
        bool IsNullable);

    private static async Task<List<UserColumnDefinition>> GetUserInsertableColumnsAsync(
        SqlConnection connection,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT
    c.name AS ColumnName,
    t.name AS DataType,
    c.max_length AS MaxLength,
    c.precision AS Precision,
    c.scale AS Scale,
    c.is_nullable AS IsNullable,
    c.is_computed AS IsComputed
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
INNER JOIN sys.tables tbl ON c.object_id = tbl.object_id
WHERE tbl.schema_id = SCHEMA_ID(@SchemaName)
  AND tbl.name = @TableName
ORDER BY c.column_id";

        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@SchemaName", schemaName);
        cmd.Parameters.AddWithValue("@TableName", tableName);

        var columns = new List<UserColumnDefinition>();
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var columnName = reader.GetString(0);
            var dataType = reader.GetString(1);
            var maxLength = reader.GetInt16(2);
            var precision = reader.GetByte(3);
            var scale = reader.GetByte(4);
            var isNullable = reader.GetBoolean(5);
            var isComputed = reader.GetBoolean(6);

            if (isComputed)
            {
                continue;
            }

            var dt = dataType.ToLowerInvariant();
            if (dt is "timestamp" or "rowversion")
            {
                continue;
            }

            columns.Add(new UserColumnDefinition(columnName, dataType, maxLength, precision, scale, isNullable));
        }

        return columns;
    }

    private static async Task EnsureSchemaAndTableExistsAsync(
        SqlConnection targetConnection,
        string targetSchema,
        string targetTable,
        List<UserColumnDefinition> columns,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        // 1) 确保 schema 存在
        using (var schemaCmd = targetConnection.CreateCommand())
        {
            schemaCmd.CommandText = @"
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @SchemaName)
BEGIN
    EXEC('CREATE SCHEMA [' + @SchemaName + ']')
END";
            schemaCmd.Parameters.AddWithValue("@SchemaName", targetSchema);
            await schemaCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // 2) 检查表是否存在
        using (var existsCmd = targetConnection.CreateCommand())
        {
            existsCmd.CommandText = @"
SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = @SchemaName AND t.name = @TableName
) THEN 1 ELSE 0 END";
            existsCmd.Parameters.AddWithValue("@SchemaName", targetSchema);
            existsCmd.Parameters.AddWithValue("@TableName", targetTable);
            var val = await existsCmd.ExecuteScalarAsync(cancellationToken);
            if (Convert.ToInt32(val) == 1)
            {
                return;
            }
        }

        // 3) 创建目标表（仅列定义 + NULL/NOT NULL，不创建索引/约束）
        var columnDefs = columns
            .Select(c => $"[{c.ColumnName}] {FormatDataType(c.DataType, c.MaxLength, c.Precision, c.Scale)}{(c.IsNullable ? string.Empty : " NOT NULL")}")
            .ToList();

        var createSql = $@"CREATE TABLE [{targetSchema}].[{targetTable}] (
    {string.Join(",\n    ", columnDefs)}
);";

        logger.LogInformation(
            "普通表归档-建表SQL: Target={Target}, Sql={Sql}",
            $"{targetSchema}.{targetTable}",
            createSql);

        using var createCmd = targetConnection.CreateCommand();
        createCmd.CommandText = createSql;
        createCmd.CommandTimeout = 300;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string FormatDataType(string dataType, short maxLength, byte precision, byte scale)
    {
        switch (dataType.ToLowerInvariant())
        {
            case "nvarchar":
            case "nchar":
                if (maxLength == -1)
                    return $"{dataType}(max)";
                return $"{dataType}({maxLength / 2})";

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
                return dataType;
        }
    }
}
