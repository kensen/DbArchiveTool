using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Partitions;

/// <summary>
/// 默认的分区切换检查实现，验证目标表结构、数据状态等前置条件。
/// </summary>
internal sealed class PartitionSwitchInspectionService : IPartitionSwitchInspectionService
{
    private readonly IDbConnectionFactory connectionFactory;
    private readonly ISqlExecutor sqlExecutor;
    private readonly ILogger<PartitionSwitchInspectionService> logger;

    public PartitionSwitchInspectionService(
        IDbConnectionFactory connectionFactory,
        ISqlExecutor sqlExecutor,
        ILogger<PartitionSwitchInspectionService> logger)
    {
        this.connectionFactory = connectionFactory;
        this.sqlExecutor = sqlExecutor;
        this.logger = logger;
    }

    public async Task<PartitionSwitchInspectionResult> InspectAsync(
        Guid dataSourceId,
        PartitionConfiguration configuration,
        PartitionSwitchInspectionContext context,
        CancellationToken cancellationToken = default)
    {
        if (configuration is null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        var blocking = new List<PartitionSwitchIssue>();
        var warnings = new List<PartitionSwitchIssue>();

        if (!int.TryParse(context.SourcePartitionKey, NumberStyles.Integer, CultureInfo.InvariantCulture, out var partitionNumber) || partitionNumber <= 0)
        {
            blocking.Add(new PartitionSwitchIssue(
                "InvalidPartitionKey",
                "源分区编号格式不正确，请使用有效的分区编号（正整数）。",
                "请从分区边界列表中选择正确的分区编号。"));
        }

        var targetSchema = context.TargetSchema?.Trim();
        var targetTable = context.TargetTable?.Trim();
        if (string.IsNullOrWhiteSpace(targetTable))
        {
            blocking.Add(new PartitionSwitchIssue(
                "InvalidTargetTable",
                "目标表名称不能为空。",
                "请提供目标表的名称，例如 Archive.SalesHistory 或 [Archive].[SalesHistory]。"));
        }

        await using var connection = await connectionFactory.CreateSqlConnectionAsync(dataSourceId, cancellationToken);

        var sourceTableInfo = await BuildTableInfoAsync(connection, configuration.SchemaName, configuration.TableName, cancellationToken);
        PartitionSwitchTableInfo targetTableInfo;
        bool targetExists = false;

        if (!string.IsNullOrWhiteSpace(targetSchema) && !string.IsNullOrWhiteSpace(targetTable))
        {
            targetExists = await TableExistsAsync(connection, targetSchema!, targetTable!, cancellationToken);
            if (!targetExists)
            {
                blocking.Add(new PartitionSwitchIssue(
                    "TargetTableMissing",
                    $"未找到目标表 {FormatQualifiedName(targetSchema!, targetTable!)}。",
                    "请确认目标表已在数据库中创建，且与源表结构保持一致。"));

                targetTableInfo = new PartitionSwitchTableInfo(targetSchema!, targetTable!, 0, Array.Empty<PartitionSwitchColumnInfo>());
            }
            else
            {
                targetTableInfo = await BuildTableInfoAsync(connection, targetSchema!, targetTable!, cancellationToken);

                if (targetTableInfo.RowCount > 0)
                {
                    blocking.Add(new PartitionSwitchIssue(
                        "TargetTableNotEmpty",
                        $"目标表 {FormatQualifiedName(targetSchema!, targetTable!)} 当前仍包含 {targetTableInfo.RowCount} 行数据。",
                        "请在执行 SWITCH 前清空目标表，或切换至其他临时表。"));
                }
            }
        }
        else
        {
            targetTableInfo = new PartitionSwitchTableInfo(
                targetSchema ?? configuration.SchemaName,
                targetTable ?? string.Empty,
                0,
                Array.Empty<PartitionSwitchColumnInfo>());
        }

        if (targetExists)
        {
            EvaluateColumnCompatibility(sourceTableInfo, targetTableInfo, blocking);
        }

        var result = new PartitionSwitchInspectionResult(
            blocking.Count == 0,
            blocking,
            warnings,
            sourceTableInfo,
            targetTableInfo);

        return result;
    }

    private async Task<bool> TableExistsAsync(SqlConnection connection, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COUNT(1)
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @Schema AND t.name = @Table;";

        var count = await sqlExecutor.QuerySingleAsync<int>(
            connection,
            sql,
            new { Schema = schema, Table = table },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();
        return count > 0;
    }

    private async Task<PartitionSwitchTableInfo> BuildTableInfoAsync(SqlConnection connection, string schema, string table, CancellationToken cancellationToken)
    {
        var columns = await LoadColumnsAsync(connection, schema, table, cancellationToken);
        var rowCount = await LoadRowCountAsync(connection, schema, table, cancellationToken);
        return new PartitionSwitchTableInfo(schema, table, rowCount, columns);
    }

    private async Task<IReadOnlyList<PartitionSwitchColumnInfo>> LoadColumnsAsync(SqlConnection connection, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT 
    c.column_id AS ColumnId,
    c.name AS ColumnName,
    t.name AS DataType,
    c.max_length AS MaxLength,
    c.precision AS Precision,
    c.scale AS Scale,
    c.is_nullable AS IsNullable,
    c.is_identity AS IsIdentity,
    c.is_computed AS IsComputed
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID(@FullName)
ORDER BY c.column_id;";

        var fullName = FormatQualifiedName(schema, table);
        var rows = await sqlExecutor.QueryAsync<ColumnRow>(
            connection,
            sql,
            new { FullName = fullName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();

        return rows
            .Select(row => new PartitionSwitchColumnInfo(
                row.ColumnName,
                row.DataType,
                NormalizeMaxLength(row.DataType, row.MaxLength),
                row.Precision,
                row.Scale,
                row.IsNullable,
                row.IsIdentity,
                row.IsComputed))
            .ToList();
    }

    private async Task<long> LoadRowCountAsync(SqlConnection connection, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COALESCE(SUM(p.rows), 0)
FROM sys.partitions p
WHERE p.object_id = OBJECT_ID(@FullName)
  AND p.index_id IN (0, 1);";

        var fullName = FormatQualifiedName(schema, table);
        var count = await sqlExecutor.QuerySingleAsync<long>(
            connection,
            sql,
            new { FullName = fullName },
            transaction: null);

        cancellationToken.ThrowIfCancellationRequested();
        return count;
    }

    private static void EvaluateColumnCompatibility(
        PartitionSwitchTableInfo source,
        PartitionSwitchTableInfo target,
        List<PartitionSwitchIssue> blocking)
    {
        if (source.Columns.Count != target.Columns.Count)
        {
            blocking.Add(new PartitionSwitchIssue(
                "ColumnCountMismatch",
                $"源表与目标表的列数量不一致（源 {source.Columns.Count} 列，目标 {target.Columns.Count} 列）。",
                "请确保目标表与源表使用完全一致的列定义。"));
            return;
        }

        for (var index = 0; index < source.Columns.Count; index++)
        {
            var sourceColumn = source.Columns[index];
            var targetColumn = target.Columns[index];

            if (!string.Equals(sourceColumn.Name, targetColumn.Name, StringComparison.OrdinalIgnoreCase))
            {
                blocking.Add(new PartitionSwitchIssue(
                    "ColumnNameMismatch",
                    $"第 {index + 1} 列名称不一致：源为 {sourceColumn.Name}，目标为 {targetColumn.Name}。",
                    "请调整目标表列名称顺序，使其与源表完全一致。"));
                break;
            }

            if (!string.Equals(sourceColumn.DataType, targetColumn.DataType, StringComparison.OrdinalIgnoreCase) ||
                Normalize(sourceColumn.MaxLength) != Normalize(targetColumn.MaxLength) ||
                Normalize(sourceColumn.Precision) != Normalize(targetColumn.Precision) ||
                Normalize(sourceColumn.Scale) != Normalize(targetColumn.Scale))
            {
                blocking.Add(new PartitionSwitchIssue(
                    "ColumnTypeMismatch",
                    $"列 {sourceColumn.Name} 的数据类型或长度与目标表不一致。",
                    "请保持两端的数据类型、精度、标度一致，否则无法执行 SWITCH。"));
                break;
            }

            if (sourceColumn.IsNullable != targetColumn.IsNullable)
            {
                blocking.Add(new PartitionSwitchIssue(
                    "ColumnNullabilityMismatch",
                    $"列 {sourceColumn.Name} 的可空性不同。",
                    "请确保目标表与源表的可空性完全一致。"));
                break;
            }

            if (sourceColumn.IsIdentity != targetColumn.IsIdentity)
            {
                blocking.Add(new PartitionSwitchIssue(
                    "IdentityMismatch",
                    $"列 {sourceColumn.Name} 的标识列属性不一致。",
                    "请确认目标表是否需要包含相同的 IDENTITY 定义。"));
                break;
            }

            if (sourceColumn.IsComputed != targetColumn.IsComputed)
            {
                blocking.Add(new PartitionSwitchIssue(
                    "ComputedColumnMismatch",
                    $"列 {sourceColumn.Name} 的计算列属性不一致。",
                    "请确保目标表计算列定义与源表一致。"));
                break;
            }
        }
    }

    private static int? NormalizeMaxLength(string dataType, short maxLength)
    {
        // 对于 nvarchar/nchar 等类型，max_length 为字节长度，需要换算为字符数。
        return dataType.Equals("nvarchar", StringComparison.OrdinalIgnoreCase)
            || dataType.Equals("nchar", StringComparison.OrdinalIgnoreCase)
            ? maxLength / 2
            : maxLength;
    }

    private static int? Normalize(int? value) => value == 0 ? null : value;
    private static byte? Normalize(byte? value) => value == 0 ? null : value;

    private static string FormatQualifiedName(string schema, string table)
        => $"[{schema}].[{table}]";

    private sealed record ColumnRow(
        int ColumnId,
        string ColumnName,
        string DataType,
        short MaxLength,
        byte Precision,
        int Scale,
        bool IsNullable,
        bool IsIdentity,
        bool IsComputed);
}
